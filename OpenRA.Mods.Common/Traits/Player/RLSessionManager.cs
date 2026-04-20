#region Copyright & License Information
/*
 * Copyright (c) The OpenRA Developers and Contributors
 * This file is part of OpenRA, which is free software. It is made
 * available to you under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version. For more
 * information, see COPYING.
 */
#endregion

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OpenRA.Network;
using OpenRA.Primitives;
using OpenRA.Support;
using OpenRA.Traits;

namespace OpenRA.Mods.Common.Traits
{
	/// <summary>
	/// Manages multiple concurrent RL game sessions within a single process.
	/// Shares ModData (loaded once) across all sessions. A fixed-size pool of
	/// worker threads processes game ticks — sessions without pending FastAdvance
	/// use zero CPU. Workers are dedicated threads (not the .NET ThreadPool)
	/// to avoid starving gRPC request handling.
	/// </summary>
	public static class RLSessionManager
	{
		static ModData modData;
		static readonly object MapCacheLock = new();
		static readonly object WorldCreateLock = new();

		/// <summary>
		/// Global tick semaphore: limits concurrent World.Tick() calls across all sessions.
		/// With IsMultiSession fixes (LongBitSet guard, IsCurrentWorld, InitializeLoaders skip,
		/// Sound guard), shared state races are eliminated. Allow full parallelism (8 per server).
		/// </summary>
		static readonly SemaphoreSlim GlobalTickSemaphore = new(8, 8);
		static readonly HashSet<string> PreparedMapUids = new();

		/// <summary>Cache resolved MapPreview by map name to avoid repeated MapCache enumeration.</summary>
		static readonly ConcurrentDictionary<string, MapPreview> ResolvedMaps = new();

		static int nextClientIndex = 100;

		/// <summary>
		/// Per-session state needed for ticking.
		/// </summary>
		internal sealed class SessionState
		{
			public readonly OrderManager OrderManager;
			public readonly World World;
			/// <summary>
			/// Per-session GameSave that captures every order sent through
			/// this session's EchoConnection. Populated during InitSession and
			/// used by SaveSnapshot to serialize a point-in-time snapshot.
			/// </summary>
			public GameSave GameSave;
			/// <summary>The MapPreview used to start this session (needed to restore on load).</summary>
			public MapPreview MapPreview;

			/// <summary>Prevents two concurrent FastAdvance calls from ticking the same World.</summary>
			public readonly SemaphoreSlim TickLock = new(1, 1);

			/// <summary>Track in-flight work so DestroySession can wait for it to finish.</summary>
			public volatile WorkItem ActiveWorkItem;

			/// <summary>Last time this session had activity (FastAdvance call). Used by reaper.</summary>
			public long LastActivityTicks = DateTime.UtcNow.Ticks;

			public void TouchActivity() { LastActivityTicks = DateTime.UtcNow.Ticks; }
			public TimeSpan IdleTime => DateTime.UtcNow - new DateTime(Interlocked.Read(ref LastActivityTicks));

			public SessionState(OrderManager om, World w)
			{
				OrderManager = om;
				World = w;
			}
		}

		/// <summary>Session state registry, keyed by session ID.</summary>
		internal static readonly ConcurrentDictionary<string, SessionState> SessionStates = new();

		/// <summary>
		/// Work item submitted to the worker pool when FastAdvance is requested.
		/// </summary>
		internal sealed class WorkItem
		{
			public readonly SessionState State;
			public readonly ExternalBotBridge Bridge;
			public readonly TaskCompletionSource<bool> Completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

			public WorkItem(SessionState s, ExternalBotBridge b) { State = s; Bridge = b; }
		}

		/// <summary>Bounded work queue. If full, FastAdvance returns RESOURCE_EXHAUSTED.</summary>
		static BlockingCollection<WorkItem> workQueue;
		static Thread[] workers;

		/// <summary>
		/// Initialize with shared ModData. Called once at process start.
		/// Starts the worker pool.
		/// </summary>
		public static void Initialize(ModData md)
		{
			modData = md;
			Game.IsMultiSession = true;
			ExternalBotBridge.MultiSessionMode = true;
			Support.PerfHistory.Disabled = true;

			var workerCount = Environment.ProcessorCount;
			workQueue = new BlockingCollection<WorkItem>(boundedCapacity: workerCount * 4);
			workers = new Thread[workerCount];

			for (var i = 0; i < workerCount; i++)
			{
				workers[i] = new Thread(WorkerLoop)
				{
					IsBackground = true,
					Name = $"RL-Worker-{i}"
				};
				workers[i].Start();
			}

			Log.Write("rl-bridge", $"RLSessionManager initialized: {workerCount} workers, queue capacity {workerCount * 4}");

			// Start session reaper: cleans up sessions idle for >5 minutes.
			// Prevents leaked sessions from exhausting server capacity.
			var reaper = new Thread(() =>
			{
				while (true)
				{
					Thread.Sleep(30_000); // Check every 30s
					try
					{
						var maxIdle = TimeSpan.FromMinutes(5);
						foreach (var kvp in SessionStates)
						{
							if (kvp.Value.IdleTime > maxIdle)
							{
								Log.Write("rl-bridge", $"Reaping idle session {kvp.Key} (idle {kvp.Value.IdleTime.TotalSeconds:F0}s)");
								try { DestroySession(kvp.Key); }
								catch (Exception e) { Log.Write("rl-bridge", $"Reaper error for {kvp.Key}: {e.Message}"); }
							}
						}

						// Also clean up orphaned entries in Sessions that have no SessionState
						foreach (var kvp in ExternalBotBridge.Sessions)
						{
							if (!SessionStates.ContainsKey(kvp.Key))
							{
								Log.Write("rl-bridge", $"Reaping orphaned session entry {kvp.Key}");
								kvp.Value.Deactivate();
							}
						}
					}
					catch (Exception e)
					{
						Log.Write("rl-bridge", $"Reaper sweep error: {e.Message}");
					}
				}
			})
			{
				IsBackground = true,
				Name = "RL-Session-Reaper"
			};
			reaper.Start();
		}

		/// <summary>
		/// Worker thread loop. Pulls work items and ticks sessions until
		/// their fast-advance is complete.
		/// </summary>
		static void WorkerLoop()
		{
			foreach (var item in workQueue.GetConsumingEnumerable())
			{
				var state = item.State;
				state.ActiveWorkItem = item;
				try
				{
					// Per-session lock: prevents two concurrent FastAdvance calls
					// from ticking the same World simultaneously.
					state.TickLock.Wait();
					try
					{
						// Global tick limit: OpenRA engine has static state
						// that crashes with too many concurrent World.Tick() calls.
						GlobalTickSemaphore.Wait();
						try
						{
							TickSession(state, item.Bridge);
						}
						finally
						{
							GlobalTickSemaphore.Release();
						}
					}
					finally
					{
						state.TickLock.Release();
					}

					item.Completed.TrySetResult(true);
				}
				catch (Exception e)
				{
					Log.Write("rl-bridge", $"Worker error: {e}");
					item.Completed.TrySetException(e);
				}
				finally
				{
					state.ActiveWorkItem = null;
				}
			}
		}

		/// <summary>
		/// Submit a tick work item to the worker pool.
		/// Returns the WorkItem so the caller can await completion.
		/// Throws if the queue is full (RESOURCE_EXHAUSTED).
		/// </summary>
		internal static WorkItem SubmitWork(SessionState state, ExternalBotBridge bridge)
		{
			var item = new WorkItem(state, bridge);
			if (!workQueue.TryAdd(item, TimeSpan.FromSeconds(5)))
				return null; // Queue persistently full — caller should return RESOURCE_EXHAUSTED

			return item;
		}

		/// <summary>
		/// Start the gRPC server on the specified port. Blocks until shutdown.
		/// </summary>
		public static void StartGrpcServer(int port)
		{
			Log.Write("rl-bridge", $"Starting multi-session gRPC server on port {port}");
			ExternalBotBridge.StartGrpcServer(port);
		}

		/// <summary>
		/// Create a new game session. Returns the session_id immediately;
		/// the game world is created asynchronously on a background thread.
		/// FastAdvance will wait for the bridge to activate before proceeding.
		/// </summary>
		public static string CreateSession(string mapName, string bots, int seed)
		{
			var sessionId = Guid.NewGuid().ToString("N")[..12];
			Log.Write("rl-bridge", $"Creating session {sessionId}: map={mapName}, bots={bots}, seed={seed}");

			var thread = new Thread(() =>
			{
				try
				{
					InitSession(sessionId, mapName, bots, seed);
				}
				catch (Exception e)
				{
					Log.Write("rl-bridge", $"Session {sessionId} init failed: {e}");
					SessionStates.TryRemove(sessionId, out _);

					if (ExternalBotBridge.Sessions.TryRemove(sessionId, out var crashed))
						crashed.Deactivate();
				}
			})
			{
				IsBackground = true,
				Name = $"RL-Init-{sessionId}"
			};
			thread.Start();

			return sessionId;
		}

		/// <summary>
		/// Destroy a session and clean up its resources.
		/// </summary>
		public static void DestroySession(string sessionId)
		{
			if (ExternalBotBridge.Sessions.TryGetValue(sessionId, out var bridge))
				bridge.Deactivate();

			if (SessionStates.TryRemove(sessionId, out var state))
			{
				// Wait for any in-flight work to finish before disposing
				var activeWork = state.ActiveWorkItem;
				if (activeWork != null)
				{
					try { activeWork.Completed.Task.Wait(TimeSpan.FromSeconds(10)); }
					catch { /* timeout or cancelled — proceed with dispose */ }
				}

				try
				{
					state.World?.Dispose();
				}
				catch (Exception e)
				{
					Log.Write("rl-bridge", $"Error disposing world for {sessionId}: {e.Message}");
				}

				try
				{
					state.OrderManager?.Dispose();
				}
				catch (Exception e)
				{
					Log.Write("rl-bridge", $"Error disposing OrderManager for {sessionId}: {e.Message}");
				}
			}

			Log.Write("rl-bridge", $"Session {sessionId} destroyed");
		}

		/// <summary>
		/// Serialize a session's current state into an .orasav-format byte blob.
		///
		/// Collects per-trait data from every IGameSaveTraitData implementor
		/// (mirrors what World.RequestGameSave does order-wise, but directly
		/// since RL mode has no Server to route GameSaveTraitData orders to
		/// GameSave.AddTraitData), then serializes the already-accumulated
		/// order stream + lobby metadata + trait data via GameSave.Save.
		/// </summary>
		public static (byte[] Bytes, int LastFrame) SaveSession(string sessionId)
		{
			if (!SessionStates.TryGetValue(sessionId, out var state))
				throw new InvalidOperationException($"SaveSession: session {sessionId} not found");

			var gs = state.GameSave
				?? throw new InvalidOperationException($"SaveSession: session {sessionId} has no GameSave");
			var world = state.World
				?? throw new InvalidOperationException($"SaveSession: session {sessionId} has no World");

			// Take the session's tick lock so we're not racing World.Tick while
			// iterating traits and serializing.
			state.TickLock.Wait();
			try
			{
				// Mirror World.RequestGameSave's trait-data collection, but write
				// directly into GameSave.TraitData instead of going through the
				// order stream (which would only be processed by a Server).
				var i = 0;
				foreach (var tp in world.ActorsWithTrait<IGameSaveTraitData>())
				{
					var data = tp.Trait.IssueTraitData(tp.Actor);
					if (data != null && data.Count > 0)
						gs.AddTraitData(i, new MiniYaml("", data));
					i++;
				}

				using var ms = new MemoryStream();
				gs.Save(ms);
				Log.Write("rl-bridge",
					$"SaveSession: {sessionId} serialized {ms.Length} bytes " +
					$"(lastFrame={gs.LastOrdersFrame}, traits={gs.TraitData.Count})");
				return (ms.ToArray(), gs.LastOrdersFrame);
			}
			finally
			{
				state.TickLock.Release();
			}
		}

		/// <summary>
		/// Create a new session by loading an .orasav byte blob.
		///
		/// Recreates the session's lobby (seed, slots, bots) from the snapshot's
		/// metadata, then replays the stored order stream through a fresh World
		/// so it reaches the saved tick. Trait-data patches in the snapshot are
		/// applied via the existing World.AddGameSaveTraitData path when the
		/// replay completes (see World.cs line 444 "wasLoadingGameSave").
		///
		/// Returns (assigned_session_id, last_frame).
		/// </summary>
		public static (string SessionId, int LastFrame) LoadSession(byte[] snapshot, string requestedSessionId)
		{
			// Parse the snapshot metadata + order stream + trait data.
			GameSave loaded;
			using (var ms = new MemoryStream(snapshot, writable: false))
				loaded = new GameSave(ms, "<LoadSnapshot>");

			var sessionId = string.IsNullOrEmpty(requestedSessionId)
				? Guid.NewGuid().ToString("N")
				: requestedSessionId;

			// Reconstruct the `bots` string (slot:bottype,...) from the saved
			// SlotClients so InitSession's existing setup path spawns the same
			// lobby the snapshot was produced under.
			var bots = string.Join(",",
				loaded.SlotClients
					.Where(kv => !string.IsNullOrEmpty(kv.Value.Bot))
					.Select(kv => $"{kv.Key}:{kv.Value.Bot}"));

			var mapName = loaded.GlobalSettings.Map;
			var seed = loaded.GlobalSettings.RandomSeed;

			Log.Write("rl-bridge",
				$"LoadSession {sessionId}: snapshot has map={mapName} seed={seed} " +
				$"bots=[{bots}] lastFrame={loaded.LastOrdersFrame} traits={loaded.TraitData.Count}");

			// Reuse InitSession to stand up the world, then patch the resulting
			// session's OrderManager with the saved order stream before any
			// RL commands come in.
			var initThread = new Thread(() =>
			{
				try { InitSession(sessionId, mapName, bots, seed); }
				catch (Exception e) { Log.Write("rl-bridge", $"LoadSession InitSession error: {e}"); }
			})
			{
				IsBackground = true,
				Name = $"RL-Load-Init-{sessionId[..8]}",
			};
			initThread.Start();

			// Wait up to 300s for the session to be registered (matches CreateSession behavior).
			var deadline = DateTime.UtcNow.AddSeconds(300);
			SessionState state = null;
			while (DateTime.UtcNow < deadline)
			{
				if (SessionStates.TryGetValue(sessionId, out state) && state.World != null)
					break;
				Thread.Sleep(50);
			}

			if (state == null)
				throw new InvalidOperationException(
					$"LoadSession {sessionId}: InitSession did not register a SessionState within 300s");

			// Replay the saved orders into the new session's OrderManager. Orders
			// are fed directly (bypassing the EchoConnection) to guarantee frame
			// alignment. During this replay the World is in IsLoadingGameSave mode
			// (NetFrameNumber < GameSaveLastFrame) and it applies the accumulated
			// trait data once the replay finishes.
			state.TickLock.Wait();
			try
			{
				var om = state.OrderManager;
				om.GameSaveLastFrame = loaded.LastOrdersFrame;
				om.GameSaveLastSyncFrame = loaded.LastSyncFrame;

				loaded.ParseOrders(om.LobbyInfo, (frame, clientIndex, data) =>
				{
					// Try order packet first; fall through to sync packet.
					if (OrderIO.TryParseOrderPacket(data, out var op))
					{
						if (op.Frame == 0)
							om.ReceiveImmediateOrders(clientIndex, op.Orders);
						else
							om.ReceiveOrders(clientIndex, op);
					}
					else if (OrderIO.TryParseSync(data, out var sync))
					{
						om.ReceiveSync(sync);
					}
					else
					{
						Log.Write("rl-bridge",
							$"LoadSession {sessionId}: unrecognized packet at frame={frame} " +
							$"client={clientIndex} len={data.Length}");
					}
				});

				// Tick the world forward until it absorbs all replayed orders and
				// exits loading mode. Safety cap of 2x LastOrdersFrame to avoid
				// infinite loops if something is wrong with the replay stream.
				var safetyCap = Math.Max(loaded.LastOrdersFrame * 2, loaded.LastOrdersFrame + 1000);
				var ticks = 0;
				while (om.World != null && om.World.IsLoadingGameSave && ticks < safetyCap)
				{
					om.LastTickTime.Value = 0;
					Sync.RunUnsynced(false, om.World, () =>
					{
						om.TickImmediate();
						return true;
					});
					if (om.TryTick())
						om.World.Tick();
					ticks++;
				}

				Log.Write("rl-bridge",
					$"LoadSession {sessionId}: replayed {ticks} ticks, " +
					$"world.NetFrame={om.NetFrameNumber}, " +
					$"IsLoadingGameSave={om.World?.IsLoadingGameSave}");

				if (om.World != null && om.World.IsLoadingGameSave)
					throw new InvalidOperationException(
						$"LoadSession {sessionId}: replay exceeded safety cap " +
						$"({ticks} ticks) without finishing load");
			}
			finally
			{
				state.TickLock.Release();
			}

			return (sessionId, loaded.LastOrdersFrame);
		}

		/// <summary>
		/// Tick a session's game forward until fast-advance completes or game ends.
		/// Called by worker threads, not by gRPC threads.
		/// </summary>
		static void TickSession(SessionState state, ExternalBotBridge bridge)
		{
			var orderManager = state.OrderManager;
			var world = state.World;

			var tickCount = 0;
			var maxTicks = 10000; // Safety limit

			while (!world.IsGameOver && !bridge.SessionDone.IsSet && tickCount < maxTicks)
			{
				orderManager.LastTickTime.Value = 0;

				Sync.RunUnsynced(false, world, () =>
				{
					orderManager.TickImmediate();
					return true;
				});

				var didTick = orderManager.TryTick();
				if (didTick)
					world.Tick();

				tickCount++;

				// Once fast-forward is done, stop ticking
				if (!orderManager.IsFastForwarding)
					break;
			}

			if (tickCount >= maxTicks)
				Log.Write("rl-bridge", $"TickSession: safety limit reached after {maxTicks} ticks!");
		}

		/// <summary>
		/// Initialize a game session: create World, find bridge, register state.
		/// The calling thread exits after this returns — no persistent tick loop.
		/// </summary>
		static void InitSession(string sessionId, string mapName, string bots, int seed)
		{
			// 1. Resolve map (cached — only first request per map name hits MapCache).
			//    MapCache.GetEnumerator() calls UpdateMaps() which mutates collections,
			//    so all MapCache access must be serialized via MapCacheLock.
			//
			//    PERF: Avoid LoadMaps() which rescans ALL map files in every directory.
			//    With 60+ stock maps plus accumulated scenario maps from previous waves,
			//    LoadMaps() takes ~1-2s per call. With 20 sessions each calling LoadMaps()
			//    (because each has a unique map name), total rescan time is 20-40s —
			//    enough to push later sessions past the 60s wait_for_ready timeout.
			//
			//    Instead, load just the specific map file via LoadMap() (single file I/O).
			var mapPreview = ResolvedMaps.GetOrAdd(mapName, name =>
			{
				lock (MapCacheLock)
				{
					var mp = modData.MapCache
						.FirstOrDefault(m => m.Status == MapStatus.Available &&
							(Path.GetFileName(m.Path) == name || m.Uid == name));

					if (mp == null)
					{
						// Try loading just this specific map file from known map directories
						// instead of rescanning everything with LoadMaps().
						Log.Write("rl-bridge", $"Session {sessionId}: Map '{name}' not in cache, loading single map...");
						foreach (var kv in modData.MapCache.MapLocations)
						{
							if (kv.Key.Contains(name))
							{
								modData.MapCache.LoadMap(name, kv.Key, kv.Value, null);
								break;
							}
						}

						mp = modData.MapCache
							.FirstOrDefault(m => m.Status == MapStatus.Available &&
								(Path.GetFileName(m.Path) == name || m.Uid == name));

						// Fallback: if single-file load didn't work, do full rescan
						if (mp == null)
						{
							Log.Write("rl-bridge", $"Session {sessionId}: Single-file load failed, full rescan...");
							modData.MapCache.LoadMaps(modData);
							mp = modData.MapCache
								.FirstOrDefault(m => m.Status == MapStatus.Available &&
									(Path.GetFileName(m.Path) == name || m.Uid == name));
						}
					}

					return mp;
				}
			});

			if (mapPreview == null)
			{
				Log.Write("rl-bridge", $"Session {sessionId}: Map '{mapName}' not found");
				ResolvedMaps.TryRemove(mapName, out _); // Don't cache failures
				return;
			}

			// 2. Load map from disk (per-session — each needs its own Map instance).
			//    MapPreview.package (its ZipFile handle) gets disposed after first Map creation,
			//    so cached MapPreview entries become stale. Retry up to 3× with cache invalidation:
			//    evict the stale entry so ResolvedMaps.GetOrAdd re-opens a fresh ZipFile handle.
			Map map = null;
			string mapPath = null;
			for (var attempt = 0; attempt < 3; attempt++)
			{
				try
				{
					lock (MapCacheLock)
					{
						map = mapPreview.ToMap();
						mapPath = mapPreview.Path;
					}
					break;
				}
				catch (ObjectDisposedException)
				{
					Log.Write("rl-bridge", $"Session {sessionId}: ZipFile disposed on attempt {attempt + 1}, evicting cache entry for '{mapName}'");
					ResolvedMaps.TryRemove(mapName, out _);
					// Re-resolve MapPreview with a fresh handle
					lock (MapCacheLock)
					{
						mapPreview = modData.MapCache.FirstOrDefault(m =>
							m.Status == MapStatus.Available &&
							(Path.GetFileName(m.Path) == mapName || m.Uid == mapName));
					}
					if (mapPreview == null)
					{
						Log.Write("rl-bridge", $"Session {sessionId}: Map '{mapName}' not found after cache eviction");
						return;
					}
				}
			}
			if (map == null)
			{
				Log.Write("rl-bridge", $"Session {sessionId}: Failed to load map '{mapName}' after 3 attempts");
				return;
			}
			Log.Write("rl-bridge", $"Session {sessionId}: Map loaded from {mapPath}, {map.ActorDefinitions.Count()} actor defs");

			// 3. PrepareMap — must run for every map (sprite sequences are map-specific,
			//    and randomized scenarios produce unique map UIDs every time).
			//    Serialized because it mutates global statics (ChromeMetrics, ChromeProvider, Sound).
			lock (WorldCreateLock)
			{
				modData.PrepareMap(map);
			}

			// 4. Create isolated OrderManager with EchoConnection (no network)
			// These are per-session objects, safe to create outside the lock.
			var connection = new EchoConnection();
			var orderManager = new OrderManager(connection);

			// Per-session GameSave: captures every order that flows through
			// this session's EchoConnection so SaveSnapshot can serialize a
			// point-in-time snapshot. Wired as an OrderRecorder callback
			// before the game starts so it sees the initial order stream.
			var gameSave = new GameSave();
			connection.OrderRecorder = (frame, data) =>
				gameSave.DispatchOrders(connection_LocalClientId(), frame, data);

			// 5. Build LobbyInfo with map slots and bot assignments
			SetupLobbyInfo(orderManager, mapPreview, map, bots, seed);

			// 6. World creation + LoadComplete (serialized — traits access shared state).
			//    With PrepareMap cached and map lookup cached, the lock only covers
			//    World construction (~300ms per session). 64 sessions ≈ 19s total.
			Log.Write("rl-bridge", $"Session {sessionId}: Creating world");
			lock (WorldCreateLock)
			{
				Game.OrderManager = orderManager;
				ExternalBotBridge.NextSessionId = sessionId;
				orderManager.World = new World(map, modData, orderManager, WorldType.Regular);
				ExternalBotBridge.NextSessionId = null;
				orderManager.World.LoadComplete(null);
				// GameSave needs the lobby metadata (slot→client mapping) before
				// DispatchOrders can classify orders. StartGame here primes the
				// internal clientsBySlotIndex array from the lobby we built.
				gameSave.StartGame(orderManager.LobbyInfo, mapPreview);
				orderManager.StartGame();
			}

			var world = orderManager.World;

			// 7. Register session state IMMEDIATELY after world is ready, BEFORE
			// the bridge becomes visible to gRPC. This prevents a race where
			// FastAdvance finds the bridge (via WaitForBridge) but SessionStates
			// hasn't been populated yet, causing NOT_FOUND.
			SessionStates[sessionId] = new SessionState(orderManager, world)
			{
				GameSave = gameSave,
				MapPreview = mapPreview,
			};

			// 8. Find the ExternalBotBridge
			ExternalBotBridge bridge = null;
			foreach (var player in world.Players)
			{
				var b = player.PlayerActor.TraitOrDefault<ExternalBotBridge>();
				if (b != null && b.IsEnabled)
				{
					bridge = b;
					break;
				}
			}

			if (bridge == null)
			{
				Log.Write("rl-bridge", $"Session {sessionId}: ExternalBotBridge not found");
				SessionStates.TryRemove(sessionId, out _);
				world.Dispose();
				orderManager.Dispose();
				return;
			}

			// Re-register under the requested sessionId
			var actualId = bridge.SessionId;
			if (actualId != sessionId)
			{
				ExternalBotBridge.Sessions.TryRemove(actualId, out _);
				ExternalBotBridge.Sessions[sessionId] = bridge;
			}

			Log.Write("rl-bridge", $"Session {sessionId}: Ready (init thread exiting)");
		}

		/// <summary>
		/// Build LobbyInfo for a game session with the specified bot configuration.
		/// </summary>
		static void SetupLobbyInfo(OrderManager orderManager, MapPreview mapPreview, Map map, string botsConfig, int seed)
		{
			var lobbyInfo = orderManager.LobbyInfo;

			lobbyInfo.GlobalSettings.Map = mapPreview.Uid;
			lobbyInfo.GlobalSettings.RandomSeed = seed != 0 ? seed : new MersenneTwister().Next();
			lobbyInfo.GlobalSettings.EnableSingleplayer = true;

			var mapPlayers = new MapPlayers(map.PlayerDefinitions);
			lobbyInfo.Slots.Clear();
			foreach (var kv in mapPlayers.Players.Where(p => p.Value.Playable))
			{
				lobbyInfo.Slots[kv.Key] = new Session.Slot
				{
					PlayerReference = kv.Key,
					Closed = false,
					AllowBots = kv.Value.AllowBots,
					LockFaction = kv.Value.LockFaction,
					LockColor = kv.Value.LockColor,
					LockTeam = kv.Value.LockTeam,
					LockHandicap = kv.Value.LockHandicap,
					LockSpawn = kv.Value.LockSpawn,
					Required = kv.Value.Required,
				};
			}

			var hostClient = new Session.Client
			{
				Index = connection_LocalClientId(),
				Name = "RL-Host",
				State = Session.ClientState.Ready,
				Faction = "Random",
				SpawnPoint = 0,
				Team = 0,
				IsAdmin = true,
			};
			lobbyInfo.Clients.Add(hostClient);

			if (!string.IsNullOrEmpty(botsConfig))
			{
				var botInfos = mapPreview.PlayerActorInfo.TraitInfos<IBotInfo>().ToList();
				var rng = new MersenneTwister();

				foreach (var entry in botsConfig.Split(','))
				{
					var parts = entry.Trim().Split(':');
					if (parts.Length != 2)
						continue;

					var slotName = parts[0];
					var botType = parts[1];

					if (!lobbyInfo.Slots.ContainsKey(slotName))
					{
						Log.Write("rl-bridge", $"Slot '{slotName}' not found in map, skipping bot");
						continue;
					}

					var botInfo = botInfos.FirstOrDefault(b => b.Type == botType);
					if (botInfo == null)
					{
						Log.Write("rl-bridge", $"Bot type '{botType}' not found, skipping");
						continue;
					}

					var clientIndex = Interlocked.Increment(ref nextClientIndex);
					var botClient = new Session.Client
					{
						Index = clientIndex,
						Name = botInfo.Name,
						Bot = botType,
						Slot = slotName,
						Faction = "Random",
						SpawnPoint = 0,
						Team = 0,
						Handicap = 0,
						State = Session.ClientState.NotReady,
						BotControllerClientIndex = connection_LocalClientId(),
						Color = Color.FromArgb(rng.Next(256), rng.Next(256), rng.Next(256)),
						PreferredColor = Color.FromArgb(rng.Next(256), rng.Next(256), rng.Next(256)),
					};

					var pr = mapPlayers.Players.GetValueOrDefault(slotName);
					if (pr != null)
						SyncClientToPlayerReference(botClient, pr);

					lobbyInfo.Clients.Add(botClient);
				}
			}

			var options = mapPreview.PlayerActorInfo.TraitInfos<ILobbyOptions>()
				.Concat(mapPreview.WorldActorInfo.TraitInfos<ILobbyOptions>())
				.SelectMany(t => t.LobbyOptions(mapPreview));

			foreach (var o in options)
			{
				lobbyInfo.GlobalSettings.LobbyOptions[o.Id] = new Session.LobbyOptionState
				{
					IsLocked = o.IsLocked,
					Value = o.DefaultValue,
					PreferredValue = o.DefaultValue,
				};
			}
		}

		static int connection_LocalClientId() => 1;

		static void SyncClientToPlayerReference(Session.Client c, PlayerReference pr)
		{
			if (pr == null)
				return;

			if (pr.LockFaction)
				c.Faction = pr.Faction;
			if (pr.LockSpawn)
				c.SpawnPoint = pr.Spawn;
			if (pr.LockTeam)
				c.Team = pr.Team;
			if (pr.LockHandicap)
				c.Handicap = pr.Handicap;

			c.Color = pr.LockColor ? pr.Color : c.PreferredColor;
		}
	}
}

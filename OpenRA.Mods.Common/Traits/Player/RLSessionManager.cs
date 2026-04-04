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
			//    Serialized via MapCacheLock: ToMap() opens the underlying ZipFile package;
			//    concurrent ToMap() calls on the same MapPreview race on ZipFile.GetEnumerator()
			//    during ComputeUID, causing ObjectDisposedException.
			Map map;
			lock (MapCacheLock)
				map = mapPreview.ToMap();
			Log.Write("rl-bridge", $"Session {sessionId}: Map loaded from {mapPreview.Path}, {map.ActorDefinitions.Count()} actor defs");

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
				orderManager.StartGame();
			}

			var world = orderManager.World;

			// 7. Register session state IMMEDIATELY after world is ready, BEFORE
			// the bridge becomes visible to gRPC. This prevents a race where
			// FastAdvance finds the bridge (via WaitForBridge) but SessionStates
			// hasn't been populated yet, causing NOT_FOUND.
			SessionStates[sessionId] = new SessionState(orderManager, world);

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

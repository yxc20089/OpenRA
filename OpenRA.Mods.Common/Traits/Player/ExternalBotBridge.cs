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
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenRA.Traits;
using RLProto = OpenRA.Mods.Common.RL;

namespace OpenRA.Mods.Common.Traits
{
	[Desc("External bot bridge for reinforcement learning agents. " +
		"Hosts a gRPC server that streams observations and receives actions.")]
	[TraitLocation(SystemActors.Player)]
	public sealed class ExternalBotBridgeInfo : TraitInfo, IBotInfo
	{
		[FieldLoader.Require]
		[Desc("Internal id for this bot.")]
		public readonly string Type = null;

		[FluentReference]
		[Desc("Human-readable name this bot uses.")]
		public readonly string Name = null;

		[Desc("gRPC port for the RL agent to connect to.")]
		public readonly int Port = 9999;

		[Desc("How many game ticks between observations sent to the agent. " +
			"1 = every tick, 10 = every 10th tick.")]
		public readonly int ObservationInterval = 1;

		string IBotInfo.Type => Type;
		string IBotInfo.Name => Name;

		public override object Create(ActorInitializer init) { return new ExternalBotBridge(this, init); }
	}

	public sealed class ExternalBotBridge : ITick, IBot, INotifyCreated
	{
		/// <summary>
		/// Thread-safe session registry. In multi-session mode, multiple bridges
		/// coexist in one process, each identified by its session/episode ID.
		/// In single-session (legacy) mode, there's exactly one entry.
		/// </summary>
		internal static readonly ConcurrentDictionary<string, ExternalBotBridge> Sessions = new();

		/// <summary>
		/// True when running in multi-session mode (RLSessionManager manages lifecycle).
		/// When false, the bridge manages its own gRPC server and calls Game.Exit() on teardown.
		/// </summary>
		internal static volatile bool MultiSessionMode;

		/// <summary>
		/// In multi-session mode, set this before World creation to assign
		/// a specific session ID to the next bridge that gets constructed.
		/// ThreadStatic so multiple init threads can create Worlds concurrently.
		/// </summary>
		[ThreadStatic]
		internal static string NextSessionId;

		static WebApplication grpcApp;
		static bool grpcServerStarted;
		static readonly object GrpcLock = new();

		public volatile bool IsEnabled;

		readonly ExternalBotBridgeInfo info;
		readonly World world;
		readonly Queue<Order> orders = new();
		readonly string episodeId;

		Player player;
		ObservationSerializer observationSerializer;
		ActionHandler actionHandler;

		// Channels for async communication between game thread and gRPC stream.
		// DropOldest ensures the game thread never blocks:
		//   - Observations: capacity=1, latest overwrites stale (agent always gets freshest state)
		//   - Actions: capacity=16, game drains all pending each tick
		readonly Channel<RLProto.GameObservation> observationChannel =
			Channel.CreateBounded<RLProto.GameObservation>(
				new BoundedChannelOptions(1)
				{
					FullMode = BoundedChannelFullMode.DropOldest,
					SingleWriter = true,
					SingleReader = true,
				});

		readonly Channel<RLProto.AgentAction> actionChannel =
			Channel.CreateBounded<RLProto.AgentAction>(
				new BoundedChannelOptions(16)
				{
					FullMode = BoundedChannelFullMode.DropOldest,
					SingleWriter = true,
					SingleReader = true,
				});

		volatile bool agentConnected;
		bool connectionLostHandled;

		// Fast-forward: when > 0, game runs at max speed until this tick is reached.
		// Volatile: written by gRPC thread in RequestFastAdvance, read by game thread in Tick.
		volatile int pendingFastAdvanceTarget;

		// Unary FastAdvance: TaskCompletionSource completed when target tick is reached.
		// Must be set BEFORE pendingFastAdvanceTarget (volatile write provides release fence).
		volatile TaskCompletionSource<RLProto.GameObservation> pendingAdvanceResult;

		// Server-side interrupt detection state
		volatile int interruptCheckInterval;  // check every N ticks (0 = disabled)
		int interruptNextCheckTick;           // next tick to run interrupt checks
		int fastAdvanceStartTick;             // tick when current advance started
		HashSet<string> enabledInterrupts = new();

		// Previous-state tracking for delta-based interrupt detection
		readonly HashSet<uint> prevVisibleEnemyIds = new();
		readonly HashSet<uint> prevOwnUnitIds = new();
		readonly Dictionary<uint, float> prevUnitHpPct = new();
		readonly HashSet<uint> prevEnemyBuildingIds = new();
		readonly HashSet<uint> prevOwnBuildingIds = new();
		float prevExploredPct;

		// Reusable collections for CheckInterrupts() (avoid GC pressure)
		readonly HashSet<uint> curEnemyIds = new();
		readonly HashSet<uint> curOwnUnitIds = new();
		readonly Dictionary<uint, float> curOwnUnitHp = new();
		readonly HashSet<uint> curEnemyBuildingIds = new();
		readonly HashSet<uint> curOwnBuildingIds = new();

		// Track which own units were moving (not idle) for unit_arrived detection
		readonly HashSet<uint> prevMovingUnitIds = new();

		// Cached actor references: populated once at advance start via SnapshotActors(),
		// then reused for fast delta checks. Full refresh every 4th check to catch
		// newly spawned actors (harvesters, produced units). This avoids expensive
		// ActorsHavingTrait<T>() enumeration on every check across 64 sessions.
		readonly List<Actor> cachedMobileActors = new();
		readonly List<Actor> cachedBuildingActors = new();
		int interruptCheckCount;
		const int FullRefreshEveryNChecks = 4;

		string pendingInterruptReason;

		/// <summary>
		/// Signaled when the session is done (game over or destroyed). Used by
		/// RLSessionManager to know when to stop the tick loop for this session.
		/// </summary>
		internal readonly ManualResetEventSlim SessionDone = new(false);

		/// <summary>
		/// Signaled when the game thread should wake up and tick (e.g. FastAdvance queued).
		/// Auto-resets after each wait so the thread blocks again when idle.
		/// </summary>
		internal readonly AutoResetEvent TickRequested = new(false);

		IBotInfo IBot.Info => info;
		Player IBot.Player => player;

		/// <summary>Session/episode ID used as the routing key for gRPC calls.</summary>
		public string SessionId => episodeId;

		public ExternalBotBridge(ExternalBotBridgeInfo info, ActorInitializer init)
		{
			this.info = info;
			world = init.World;

			// In multi-session mode, use the pre-assigned session ID so the bridge
			// registers under the correct ID for gRPC routing from the start.
			if (MultiSessionMode && !string.IsNullOrEmpty(NextSessionId))
				episodeId = NextSessionId;
			else
				episodeId = Guid.NewGuid().ToString("N")[..12];
		}

		void INotifyCreated.Created(Actor self)
		{
			// gRPC server is started in Activate() (single-session) or by RLSessionManager (multi-session)
		}

		/// <summary>
		/// Start the gRPC server. Called from Activate() in single-session mode,
		/// or from RLSessionManager.StartGrpcServer() in multi-session mode.
		/// </summary>
		internal static void StartGrpcServer(int port)
		{
			lock (GrpcLock)
			{
				if (grpcServerStarted)
					return;

				grpcServerStarted = true;
			}

			try
			{
				var builder = WebApplication.CreateBuilder(new WebApplicationOptions
				{
					Args = []
				});

				builder.WebHost.ConfigureKestrel(options =>
				{
					options.ListenAnyIP(port, listenOptions =>
						listenOptions.Protocols = HttpProtocols.Http2);
				});

				builder.Services.AddGrpc();

				// Suppress all ASP.NET Core console logging
				builder.Logging.ClearProviders();

				grpcApp = builder.Build();
				grpcApp.MapGrpcService<RLBridgeService>();

				Log.Write("rl-bridge", $"gRPC server starting on port {port}");
				grpcApp.Run();
			}
			catch (Exception e)
			{
				Log.Write("rl-bridge", $"gRPC server failed: {e}");
				lock (GrpcLock)
				{
					grpcServerStarted = false;
				}
			}
		}

		public void Activate(Player p)
		{
			if (p.World.IsReplay)
				return;

			IsEnabled = true;
			player = p;
			observationSerializer = new ObservationSerializer(world, player, episodeId);
			actionHandler = new ActionHandler(world, player);

			// Register in the session registry
			Sessions[episodeId] = this;

			// Pause game until RL agent connects (gives LLM time to plan)
			if (MultiSessionMode)
			{
				// In multi-session mode, use SetLocalPauseState to avoid queuing
				// a PauseGame Order that would re-pause during the first tick.
				world.SetLocalPauseState(true);
			}
			else
			{
				// In single-session mode, use SetPauseState so the pause order
				// is recorded in the replay for correct playback.
				world.SetPauseState(true);
			}

			Log.Write("rl-bridge", "Game paused — waiting for RL agent to connect");

			Log.Write("rl-bridge", $"ExternalBotBridge activated for player {p.InternalName}, session {episodeId}");

			// In single-session (legacy) mode, start gRPC server from here
			if (!MultiSessionMode)
			{
				var port = info.Port;
				var envPort = Environment.GetEnvironmentVariable("RL_GRPC_PORT");
				if (!string.IsNullOrEmpty(envPort) && int.TryParse(envPort, out var pp))
					port = pp;

				var thread = new Thread(() => StartGrpcServer(port))
				{
					IsBackground = true,
					Name = "RL-Bridge-gRPC"
				};
				thread.Start();
			}
		}

		/// <summary>
		/// Deactivate and clean up this session. In multi-session mode, this replaces
		/// Game.Exit() — only this session is torn down, not the entire process.
		/// </summary>
		internal void Deactivate()
		{
			Sessions.TryRemove(episodeId, out _);
			SessionDone.Set();
			TickRequested.Set(); // Wake tick loop so it can exit

			// Complete any pending FastAdvance so the gRPC call doesn't hang forever
			pendingAdvanceResult?.TrySetCanceled();

			Log.Write("rl-bridge", $"Session {episodeId} deactivated");
		}

		void IBot.QueueOrder(Order order)
		{
			orders.Enqueue(order);
		}

		bool gameOverHandled;

		void ITick.Tick(Actor self)
		{
			if (!IsEnabled || self.World.IsLoadingGameSave)
				return;

			// Detect game over in Tick (IGameOver.GameOver doesn't fire on Player traits)
			if (!gameOverHandled && world.IsGameOver)
			{
				gameOverHandled = true;
				Log.Write("rl-bridge", $"Game over detected at tick {world.WorldTick}, session {episodeId}");

				// Complete any pending FastAdvance so the gRPC call returns immediately
				// instead of hanging until the 300s deadline.
				var gameOverTcs = pendingAdvanceResult;
				if (gameOverTcs != null)
				{
					try
					{
						var obs = observationSerializer.Serialize(world.WorldTick);
						obs.Done = true;
						obs.Result = player.WinState == WinState.Won ? "win" : "lose";
						gameOverTcs.TrySetResult(obs);
					}
					catch (Exception e)
					{
						gameOverTcs.TrySetException(e);
					}

					pendingAdvanceResult = null;
					pendingFastAdvanceTarget = 0;
					world.SetTickScale(1.0f);
				}

				if (MultiSessionMode)
				{
					// In multi-session mode, just deactivate — don't exit the process
					Deactivate();
				}
				else
				{
					// Legacy single-session mode: exit the whole process
					new Thread(() =>
					{
						Thread.Sleep(3000);
						Log.Write("rl-bridge", "Exiting game process to finalize replay");
						Game.Exit();
					})
					{
						IsBackground = true,
						Name = "RL-Bridge-GameOver"
					}.Start();
				}
			}

			// Detect internal connection loss (TCP between client and embedded server)
			if (!connectionLostHandled && !world.IsConnectionAlive)
			{
				connectionLostHandled = true;
				Log.Write("rl-bridge", $"Internal connection lost at tick {world.WorldTick}! Session {episodeId}.");

				// Complete any pending FastAdvance so gRPC doesn't hang
				var connLostTcs = pendingAdvanceResult;
				if (connLostTcs != null)
				{
					connLostTcs.TrySetCanceled();
					pendingAdvanceResult = null;
					pendingFastAdvanceTarget = 0;
					world.SetTickScale(1.0f);
				}

				if (MultiSessionMode)
				{
					Deactivate();
				}
				else
				{
					new Thread(() =>
					{
						Thread.Sleep(500);
						Log.Write("rl-bridge", "Exiting game process due to connection loss");
						Game.Exit();
					})
					{
						IsBackground = true,
						Name = "RL-Bridge-ConnLost"
					}.Start();
				}

				return;
			}

			// Check for server-side interrupts during fast-forward
			if (pendingFastAdvanceTarget > 0 && interruptCheckInterval > 0
				&& world.WorldTick >= interruptNextCheckTick
				&& pendingInterruptReason == null)
			{
				interruptNextCheckTick = world.WorldTick + interruptCheckInterval;
				try
				{
					var signal = CheckInterrupts();
					if (signal != null)
					{
						pendingInterruptReason = signal;
						// Stop advancing — complete on the next Tick check below
						pendingFastAdvanceTarget = world.WorldTick;
						Console.Error.WriteLine($"[rl-bridge] Interrupt [{signal}] at tick {world.WorldTick} (started at {fastAdvanceStartTick})");
					}
				}
				catch (Exception e)
				{
					Console.Error.WriteLine($"[rl-bridge] CheckInterrupts ERROR at tick {world.WorldTick}: {e}");
					// Disable further checks to avoid repeated errors
					interruptCheckInterval = 0;
				}
			}

			// Check if fast-forward target tick has been reached (or interrupted)
			if (pendingFastAdvanceTarget > 0 && world.WorldTick >= pendingFastAdvanceTarget)
			{
				world.SetTickScale(1.0f);
				var reason = pendingInterruptReason;
				if (reason != null)
					Log.Write("rl-bridge", $"Fast-forward interrupted at tick {world.WorldTick} by [{reason}]");
				else
					Log.Write("rl-bridge", $"Fast-forward complete at tick {world.WorldTick}, restored normal speed");
				pendingFastAdvanceTarget = 0;

				// Complete unary FastAdvance if pending
				var advTcs = pendingAdvanceResult;
				if (advTcs != null)
				{
					try
					{
						var obs = observationSerializer.Serialize(world.WorldTick);
						// Set interrupt fields
						if (reason != null)
						{
							obs.Interrupted = true;
							obs.InterruptReason = reason;
						}
						obs.ActualTicksAdvanced = world.WorldTick - fastAdvanceStartTick;

						// Set explored_percent
						var shroud = player?.Shroud;
						if (shroud != null)
						{
							var totalCells = world.Map.AllCells.Count();
							var exploredCells = world.Map.AllCells.Count(c => shroud.IsExplored(c));
							obs.ExploredPercent = totalCells > 0 ? (float)exploredCells / totalCells * 100f : 0f;
						}

						advTcs.TrySetResult(obs);
					}
					catch (Exception e)
					{
						advTcs.TrySetException(e);
					}

					pendingAdvanceResult = null;
					pendingInterruptReason = null;
				}
			}

			try
			{
				// Process all pending actions from agent (non-blocking drain)
				while (actionChannel.Reader.TryRead(out var agentAction))
				{
					// Intercept FAST_ADVANCE before ActionHandler
					foreach (var cmd in agentAction.Commands)
					{
						if (cmd.Action == RLProto.ActionType.FastAdvance)
						{
							var ticks = Math.Max(1, Math.Min(cmd.Ticks, 5000));
							pendingFastAdvanceTarget = world.WorldTick + ticks;
							world.SetTickScale(0.025f);
							Log.Write("rl-bridge", $"Fast-forward: {ticks} ticks from {world.WorldTick} to {pendingFastAdvanceTarget} (tickScale=0.025)");
						}
					}

					actionHandler.ProcessAction(agentAction, this);
				}
			}
			catch (ChannelClosedException)
			{
				Log.Write("rl-bridge", "Agent disconnected (action channel closed)");
				agentConnected = false;
			}

			// Issue any queued orders
			while (orders.Count > 0)
				world.IssueOrder(orders.Dequeue());

			// Only send observations at the configured interval
			if (world.WorldTick % info.ObservationInterval != 0)
				return;

			if (!agentConnected)
				return;

			try
			{
				// Serialize and push observation (DropOldest — never blocks)
				var observation = observationSerializer.Serialize(world.WorldTick);
				observationChannel.Writer.TryWrite(observation);
			}
			catch (ChannelClosedException)
			{
				Log.Write("rl-bridge", "Agent disconnected (observation channel closed)");
				agentConnected = false;
			}
			catch (Exception e)
			{
				Log.Write("rl-bridge", $"Error serializing observation: {e}");
			}
		}

		/// <summary>
		/// Called by RLBridgeService to signal that an agent has connected.
		/// </summary>
		internal void OnAgentConnected()
		{
			agentConnected = true;

			// Use SetLocalPauseState for immediate effect — SetPauseState issues an Order
			// which requires the game loop to process it, but Tick() doesn't run while
			// Paused is true, creating a deadlock for custom maps where loading is slow.
			world.SetLocalPauseState(false);
			Log.Write("rl-bridge", "RL agent connected — game unpaused");
		}

		/// <summary>
		/// Called by RLBridgeService to signal that the agent has disconnected.
		/// In multi-session mode, deactivates the session. In legacy mode, exits the process.
		/// </summary>
		internal void OnAgentDisconnected()
		{
			agentConnected = false;

			if (MultiSessionMode)
			{
				Log.Write("rl-bridge", $"RL agent disconnected from session {episodeId}");
				Deactivate();
			}
			else
			{
				Log.Write("rl-bridge", "RL agent disconnected, scheduling graceful exit");
				new Thread(() =>
				{
					Thread.Sleep(2000);
					Log.Write("rl-bridge", "Exiting game process to finalize replay");
					Game.Exit();
				})
				{
					IsBackground = true,
					Name = "RL-Bridge-Exit"
				}.Start();
			}
		}

		/// <summary>
		/// Channel for the game thread to push observations to the gRPC stream.
		/// </summary>
		internal ChannelReader<RLProto.GameObservation> ObservationReader => observationChannel.Reader;

		/// <summary>
		/// Channel for the gRPC stream to push actions to the game thread.
		/// </summary>
		internal ChannelWriter<RLProto.AgentAction> ActionWriter => actionChannel.Writer;

		/// <summary>
		/// Snapshot all Mobile and Building actors into cached lists.
		/// Called once at advance start and refreshed every FullRefreshEveryNChecks.
		/// </summary>
		void SnapshotActors()
		{
			cachedMobileActors.Clear();
			cachedMobileActors.AddRange(world.ActorsHavingTrait<Mobile>());
			cachedBuildingActors.Clear();
			cachedBuildingActors.AddRange(world.ActorsHavingTrait<Building>());
		}

		/// <summary>
		/// Check all enabled interrupt signals against current world state.
		/// Returns the signal name if one fires, null otherwise.
		/// Uses cached actor references for fast iteration (no trait dictionary walk).
		/// Full trait re-enumeration every 4th check to catch newly spawned actors.
		/// </summary>
		string CheckInterrupts()
		{
			if (observationSerializer == null)
				return null;

			// Periodically refresh cached actor lists to catch new spawns (harvesters, produced units)
			interruptCheckCount++;
			if (interruptCheckCount >= FullRefreshEveryNChecks)
			{
				SnapshotActors();
				interruptCheckCount = 0;
			}

			// Build current state from cached actor references (fast — no trait lookup)
			curEnemyIds.Clear();
			curOwnUnitIds.Clear();
			curOwnUnitHp.Clear();
			curEnemyBuildingIds.Clear();
			curOwnBuildingIds.Clear();

			var shroud = player?.Shroud;

			foreach (var a in cachedMobileActors)
			{
				if (a.IsDead || !a.IsInWorld)
					continue;

				var owner = a.Owner;
				if (owner == player)
				{
					curOwnUnitIds.Add(a.ActorID);
					var health = a.TraitOrDefault<Health>();
					if (health != null)
						curOwnUnitHp[a.ActorID] = (float)health.HP / health.MaxHP;
				}
				else if (!owner.NonCombatant
					&& (shroud == null || shroud.IsVisible(a.CenterPosition)))
				{
					curEnemyIds.Add(a.ActorID);
				}
			}

			foreach (var a in cachedBuildingActors)
			{
				if (a.IsDead || !a.IsInWorld)
					continue;

				if (a.Owner == player)
					curOwnBuildingIds.Add(a.ActorID);
				else if (!a.Owner.NonCombatant
					&& (shroud == null || shroud.IsVisible(a.CenterPosition)))
					curEnemyBuildingIds.Add(a.ActorID);
			}

			float exploredPct = prevExploredPct;

			// On first check of a new advance, just populate prev-state without firing.
			bool isFirstCheck = prevVisibleEnemyIds.Count == 0 && prevOwnUnitIds.Count == 0;
			if (isFirstCheck)
			{
				UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
					curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
				return null;
			}

			// Check signals in priority order
			if (enabledInterrupts.Contains("game_over") && world.IsGameOver)
			{
				UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
					curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
				return "game_over";
			}

			if (enabledInterrupts.Contains("enemy_spotted"))
			{
				foreach (var id in curEnemyIds)
				{
					if (!prevVisibleEnemyIds.Contains(id))
					{
						UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
							curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
						return "enemy_spotted";
					}
				}
			}

			if (enabledInterrupts.Contains("unit_destroyed"))
			{
				foreach (var id in prevOwnUnitIds)
				{
					if (!curOwnUnitIds.Contains(id))
					{
						UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
							curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
						return "unit_destroyed";
					}
				}
			}

			if (enabledInterrupts.Contains("under_attack"))
			{
				foreach (var kvp in prevUnitHpPct)
				{
					if (curOwnUnitHp.TryGetValue(kvp.Key, out var currentHp))
					{
						if (kvp.Value - currentHp > 0.05f)
						{
							UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
								curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
							return "under_attack";
						}
					}
				}
			}

			if (enabledInterrupts.Contains("building_discovered"))
			{
				foreach (var id in curEnemyBuildingIds)
				{
					if (!prevEnemyBuildingIds.Contains(id))
					{
						UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
							curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
						return "building_discovered";
					}
				}
			}

			if (enabledInterrupts.Contains("enemy_building_destroyed"))
			{
				foreach (var id in prevEnemyBuildingIds)
				{
					if (!curEnemyBuildingIds.Contains(id))
					{
						UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
							curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
						return "enemy_building_destroyed";
					}
				}
			}

			if (enabledInterrupts.Contains("own_building_destroyed"))
			{
				foreach (var id in prevOwnBuildingIds)
				{
					if (!curOwnBuildingIds.Contains(id))
					{
						UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
							curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
						return "own_building_destroyed";
					}
				}
			}

			// Priority 3: unit_arrived (was moving, now idle)
			if (enabledInterrupts.Contains("unit_arrived"))
			{
				foreach (var a in cachedMobileActors)
				{
					if (a.IsDead || !a.IsInWorld || a.Owner != player)
						continue;
					if (prevMovingUnitIds.Contains(a.ActorID) && a.IsIdle)
					{
						UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
							curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
						UpdateMovingUnits();
						return "unit_arrived";
					}
				}
			}

			// Priority 5: production_complete (own unit count increased)
			if (enabledInterrupts.Contains("production_complete"))
			{
				if (curOwnUnitIds.Count > prevOwnUnitIds.Count)
				{
					UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
						curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
					UpdateMovingUnits();
					return "production_complete";
				}
			}

			if (enabledInterrupts.Contains("exploration_milestone"))
			{
				var prevBucket = (int)(prevExploredPct / 10f);
				var curBucket = (int)(exploredPct / 10f);
				if (curBucket > prevBucket)
				{
					UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
						curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
					UpdateMovingUnits();
					return "exploration_milestone";
				}
			}

			UpdatePrevState(curEnemyIds, curOwnUnitIds, curOwnUnitHp,
				curEnemyBuildingIds, curOwnBuildingIds, exploredPct);
			UpdateMovingUnits();
			return null;
		}

		void UpdatePrevState(HashSet<uint> enemyIds, HashSet<uint> ownUnitIds,
			Dictionary<uint, float> ownUnitHp, HashSet<uint> enemyBuildingIds,
			HashSet<uint> ownBuildingIds, float exploredPct)
		{
			prevVisibleEnemyIds.Clear();
			prevVisibleEnemyIds.UnionWith(enemyIds);
			prevOwnUnitIds.Clear();
			prevOwnUnitIds.UnionWith(ownUnitIds);
			prevUnitHpPct.Clear();
			foreach (var kvp in ownUnitHp)
				prevUnitHpPct[kvp.Key] = kvp.Value;
			prevEnemyBuildingIds.Clear();
			prevEnemyBuildingIds.UnionWith(enemyBuildingIds);
			prevOwnBuildingIds.Clear();
			prevOwnBuildingIds.UnionWith(ownBuildingIds);
			prevExploredPct = exploredPct;
		}

		void UpdateMovingUnits()
		{
			prevMovingUnitIds.Clear();
			foreach (var a in cachedMobileActors)
			{
				if (!a.IsDead && a.IsInWorld && a.Owner == player && !a.IsIdle)
					prevMovingUnitIds.Add(a.ActorID);
			}
		}

		/// <summary>
		/// Unary FastAdvance: submit commands, fast-forward N ticks, return observation.
		/// In multi-session mode, acquires a worker slot and ticks inline on the
		/// calling (gRPC) thread. Returns RESOURCE_EXHAUSTED if all workers are busy.
		/// In single-session mode, routes through the action channel to the game thread.
		/// </summary>
		internal async Task<RLProto.GameObservation> RequestFastAdvance(
			int ticks, IEnumerable<RLProto.Command> commands, CancellationToken ct,
			int checkEventsEvery = 0, IEnumerable<string> enabledInterruptNames = null)
		{
			// Fail fast if session is already dead
			if (SessionDone.IsSet)
				throw new RpcException(new Status(StatusCode.Aborted,
					$"Session {episodeId} is no longer active"));

			// Connect agent if this is the first call (unpauses game)
			if (!agentConnected)
				OnAgentConnected();

			var n = Math.Max(1, Math.Min(ticks, 5000));

			// Configure server-side interrupt detection
			interruptCheckInterval = Math.Max(0, checkEventsEvery);
			fastAdvanceStartTick = world.WorldTick;
			interruptNextCheckTick = world.WorldTick + Math.Max(interruptCheckInterval, 1);
			pendingInterruptReason = null;
			interruptCheckCount = 0;
			enabledInterrupts.Clear();
			if (enabledInterruptNames != null)
				foreach (var name in enabledInterruptNames)
					enabledInterrupts.Add(name);

			// Snapshot actors once at advance start — CheckInterrupts uses cached lists
			// instead of expensive ActorsHavingTrait calls on every check
			if (interruptCheckInterval > 0)
				SnapshotActors();

			// Set up the TCS before queueing — worker will complete it via ITick.Tick.
			// Keep a local reference because ITick.Tick nulls the field after completion.
			var tcs = new TaskCompletionSource<RLProto.GameObservation>(
				TaskCreationOptions.RunContinuationsAsynchronously);
			pendingAdvanceResult = tcs;

			// Build a single AgentAction with user commands + FAST_ADVANCE
			var action = new RLProto.AgentAction();
			var cmds = commands.ToList();
			if (cmds.Count > 0)
				action.Commands.Add(cmds);

			action.Commands.Add(new RLProto.Command
			{
				Action = RLProto.ActionType.FastAdvance,
				Ticks = n,
			});
			actionChannel.Writer.TryWrite(action);

			if (MultiSessionMode)
			{
				// Wait briefly for session state — it may still be registering
				// if FastAdvance arrives right after the bridge activates during
				// World creation but before InitSession registers SessionStates.
				RLSessionManager.SessionState state = null;
				for (var retry = 0; retry < 50; retry++)
				{
					if (RLSessionManager.SessionStates.TryGetValue(episodeId, out state))
						break;
					Thread.Sleep(100);
				}

				if (state == null)
					throw new RpcException(new Status(StatusCode.NotFound,
						$"Session state not found for {episodeId}"));

				var workItem = RLSessionManager.SubmitWork(state, this);
				if (workItem == null)
					throw new RpcException(new Status(StatusCode.ResourceExhausted,
						"All worker slots busy, retry later"));

				using var reg = ct.Register(() =>
				{
					tcs.TrySetCanceled();
					workItem.Completed.TrySetCanceled();
				});

				// Wait for the worker to finish ticking (non-blocking await)
				await workItem.Completed.Task;
				return await tcs.Task;
			}
			else
			{
				// Legacy single-session mode: delegate to the game thread
				TickRequested.Set();

				using var reg = ct.Register(() => tcs.TrySetCanceled());
				return await tcs.Task;
			}
		}

		/// <summary>
		/// Get a snapshot of the current game state for unary GetState RPC.
		/// </summary>
		internal RLProto.GameState GetCurrentState()
		{
			var phase = !IsEnabled ? "waiting"
				: world.IsGameOver ? "game_over"
				: "playing";

			var winner = "";
			if (world.IsGameOver)
			{
				foreach (var p in world.Players)
				{
					if (p.WinState == WinState.Won)
					{
						winner = p.InternalName;
						break;
					}
				}
			}

			// Player and enemy faction
			var playerFaction = player?.Faction?.InternalName ?? "";
			var enemyFaction = "";
			foreach (var p in world.Players)
			{
				if (p != player && !p.NonCombatant && p.Playable)
				{
					enemyFaction = p.Faction?.InternalName ?? "";
					break;
				}
			}

			return new RLProto.GameState
			{
				EpisodeId = episodeId,
				Tick = world.WorldTick,
				Phase = phase,
				Winner = winner,
				PlayerCount = world.Players.Length,
				PlayerFaction = playerFaction,
				EnemyFaction = enemyFaction,
			};
		}

		/// <summary>
		/// Look up a session by ID. If sessionId is empty, returns the first (only) session
		/// for backward compatibility with single-session mode.
		/// </summary>
		internal static ExternalBotBridge LookupSession(string sessionId)
		{
			if (!string.IsNullOrEmpty(sessionId))
			{
				Sessions.TryGetValue(sessionId, out var bridge);
				return bridge;
			}

			// Legacy fallback: return first available session
			foreach (var kvp in Sessions)
				return kvp.Value;

			return null;
		}
	}
}

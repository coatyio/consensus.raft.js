/*! Copyright 2023 Siemens AG

   Licensed under the Apache License, Version 2.0 (the "License"); you may not
   use this file except in compliance with the License. You may obtain a copy of
   the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   License for the specific language governing permissions and limitations under
   the License.
*/

import { Controller } from "@coaty/core";
import { DbLocalContext } from "@coaty/core/db";
import { SqLiteNodeAdapter } from "@coaty/core/db/adapter-sqlite-node";
import { Observable, Subject } from "rxjs";

import { RaftNode } from "./raft-node";
import { RaftCommunication, RaftCommunicationController } from "./raft-node-interfaces/communication";
import { RaftPersistency, RaftPersistencyStore } from "./raft-node-interfaces/persistency";
import { RaftStateMachine } from "./raft-node-interfaces/state-machine";
import { ridnone } from "../ported/raftpb/raft.pb";

/**
 * Maintains a replicated state machine (RSM) with other `RaftController`
 * instances in the Coaty network using the Raft protocol. For that each
 * `RaftController` joins a specified Raft cluster. An instance of the RSM in
 * form of a {@linkcode RaftStateMachine} has to be provided in the
 * {@linkcode RaftControllerOptions} when bootstrapping this controller.
 *
 * A new `RaftController` instance needs to `connect()` to the Raft cluster
 * before it can modify/access the replicated state with the following
 * functions:
 *
 * - `propose(input)` proposes a new input that should be applied to the RSM.
 * - `getState()` returns the state of the RSM.
 * - `observeState()` returns an observable emitting on every RSM change.
 *
 * After a `RaftController` has connected to the Raft cluster it can monitor the
 * cluster configuration with the following functions:
 *
 * - `getClusterConfiguration()` returns the cluster configuration.
 * - `observeClusterConfiguration()` returns an observable emitting on every
 *   cluster configuration change.
 *
 * Before shutting down the Coaty container of this `RaftController` the user
 * has to either `disconnect()` from the Raft cluster or `stop()` this
 * controller.
 *
 * @category Raft Controller
 */
export class RaftController extends Controller {
    /**
     * Used to make input- and cluster-change-proposals to Raft. Is not null if
     * `this._connectionState` is "connected".
     */
    private _node: RaftNode | null = null;

    /** Raft cluster connection state of this controller. */
    private _connectionState:
        | "disconnected"
        | "connecting"
        | "connected"
        | "disconnecting"
        | "stopping"
        = "disconnected";

    /** Helpers for connect(), disconnect() and stop(). */
    private _connected: Subject<void>;
    private _disconnected: Subject<void>;
    private _stopped: Subject<void>;

    /**
     * Resolves once the next input proposal can be relayed to the underlying
     * `RaftNode`. Is used to serialize input proposals.
     */
    private _readyForInput: Promise<void> = Promise.resolve();

    /**
     * Counts the number of queued up input proposals. Is used to limit this
     * number to 1000.
     */
    private _numOfQueuedUpInputProposals = 0;

    /**
     * Used to access QueuedUpInputProposalsLimit.
     */
    private _raftConfig: Required<RaftConfiguration>;

    /**
     * Connects this `RaftController` to the Raft cluster. Users of this
     * `RaftController` need to wait til the returned promise is resolved before
     * they can propose inputs with `propose(...)`, observe the replicated state
     * with `getState()` and `observeState()`, and monitor the cluster
     * configuration with `getClusterConfiguration()` and
     * `observeClusterConfiguration()`.
     *
     * If this function is called after a crash and this `RaftController` was
     * connected to the Raft cluster before the crash, persisted data will be
     * used to restore this controller and reconnect to the Raft cluster.
     *
     * @remarks The `RaftController` with
     * {@linkcode RaftControllerOptions.shouldCreateCluster} set to `true` will
     * create a new Raft cluster on initial startup. Trying to connect a
     * `RaftController` with this option set to `false` will only resolve after
     * a Raft cluster has been created and a majority of the existing cluster
     * has agreed on letting this `RaftController` join.
     *
     * @returns A promise that resolves once this `RaftController` has connected
     * to the Raft cluster.
     *
     * @throws An {@linkcode InvalidControllerOptionsError} if the
     * {@linkcode RaftControllerOptions} provided when bootstrapping this
     * controller are invalid or missing.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is currently in the process of disconnecting or
     * stopping.
     *
     * @category Raft
     */
    async connect(): Promise<void> {
        switch (this._connectionState) {
            case "connected":
                return;
            case "disconnected":
                // Start new connection process
                this._connectionState = "connecting";
                this._connected = new Subject<void>();

                // Components needed to create RaftNode
                const id = this.options["id"];
                const raftStateMachine = this.options["stateMachine"];
                const shouldCreateCluster = this.options["shouldCreateCluster"];
                const cluster = this.options["cluster"] ?? "";
                const databaseKey = this.options["databaseKey"] ?? "raftdb";
                const configuration = this.options["configuration"] ?? {};

                // Check if RaftControllerOptions are valid
                if (
                    typeof id !== "string" || id === ridnone ||
                    raftStateMachine === null || typeof raftStateMachine !== "object" || Array.isArray(raftStateMachine) ||
                    typeof shouldCreateCluster !== "boolean" ||
                    typeof cluster !== "string" ||
                    typeof databaseKey !== "string" ||
                    typeof configuration !== "object" || Array.isArray(configuration)
                ) {
                    throw new InvalidControllerOptionsError();
                }

                // Fill up configuration with default values where necessary
                this._raftConfig = this._getConfigurationWithDefaults(configuration);

                const raftCommunication = this.getRaftCommunicationImplementation();
                const raftPersistency = this.getRaftPersistencyImplementation();

                // Initialize _node and connect to new or existing Raft cluster
                this._node = new RaftNode(
                    id,
                    cluster,
                    shouldCreateCluster,
                    raftStateMachine,
                    raftCommunication,
                    raftPersistency,
                    this._raftConfig
                );
                await this._node.connect();
                this._connectionState = "connected";

                // Signal that connection process is finished
                this._connected.next();
                this._connected.complete();
                return this._connected.toPromise();
            case "connecting":
                return this._connected.toPromise();
            case "disconnecting":
            case "stopping":
                throw new OperationNotSupportedInCurrentConnectionStateError(
                    "connect",
                    this._connectionState
                );
        }
    }

    /**
     * Proposes the given `input` to Raft. Raft will try to add the input to a
     * log shared between all `RaftController`s in the Raft cluster. If the
     * input is accepted into the log it counts as committed and is applied to
     * the RSM. The returned promise resolves to the resulting state of the RSM
     * after the proposed input has been applied.
     *
     * A majority of controllers need to give their consent before the given
     * input is accepted into the log. This might take indefinitely if no
     * majority can be reached, e.g. if too many agents have crashed and cannot
     * recover. In this case the returned promise is never resolved or rejected.
     *
     * @remark This function implements reproposed inputs as explained inside
     * the Raft thesis. Manual reproposing by the user is not intended. That is
     * why the returned promise never times out.
     *
     * @remark The object structure of input and resulting state can be chosen
     * by the user and depends on the implementation of the RSM that is provided
     * in the {@linkcode RaftControllerOptions} of this controller. Both input
     * and resulting state have to be of type {@linkcode RaftData}.
     *
     * @param input Input that should be committed and applied to the RSM. Has
     * to be of type {@linkcode RaftData}.
     *
     * @returns A promise resolving to the resulting state of the RSM after the
     * proposed input has been applied if it ever gets applied. Is of type
     * {@linkcode RaftData}. Is rejected if this `RaftController` is stopped or
     * disconnected before it could make sure that the input was committed. The
     * input might still be committed.
     *
     * @throws A {@linkcode TooManyQueuedUpInputProposalsError} if there are
     * more than 1000 input proposals queued up that haven't been committed yet.
     *
     * @throws A {@linkcode DisconnectBeforeOperationCompleteError} if this
     * controller is disconnected from the cluster before it could make sure
     * that the proposed input was committed.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is not connected to the Raft cluster, `this.connect()`
     * needs to be called first.
     *
     * @category Raft
     */
    async propose(input: RaftData): Promise<RaftData> {
        if (this._connectionState !== "connected") {
            throw new OperationNotSupportedInCurrentConnectionStateError(
                "propose",
                this._connectionState
            );
        } else {
            if (this._numOfQueuedUpInputProposals >= this._raftConfig.queuedUpInputProposalsLimit) {
                throw new TooManyQueuedUpInputProposalsError();
            }
            this._numOfQueuedUpInputProposals++;
            return new Promise<RaftData>((resolve, reject) => {
                // This RaftController should only propose one input at a time
                this._readyForInput = new Promise<void>((res) => {
                    this._readyForInput.then(() => {
                        if (this._connectionState !== "connected") {
                            // Disconnect before propose
                            reject(new DisconnectBeforeOperationCompleteError("propose"));
                            res();
                            this._numOfQueuedUpInputProposals--;
                        } else {
                            this._node!.propose(input)
                                .then((state) => {
                                    // Successful commit
                                    resolve(state);
                                    res();
                                    this._numOfQueuedUpInputProposals--;
                                })
                                .catch((_) => {
                                    // Disconnect before commit
                                    reject(new DisconnectBeforeOperationCompleteError("propose"));
                                    res();
                                    this._numOfQueuedUpInputProposals--;
                                });
                        }
                    });
                });
            });
        }
    }

    /**
     * Returns a promise resolving to the state of the RSM. The state is
     * retrieved by proposing a NOOP and waiting for its resulting state. The
     * NOOP is being proposed internally and therefore cannot be observed by
     * `observeState()`.
     *
     * @remark The returned state is guaranteed to be up to date or newer
     * respective to the point in time when this function was called. In other
     * words calling `getState()` at time t will return the distributed state
     * from some point in time inside [t, ∞]. This means that all inputs
     * committed before t are guaranteed to already be applied in this state.
     * Newer inputs that were committed after t might also already be applied.
     *
     * @returns A promise resolving to the state of the RSM. The state will be
     * of type {@linkcode RaftData}.
     *
     * @throws A {@linkcode DisconnectBeforeOperationCompleteError} if this
     * controller is disconnected from the cluster before the state could be
     * retrieved.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is not connected to the Raft cluster, `this.connect()`
     * needs to be called first.
     *
     * @category Raft
     */
    async getState(): Promise<RaftData> {
        if (this._connectionState !== "connected") {
            throw new OperationNotSupportedInCurrentConnectionStateError(
                "getState",
                this._connectionState
            );
        } else {
            return this._node!.getState()
                .catch((_) => {
                    throw new DisconnectBeforeOperationCompleteError("getState");
                });
        }
    }

    /**
     * Returns an observable that emits the state of the RSM on every state
     * update as soon as the update becomes known to this controller. State
     * updates are triggered when a new input is committed.
     *
     * The returned observable completes and its subscriptions are
     * **automatically unsubscribed** once the controller is disconnected or
     * stopped, in order to release system resources and to avoid memory leaks.
     *
     * @returns an observable emitting the state of the RSM on every state
     * update. The emitted state will be of type {@linkcode RaftData}.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is not connected to the Raft cluster, `this.connect()`
     * needs to be called first.
     *
     * @category Raft
     */
    observeState(): Observable<RaftData> {
        if (this._connectionState !== "connected") {
            throw new OperationNotSupportedInCurrentConnectionStateError(
                "observeState",
                this._connectionState
            );
        } else {
            return this._node!.observeState();
        }
    }

    /**
     * Returns a promise resolving to the cluster configuration. The cluster
     * configuration contains the ids of all `RaftController`s in the cluster.
     *
     * @remark The returned configuration is guaranteed to be up to date or
     * newer respective to the point in time when this function was called. In
     * other words calling `getClusterConfiguration()` at time t will return the
     * cluster configuration from some point in time inside [t, ∞]. This means
     * that all connects and disconnects that finished before t are guaranteed
     * to already be applied in this configuration. Newer connects and
     * disconnects that finished after t might also already be applied.
     *
     * @returns A promise resolving to the cluster configuration. The cluster
     * configuration contains the ids of all `RaftController`s in the cluster.
     *
     * @throws A {@linkcode DisconnectBeforeOperationCompleteError} if this
     * controller disconnected before the cluster configuration could be
     * retrieved.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is not connected to the Raft cluster, `this.connect()`
     * needs to be called first.
     *
     * @category Raft
     */
    async getClusterConfiguration(): Promise<string[]> {
        if (this._connectionState !== "connected") {
            throw new OperationNotSupportedInCurrentConnectionStateError(
                "getClusterConfiguration",
                this._connectionState
            );
        } else {
            return this._node!.getClusterConfiguration()
                .catch((_) => {
                    throw new DisconnectBeforeOperationCompleteError("getClusterConfiguration");
                });
        }
    }

    /**
     * Returns an observable that emits the cluster configuration on every
     * configuration change as soon as the change becomes known to this
     * controller. Cluster configuration changes are triggered when a
     * `RaftController` connects to or disconnects from the Raft cluster.
     *
     * The returned observable completes and its subscriptions are
     * **automatically unsubscribed** once the controller is disconnected or
     * stopped, in order to release system resources and to avoid memory leaks.
     *
     * @returns an observable emitting the cluster configuration on every
     * configuration change. The cluster configuration contains the ids of all
     * `RaftController`s in the cluster.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is not connected to the Raft cluster, `this.connect()`
     * needs to be called first.
     *
     * @category Raft
     */
    observeClusterConfiguration(): Observable<string[]> {
        if (this._connectionState !== "connected") {
            throw new OperationNotSupportedInCurrentConnectionStateError(
                "observeClusterConfiguration",
                this._connectionState
            );
        } else {
            return this._node!.observeClusterConfiguration();
        }
    }

    /**
     * Disconnects this `RaftController` from the Raft cluster therefore
     * removing it from the cluster configuration. Users of this
     * `RaftController` need to wait til the returned promise is resolved before
     * they can shutdown the corresponding Coaty container. Deletes all
     * persisted state of this controller. The shared state of a Raft cluster is
     * lost after all `RaftControllers` of that cluster have disconnected.
     *
     * @returns A promise that resolves once this `RaftController` has
     * disconnected.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is currently in the process of connecting or stopping.
     *
     * @category Raft
     */
    async disconnect(): Promise<void> {
        switch (this._connectionState) {
            case "disconnected":
                return;
            case "connected":
                // Start new disconnection process
                this._connectionState = "disconnecting";
                this._disconnected = new Subject<void>();

                // Request disconnect from cluster
                await this._node!.disconnect(false);
                this._node = null;
                this._connectionState = "disconnected";

                // Signal that disconnection process is finished
                this._disconnected.next();
                this._disconnected.complete();
                return this._disconnected.toPromise();
            case "disconnecting":
                return this._disconnected.toPromise();
            case "connecting":
            case "stopping":
                throw new OperationNotSupportedInCurrentConnectionStateError(
                    "disconnect",
                    this._connectionState
                );
        }
    }

    /**
     * Stops this `RaftController` WITHOUT disconnecting from the Raft cluster
     * and WITHOUT deleting the persisted state. Users of this `RaftController`
     * need to wait til the returned promise is resolved before they can
     * shutdown the corresponding Coaty container.
     *
     * Users can use this function instead of `disconnect()` if they want to
     * shutdown this controller for some time without losing the current shared
     * state and cluster configuration. This might be useful if e.g. the entire
     * cluster has to shutdown for a few days but its state and configuration
     * should be preserved. `stop()` can also be seen as a graceful crash.
     *
     * @remark If at least half of all `RaftControllers` in the Raft cluster are
     * stopped the cluster can no longer make any progress til enough stopped
     * controllers reconnect using `connect()`.
     *
     * @returns A promise that resolves once this `RaftController` has been
     * stopped.
     *
     * @throws An {@linkcode OperationNotSupportedInCurrentConnectionStateError}
     * if this controller is currently in the process of connecting or
     * disconnecting.
     *
     * @category Raft
     */
    async stop(): Promise<void> {
        switch (this._connectionState) {
            case "disconnected":
                return;
            case "connected":
                // Start new stopping process
                this._connectionState = "stopping";
                this._stopped = new Subject<void>();

                // Stop RaftNode without disconnecting from cluster
                await this._node!.disconnect(true);
                this._node = null;
                this._connectionState = "disconnected";

                // Signal that stopping process is finished
                this._stopped.next();
                this._stopped.complete();
                return this._stopped.toPromise();
            case "stopping":
                return this._stopped.toPromise();
            case "connecting":
            case "disconnecting":
                throw new OperationNotSupportedInCurrentConnectionStateError(
                    "stop",
                    this._connectionState
                );
        }
    }

    /**
     * Can be overwritten to change the {@linkcode RaftPersistency}
     * implementation used by Raft.
     *
     * The default implementation persists Raft data using Coaty's SQL-based
     * local storage in the form of a local SQLite database file. This database
     * must be referenced in Coaty `Configuration.databaseOptions` under the key
     * specified by RaftControllerOptions.databaseKey (or "raftdb" if not
     * specified) using the adapter named "SqLiteNodeAdapter". Make sure the
     * path to the database file (i.e. connectionString) exists and is
     * accessible.
     *
     * @category Raft Interfaces
     */
    protected getRaftPersistencyImplementation(): RaftPersistency {
        const connectionInfo = this.runtime.databaseOptions[this.options.databaseKey ?? "raftdb"];
        const dbContext = new DbLocalContext(connectionInfo, SqLiteNodeAdapter);
        return new RaftPersistencyStore(dbContext);
    }

    /**
     * Can be overwritten to change the {@linkcode RaftCommunication}
     * implementation used by Raft.
     *
     * @category Raft Interfaces
     */
    protected getRaftCommunicationImplementation(): RaftCommunication {
        const communicationController = this.container.registerController(
            "RaftCommunicationController",
            RaftCommunicationController
        );
        return communicationController;
    }

    private _getConfigurationWithDefaults(configuration: RaftConfiguration): Required<RaftConfiguration> {
        const optionalDefaults: Required<RaftConfiguration> = {
            electionTick: 10,
            heartbeatTick: 1,
            maxSizePerMsg: 1024 * 1024,
            maxCommittedSizePerReady: 1000,
            maxUncommittedEntriesSize: Math.pow(2, 30),
            maxInflightMsgs: 256,
            tickTime: 100,
            defaultSnapCount: 1000,
            snapshotCatchUpEntriesN: 50,
            inputReproposeInterval: 1000,
            confChangeReproposeInterval: 1000,
            queuedUpInputProposalsLimit: 1000,
            runningJoinRequestsLimit: 1000,
            raftEntriesKey: "entries",
            raftHardstateKey: "hardstate",
            raftSnapshotKey: "snapshot",
            snapshotReceiveTimeout: 60000,
            joinRequestRetryInterval: 5000
        };
        return Object.assign(optionalDefaults, configuration);
    }
}

/**
 * Configuration that can be provided as part of the
 * {@linkcode RaftControllerOptions}. If no configuration is provided the
 * default configuration is used.
 *
 * @category Raft Controller
 */
export interface RaftConfiguration {
    /**
     * ElectionTick is the number of Node.Tick invocations that must pass
     * between elections. That is, if a follower does not receive any message
     * from the leader of current term before ElectionTick has elapsed, it will
     * become candidate and start an election. ElectionTick must be greater than
     * HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
     * unnecessary leader switching.
     *
     * If not set, default electionTick is 10.
     */
    electionTick?: number;

    /**
     * HeartbeatTick is the number of Node.Tick invocations that must pass
     * between heartbeats. That is, a leader sends heartbeat messages to
     * maintain its leadership every HeartbeatTick ticks.
     *
     * If not set, default heartbeatTick is 1.
     */
    heartbeatTick?: number;

    /**
     * MaxSizePerMsg limits the max size of each append message. The size is
     * measured by serializing the message into a string and counting its
     * characters. Smaller value lowers the Raft recovery cost(initial probing
     * and message lost during normal operation). On the other side, it might
     * affect the throughput during normal replication. Note: `Number.MAX_VALUE`
     * for unlimited, 0 for at most one entry per message.
     *
     * If not set, default maxSizePerMsg is (1024 * 1024).
     */
    maxSizePerMsg?: number;

    /**
     * MaxCommittedSizePerReady limits the size of the committed entries which
     * can be applied.
     *
     * If not set, default maxCommittedSizePerReady is 1000.
     */
    maxCommittedSizePerReady?: number;

    /**
     * MaxUncommittedEntriesSize limits the aggregate size of the uncommitted
     * entries that may be appended to a leader's log. The size is measured by
     * serializing the entries into a string and counting its characters. Once
     * this limit is exceeded, proposals will begin to return ErrProposalDropped
     * errors. Note: 0 for no limit.
     *
     * If not set, default maxUncommittedEntriesSize is 2^30.
     */
    maxUncommittedEntriesSize?: number;

    /**
     * MaxInflightMsgs limits the max number of in-flight append messages during
     * optimistic replication phase. The application transportation layer
     * usually has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs
     * to avoid overflowing that sending buffer.
     *
     * If not set, default maxInflightMsgs is 256.
     */
    maxInflightMsgs?: number;

    /**
     * Specifies how long one tick takes in ms.
     *
     * If not set, default tickTime is 100.
     */
    tickTime?: number;

    /**
     * Specifies after how many committed entries a manual snapshot is
     * triggered.
     *
     * If not set, default defaultSnapCount is 1000.
     */
    defaultSnapCount?: number;

    /**
     * Specifies the index at which a manual snapshot will be taken. If a manual
     * snapshot is taken the application will try to leave the newest
     * snapshotCatchUpEntriesN entries in the log and compact all older entries
     * into a snapshot.
     *
     * If not set, default snapshotCatchUpEntriesN is 50.
     */
    snapshotCatchUpEntriesN?: number;

    /**
     * Specifies how long it takes for an input to be reproposed if it was not
     * committed by that time.
     *
     * If not set, default inputReproposeInterval is 1000.
     */
    inputReproposeInterval?: number;

    /**
     * Specifies how long it takes for a configuration change to be reproposed
     * if it was not committed by that time.
     *
     * If not set, default confChangeReproposeInterval is 1000.
     */
    confChangeReproposeInterval?: number;

    /**
     * Limits the queue for input proposals. Input proposals are rejected if
     * this limit is exceeded.
     *
     * If not set, default queuedUpInputProposalsLimit is 1000.
     */
    queuedUpInputProposalsLimit?: number;

    /**
     * Limits the number of concurrently processed join requests. Join requests
     * are dropped if this limit is exceeded.
     *
     * If not set, default runningJoinRequestLimit is 1000.
     */
    runningJoinRequestsLimit?: number;

    /**
     * Database key for Raft entries.
     *
     * If not set, default raftEntriesKey is "entries".
     */
    raftEntriesKey?: string;

    /**
     * Database key for the Raft hardstate.
     *
     * If not set, default raftHardstateKey is "hardstate".
     */
    raftHardstateKey?: string;

    /**
     * Database key for the Raft snapshot.
     *
     * If not set, default raftSnapshotKey is "snapshot".
     */
    raftSnapshotKey?: string;

    /**
     * A snapshot counts as lost if it was not received within this time in ms.
     *
     * If not set, default snapshotReceiveTimeout is 60000
     */
    snapshotReceiveTimeout?: number;

    /**
     * Time in ms after which a join request is rebroadcasted to the cluster.
     *
     * If not set, default joinRequestRetryInterval is 5000.
     */
    joinRequestRetryInterval?: number;
}

/**
 * Controller options that should be provided when bootstrapping a
 * {@linkcode RaftController} in Coaty. Are used to configure the controller.
 *
 * Bootstrapping a `RaftController` in Coaty looks like this:
 *
 * ```typescript
 *  // Define RaftControllerOptions
 *  const opts: RaftControllerOptions = {
 *      id: "1"
 *      stateMachine: new YourRaftStateMachineImplementation(),
 *      shouldCreateCluster: true,
 *  };
 *
 *  // Create Components and Configuration
 *  const components: Components = { controllers: { RaftController } };
 *  const configuration: Configuration = { controllers: { RaftController: opts }, ... };
 *
 *  // Bootstrap RaftController
 *  const container = Container.resolve(components, configuration);
 *  const raftController = container.getController<RaftController>("RaftController");
 * ```
 *
 * @category Raft Controller
 */
export interface RaftControllerOptions {
    /**
     * Id of the {@linkcode RaftController}. Has to be unique among all
     * `RaftController`s in the given Raft cluster named by property "cluster"
     * (or default one). Must not be an empty string.
     *
     * @remark Ids cannot be reused within a cluster after the old controller
     * has left by disconnecting.
     *
     * Note that if the standard `RaftCommunication` implementation with Coaty
     * event patterns is used, an id must not contain the following characters:
     * `NULL (U+0000)`, `# (U+0023)`, `+ (U+002B)`, `/ (U+002F)`.
     *
     * Basically, consider using UUID v4 strings to ensure uniqueness within the
     * context of a Raft cluster.
     */
    id: string;

    /**
     * Will be used internally to describe the replicated state machine (RSM) of
     * the Raft cluster. All controllers in a cluster must use the same
     * {@linkcode RaftStateMachine} implementation and provide a new instance of
     * it here.
     */
    stateMachine: RaftStateMachine;

    /**
     * Defines whether or not the {@linkcode RaftController} should create a new
     * Raft cluster when it is first started. It is required that exactly one
     * `RaftController` per cluster has this variable set to `true` to create
     * the initial Raft cluster. Multiple `RaftController`s in the same cluster
     * having this variable set to `true` will lead to undefined behavior.
     *
     * @remark The `RaftController` that creates the Raft cluster is not
     * required to stay in the cluster. It is possible to use one designated
     * controller for creating the cluster that disconnects after the cluster is
     * up and running. In this case it is important that at least one other
     * controller has connected before the "creating" controller disconnects.
     */
    shouldCreateCluster: boolean;

    /**
     * Id of the Raft cluster of this {@linkcode RaftController}.
     *
     * If not given, the empty string is used.
     *
     * This option enables users to define multiple Raft clusters with different
     * {@linkcode RaftStateMachine} implementations that can run in parallel.
     */
    cluster?: string;

    /**
     * Key in Coaty `Configuration.databaseOptions` which defines the location
     * of the Raft persistency store (optional).
     *
     * If not given, a default database key named "raftdb" is used.
     */
    databaseKey?: string;

    /**
     * {@linkcode RaftConfiguration} that can be used to further configure the
     * underlying Raft implementation.
     *
     * If not given, the default configuration is used.
     */
    configuration?: RaftConfiguration;
}

/**
 * Data conforming to this type **must** be JSON compatible.
 *
 * @category Raft Controller
 */
export type RaftData = any;

/**
 * Instance of the `RaftData` data type that represents empty data.
 */
export const emptyRaftData: RaftData = null;

/**
 * Gets thrown if the provided {@linkcode RaftControllerOptions} for a
 * {@linkcode RaftController} are invalid or missing.
 *
 * @category Raft Controller Errors
 */
export class InvalidControllerOptionsError extends Error {
    constructor() {
        super("Invalid controller options.");
        this.name = "InvalidControllerOptionsError";
    }
}

/**
 * Gets thrown if an operation of {@linkcode RaftController} is called while the
 * controller is not in an appropriate connection state (e.g. `propose()` is
 * called before the controller has connected to the Raft cluster).
 *
 * @category Raft Controller Errors
 */
export class OperationNotSupportedInCurrentConnectionStateError extends Error {
    constructor(operationName: string, connectionState: string) {
        super(`Cannot execute ${operationName} because RaftController is ${connectionState}.`);
        this.name = "OperationNotSupportedInCurrentConnectionStateError";
    }
}

/**
 * Gets thrown if a {@linkcode RaftController} disconnects from the Raft cluster
 * before an operation has been completed (e.g. `propose()` couldn't make sure
 * that an input has been committed).
 *
 * @category Raft Controller Errors
 */
export class DisconnectBeforeOperationCompleteError extends Error {
    constructor(operationName: string) {
        super(`RaftController was disconnected before it could complete ${operationName}`);
        this.name = "DisconnectBeforeOperationCompleteError";
    }
}

/**
 * Gets thrown if an input is proposed inside {@linkcode RaftController} while
 * there are too many pending (i.e. not yet committed) input proposals queued
 * up.
 *
 * @category Raft Controller Errors
 */
export class TooManyQueuedUpInputProposalsError extends Error {
    constructor() {
        super(
            "Too many input proposals are queued up and haven't been committed yet. " +
            "Wait for the queued up proposals to be committed before you propose new inputs."
        )
        this.name = "TooManyQueuedUpInputProposalsError";
    }
}

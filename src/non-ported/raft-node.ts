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

import Channel from "../@nodeguy/channel/lib";
import { Observable, Subject, Subscription } from "rxjs";
import { filter, take, takeUntil } from "rxjs/operators";

import { emptyRaftData, RaftConfiguration, RaftData } from "./raft-controller";
import { RaftCommunication } from "./raft-node-interfaces/communication";
import { RaftCommunicationWrapper } from "./raft-node-interfaces/communication-wrapper";
import { RaftPersistency } from "./raft-node-interfaces/persistency";
import { RaftPersistencyWrapper } from "./raft-node-interfaces/persistency-wrapper";
import { RaftStateMachine } from "./raft-node-interfaces/state-machine";
import {
    RaftConfChangeCommit,
    RaftConfChangeProposal,
    RaftInputCommit,
    RaftInputProposal,
    RaftStateMachineWrapper
} from "./raft-node-interfaces/state-machine-wrapper";
import { getLogger } from "../ported/logger";
import {
    Node,
    Peer,
    restartNode,
    SnapshotStatus,
    startNode
} from "../ported/node";
import { RaftConfig } from "../ported/raft";
import {
    ConfChange,
    ConfChangeType,
    ConfState,
    Entry,
    EntryType,
    HardState,
    Message,
    MessageType,
    ridnone,
    Snapshot
} from "../ported/raftpb/raft.pb";
import { MemoryStorage } from "../ported/storage";

/**
 * Implements the Raft loop needed to use the etcd Raft library ported from go
 * as described here: https://github.com/etcd-io/etcd/tree/main/raft.
 *
 * @remark The raft-example project, which can also be found inside the etcd
 * github repository (etcd/contrib/raftexample), was used as reference while
 * developing this class.
 */
export class RaftNode {
    // -------------------------------------------------------------------------
    // GET SET IN CONSTRUCTOR, READONLY:

    /**
     * Uniquely identifies this `RaftNode` in the cluster.
     */
    private readonly _id: string;

    /**
     * Uniquely identifies the Raft cluster this `RaftNode` is a part of.
     */
    private readonly _cluster: string;

    /**
     * Specifies whether or not this node should create a new Raft cluster when
     * connecting.
     */
    private readonly _shouldCreateCluster: boolean;

    /**
     * Used to get and set the current Raft state, apply committed entries and
     * track the cluster configuration.
     */
    private readonly _stateMachine: RaftStateMachineWrapper;

    /**
     * Used for exchanging Raft messages and join requests with other
     * `RaftNode`s.
     */
    private readonly _communicationLayer: RaftCommunicationWrapper;

    /**
     * Used to access persistent memory for Raft entries, hardstate and
     * snapshot.
     */
    private readonly _persistentMemory: RaftPersistencyWrapper;

    /**
     * Used to configure the Raft implementation inside this node + subclasses.
     */
    private readonly _raftConfig: Required<RaftConfiguration>;

    // -------------------------------------------------------------------------
    // GET SET WHEN CONNECTING THIS RAFTNODE:

    /**
     * Access point to the etcd Raft implementation ported from go.
     */
    private _node: Node;

    /**
     * Used to manage hardstate, snapshot and entries.
     */
    private _storage: MemoryStorage;

    /**
     * Keeps track of the index of the last applied entry and snapshot and the
     * cluster configuration.
     */
    private _state: RaftNodeState;

    // -------------------------------------------------------------------------
    // HELPERS

    /**
     * Emits a `RaftInputCommit` for every input proposal that has been
     * committed.
     */
    private _committedInputs = new Subject<RaftInputCommit>();

    /**
     * Emits a `RaftConfChangeCommit` for every configuration change proposal
     * that has been committed.
     */
    private _committedConfChanges = new Subject<RaftConfChangeCommit>();

    /**
     * Completes after this `RaftNode` has been stopped.
     */
    private _stopRaft = new Subject<void>();

    /**
     * Ticker for the Raft loop inside `_startProcessingNodeReadyUpdates()`.
     */
    private _ticker = new Channel<{}>();

    /**
     * Used to stop the Raft loop inside `_startProcessingNodeReadyUpdates()`.
     */
    private _done = new Channel<{}>();

    /**
     * Keeps track of and is used to limit the number of concurrently running
     * join requests.
     */
    private _numRunningJoinRequests = 0;

    constructor(
        id: string,
        cluster: string,
        shouldCreateCluster: boolean,
        stateMachine: RaftStateMachine,
        communicationLayer: RaftCommunication,
        persistentMemory: RaftPersistency,
        raftConfig: Required<RaftConfiguration>
    ) {
        this._id = id;
        this._cluster = cluster;
        this._shouldCreateCluster = shouldCreateCluster;
        this._stateMachine = new RaftStateMachineWrapper(stateMachine, id, cluster, raftConfig);
        this._communicationLayer = new RaftCommunicationWrapper(communicationLayer, id, cluster, raftConfig);
        this._persistentMemory = new RaftPersistencyWrapper(persistentMemory, id, cluster, raftConfig);
        this._raftConfig = raftConfig;
    }

    /**
     * Connects this node to the existing Raft cluster or creates a new cluster
     * if `this._shouldCreateCluster` is set to true. If this node was already
     * part of the cluster but crashed, connection to the cluster is simply
     * reestablished.
     *
     * @returns A promise that is resolved once this node is connected to a new
     * or existing cluster.
     */
    public async connect(): Promise<void> {
        if (await this._persistentMemory.shouldRestartFromPersistedState()) {
            // This node was already part of the cluster before it crashed
            this._logInfo("Restarting from persisted state");
            this._storage = await this._persistentMemory.getPersistedStorage();
            this._node = restartNode(this._config());
        } else if (this._shouldCreateCluster) {
            // This node has to create the cluster
            this._logInfo("Creating a new single node cluster");
            this._storage = MemoryStorage.NewMemoryStorage();
            this._node = startNode(this._config(), this._peers());
        } else {
            // This node has to join an existing cluster
            this._logInfo("Joining the existing cluster");
            await this._requestToJoinExistingCluster();
            this._storage = MemoryStorage.NewMemoryStorage();
            this._node = restartNode(this._config());
        }
        this._initializeState();
        await this._start();
    }

    /**
     * Proposes the given input to the Raft cluster. As soon as a majority of
     * the cluster has accepted the input it counts as committed, is applied to
     * the distributed state machine and the promise returned by this function
     * resolves with the resulting state. If this node is being shut down before
     * it could make sure that the input was committed, the promise is rejected.
     *
     * It is important that no two inputs are proposed in parallel. Users of
     * this function always have to wait until the returned promise of one input
     * is resolved before proposing another one.
     *
     * @remark The ported Raft implementation doesn't guarantee that a proposed
     * input will be committed. That is why inputs are reproposed after a
     * timeout if they take too long to commit. Also see
     * `RaftStateMachineWrapper` for further details on reproposed inputs.
     *
     * @param input The input to be proposed to the Raft cluster and applied to
     * the distributed state machine. This input will be serialized/deserialized
     * (should not contain functions etc.) and should have a format that is
     * accepted by the `processInput(...)` function of the `RaftStateMachine`
     * implementation used.
     *
     * @returns A promise resolving to the resulting state of the state machine
     * after the proposed input has been committed and applied.
     */
    public async propose(input: RaftData): Promise<RaftData> {
        const proposal = this._stateMachine.createInputProposal(input, false);
        const commit = await this._proposeInput(proposal);
        return commit.resultingState;
    }

    /**
     * Returns a promise resolving to the state of the RSM. The state is
     * retrieved by proposing a NOOP and waiting til it is committed before
     * retrieving the state. The NOOP is being proposed internally and therefore
     * cannot be observed by `observeState()`.
     */
    public async getState(): Promise<RaftData> {
        await this._proposeNoopInput();
        return this._stateMachine.state;
    }

    /**
     * Returns a promise resolving to an observable that emits the state of the
     * RSM on every state update as soon as the update becomes known to this
     * node. State updates are triggered when a new input is committed.
     */
    public observeState(): Observable<RaftData> {
        return this._stateMachine.stateUpdates;
    }

    /**
     * Returns a promise resolving to the cluster configuration. The cluster
     * configuration contains the ids of all `RaftController`s in the cluster.
     */
    public async getClusterConfiguration(): Promise<RaftData> {
        await this._proposeNoopInput();
        return this._stateMachine.clusterConfiguration;
    }

    /**
     * Returns a promise resolving to an observable that emits the cluster
     * configuration on every configuration change as soon as the change becomes
     * known to this node. Cluster configuration changes are triggered when a
     * `RaftController` connects to or disconnects from the Raft cluster.
     */
    public observeClusterConfiguration(): Observable<string[]> {
        return this._stateMachine.clusterConfigurationUpdates;
    }

    /**
     * Shut's down this node and releases all underlying resources. The returned
     * promise is resolved after the shutdown process is complete.
     *
     * If `stayInCluster` is false (default) this node proposes to get removed
     * from the current Raft cluster. After a majority of the existing cluster
     * has agreed on removing this node it can shut down and release all its
     * resources.
     *
     * If `stayInCluster` is set to true this node shut's down and releases its
     * resources without leaving the current Raft cluster. If too many nodes do
     * this, the majority needed for input and cluster change proposals might no
     * longer be reachable.
     */
    public async disconnect(stayInCluster: boolean = false): Promise<void> {
        if (stayInCluster) {
            return this._stop(false);
        } else {
            const removeProposal = this._stateMachine.createConfChangeProposal("remove", this._id);
            await this._proposeConfChange(removeProposal); // Never throws
            return this._stop(true);
        }
    }

    /**
     * Creates the `RaftConfig` for this node. Should be called after
     * `this._storage` has been assigned since the storage is part of the
     * config.
     * @returns The created `RaftConfig`.
     */
    private _config(): RaftConfig {
        return new RaftConfig({
            id: this._id,
            electionTick: this._raftConfig.electionTick,
            heartbeatTick: this._raftConfig.heartbeatTick,
            storage: this._storage,
            maxSizePerMsg: this._raftConfig.maxSizePerMsg,
            maxCommittedSizePerReady: this._raftConfig.maxCommittedSizePerReady,
            maxUncommittedEntriesSize: this._raftConfig.maxUncommittedEntriesSize,
            maxInflightMsgs: this._raftConfig.maxInflightMsgs,
            preVote: true,
            checkQuorum: true,
        });
    }

    /**
     * Creates the `Peer` array that has to be provided inside `startNode()`.
     * Since we're creating a single node cluster we have to provide this node
     * as the only `Peer`. The `Peer.context` has to be set to the
     * `RaftConfChangeProposal` that should be assigned to the context of the
     * configuration change that will be created internally by Raft when
     * starting the cluster to add this node as initial node.
     */
    private _peers(): Peer[] {
        const initialNodeAddProposal = this._stateMachine.createConfChangeProposal("add", this._id);
        return [{ id: this._id, context: initialNodeAddProposal }];
    }

    /**
     * Requests to join the existing Raft cluster by proposing a join request
     * with `this._id`. The `RaftNode`s that are already part of the current
     * Raft cluster will propose a cluster configuration change adding this
     * node. This function returns as soon as the configuration has changed
     * accordingly.
     */
    private async _requestToJoinExistingCluster(): Promise<void> {
        await this._communicationLayer.proposeJoinRequest();
    }

    /**
     * Starts processing new node ready updates, received Raft messages and join
     * requests as well as the ticker used by raft. Should be called after
     * `this._node` has been started.
     */
    private async _start() {
        this._startProcessingNodeReadyUpdates();
        this._startProcessingReceivedMessages();
        this._startProcessingJoinRequests();
        this._startTicker();

        // Wait for NOOP to be committed to make sure all entries have been
        // replayed
        await this._proposeNoopInput();
        this._logInfo("Now running");
    }

    /**
     * Initializes `this._state` and `this._stateMachine` from storage.
     * Therefore `this._storage` has to be initialized before calling this
     * function.
     */
    private _initializeState() {
        const [snapshot, _] = this._storage.Snapshot(); // returned error is always null
        this._state = {
            confState: snapshot.metadata.confState,
            appliedIndex: snapshot.metadata.index,
            snapshotIndex: snapshot.metadata.index,
        };
        this._stateMachine.setStateFromSnapshotData(snapshot.data);
    }

    /**
     * Starts the etcd Raft loop. Further details can be found inside the README
     * of etcd Raft on github: https://github.com/etcd-io/etcd/tree/main/raft
     *
     * @remark etcd/contrib/raftexample/raft.go -> serveChannels() was used as
     * reference.
     */
    private async _startProcessingNodeReadyUpdates() {
        while (true) {
            switch (
            await Channel.select([
                this._ticker.shift(),
                this._node.ready().shift(),
                this._done.shift(),
            ])
            ) {
                case this._ticker:
                    this._node.tick();
                    break;
                case this._node.ready():
                    // Read new ready
                    const ready = this._node.ready().value();

                    // Persist entries, hardstate and snapshot if defined and
                    // update in-memory storage
                    await this._persist(ready.entries, ready.hardState, ready.snapshot);

                    // Send Raft messages
                    this._sendMessages(ready.messages);

                    // Apply snapshot if present
                    this._applySnapshot(ready.snapshot);

                    // Apply committed entries
                    await this._applyCommittedEntries(ready.committedEntries);

                    // Trigger snapshot if necessary
                    await this._maybeTriggerSnapshot();

                    // Signal readiness for the next batch of updates
                    await this._node.advance();
                    break;
                case this._done:
                    return;
            }
        }
    }

    /**
     * Persists entries, hardstate and snapshot if defined and updates
     * `this._storage` accordingly.
     */
    private async _persist(entries: Entry[], hardstate: HardState, snapshot: Snapshot) {
        // Persist new data
        await this._persistentMemory.persist(entries, hardstate, snapshot);

        // Update in-memory storage
        if (!snapshot.isEmptySnap()) {
            const snapErr = this._storage.ApplySnapshot(snapshot);
            if (snapErr !== null) {
                throw snapErr;
            }
        }
        if (!hardstate.isEmptyHardState()) {
            const hsErr = this._storage.SetHardState(hardstate);
            if (hsErr !== null) {
                throw hsErr;
            }
        }
        if (entries.length > 0) {
            const entErr = this._storage.Append(entries);
            if (entErr !== null) {
                throw entErr;
            }
        }
    }

    /**
     * Sends the given `Message`s to the respective `RaftNode`s inside the raft
     * cluster. Snapshot messages might be big and therefore require
     * `this._node.reportSnapshot()` to be called after they were received. For
     * further details see `RaftCommunicationWrapper`.
     */
    private _sendMessages(messages: Message[]): void {
        if (messages.length === 0) {
            return;
        }
        messages.forEach((message) => {
            if (message.to === ridnone) return;
            if (message.type === MessageType.MsgSnap) {
                // When there is a `raftpb.EntryConfChange` after creating the
                // snapshot, then the confState included in the snapshot is out
                // of date. so We need to update the confState before sending a
                // snapshot to a follower. See
                // https://github.com/etcd-io/etcd/issues/13741
                message.snapshot.metadata.confState = this._state.confState;

                // If message has type MsgSnap, call Node.ReportSnapshot() after
                // it has been sent
                this._communicationLayer
                    .sendRaftSnapshotMessage(message)
                    .then((success) => {
                        const status = success
                            ? SnapshotStatus.SnapshotFinish
                            : SnapshotStatus.SnapshotFailure;
                        this._node.reportSnapshot(message.to, status);
                    });
            } else {
                // All other messages are sent without checking if they were
                // received
                this._communicationLayer.sendRaftMessage(message);
            }
        });
    }

    /**
     * Applies the given snapshot to the underlying `RaftStateMachine` and
     * updates the state of this `RaftNode` accordingly.
     *
     * @remark etcd/contrib/raftexample/raft.go -> publishSnapshot() was used as
     * reference.
     */
    private _applySnapshot(snapshot: Snapshot): void {
        if (snapshot.isEmptySnap()) {
            return;
        }
        if (snapshot.metadata.index <= this._state.appliedIndex) {
            throw new Error(
                `Snapshot index [${snapshot.metadata.index}] should > ` +
                `progress.appliedIndex[${this._state.appliedIndex}]`
            );
        }
        this._stateMachine.setStateFromSnapshotData(snapshot.data);
        this._state = {
            confState: snapshot.metadata.confState,
            appliedIndex: snapshot.metadata.index,
            snapshotIndex: snapshot.metadata.index,
        };
    }

    /**
     * Applies the given entries. Cluster config changes are applied using
     * `this._node.applyConfChange()`. Those config changes might also include
     * removal of this `RaftNode` in which case `this._stop(true)` is called.
     * Committed inputs are applied to the state machine.
     */
    private async _applyCommittedEntries(entries: Entry[]) {
        const entriesToApply = this._entriesToApply(entries);
        for (const entry of entriesToApply) {
            if (entry.type === EntryType.EntryConfChange) {
                // Get proposal
                const cc = new ConfChange(entry.data);
                const proposal = cc.context as RaftConfChangeProposal;

                // Process proposal inside state machine
                const commit = this._stateMachine.processConfChangeProposal(proposal);

                // Apply ConfChange to etcd node if necessary
                if (commit.status === "applied") {
                    this._state.confState = await this._node.applyConfChange(cc);
                }

                // Forward commit back to this._proposeConfChange()
                this._committedConfChanges.next(commit);
            } else if (entry.type === EntryType.EntryNormal) {
                // Skip empty entries
                if (entry.data === emptyRaftData) continue;

                // Get proposal
                const proposal = entry.data as RaftInputProposal;

                // Process proposal inside state machine
                const commit = this._stateMachine.processInputProposal(proposal);

                // Forward commit back to this._proposeInput()
                this._committedInputs.next(commit);
            }
        }
        this._state.appliedIndex = entriesToApply.pop()?.index ?? this._state.appliedIndex;
    }

    /**
     * Checks for missing entries and filters out entries that already were
     * applied.
     *
     * @remark etcd/contrib/raftexample/raft.go -> entriesToApply() was used as
     * reference.
     */
    private _entriesToApply(entries: Entry[]): Entry[] {
        if (entries.length === 0) {
            return [];
        }
        const firstIdx = entries[0].index;
        // applied: 5, entries: [7,8,9] => error
        if (firstIdx > this._state.appliedIndex + 1) {
            // Entry has been skipped
            throw new Error(
                `first index of committed entry[${firstIdx}] should <= ` +
                `progress.appliedIndex[${this._state.appliedIndex}]+1`
            );
        }
        // - applied: 5, entries: [2,3,4,5,6,7] => return [6,7]
        // - applied: 5, entries: [2,3,4] => return []
        if (this._state.appliedIndex + 1 - firstIdx < entries.length) {
            // Remove entries that were already applied according to
            // appliedIndex
            return entries.slice(this._state.appliedIndex - firstIdx + 1);
        } else {
            return [];
        }
    }

    /**
     * Checks if a snapshot should be triggered and triggers it if necessary.
     *
     * @remark etcd/contrib/raftexample/raft.go -> MaybeTriggerSnapshot() was
     * used as reference.
     */
    private async _maybeTriggerSnapshot() {
        if (this._state.appliedIndex - this._state.snapshotIndex <= this._raftConfig.defaultSnapCount) {
            return;
        }
        this._logInfo(
            "Starting snapshot [applied index: %d | last snapshot index: %d]",
            this._state.appliedIndex,
            this._state.snapshotIndex
        );
        const [snapshot, err] = this._storage.CreateSnapshot(
            this._state.appliedIndex,
            this._state.confState,
            this._stateMachine.toSnapshotData()
        );
        if (err !== null) {
            throw err;
        }
        // Save Snapshot to persistent memory
        await this._persistentMemory.persist(null, null, snapshot);
        // Compact the storage up to the index snapshotCatchUpEntriesN behind
        // current appliedIndex
        let compactIndex = 1;
        if (this._state.appliedIndex > this._raftConfig.snapshotCatchUpEntriesN) {
            compactIndex = this._state.appliedIndex - this._raftConfig.snapshotCatchUpEntriesN;
        }
        const compactErr = this._storage.Compact(compactIndex);
        if (compactErr !== null) {
            throw compactErr;
        }
        this._logInfo("Compacted log at index %d", compactIndex);
        this._state.snapshotIndex = this._state.appliedIndex;
    }

    /**
     * `Message`s received from other `RaftNode`s in the cluster are relayed to
     * etcd Raft via `this._node.step()`.
     */
    private _startProcessingReceivedMessages() {
        this._communicationLayer
            .startReceivingRaftMessages()
            .pipe(takeUntil(this._stopRaft)) // Stop processing messages when _stopRaft emits its first value
            .subscribe((msg) => this._node.step(msg));
    }

    /**
     * Starts listening to the join requests send out by other `RaftNode`
     * instances via `_requestToJoin(...)`. When a new join request is received
     * this `RaftNode` (which is already part of the Raft cluster, otherwise
     * this function wouldn't have been called) asks Raft to include the raftId
     * associated with the join request in the cluster. If the id is valid and
     * doesn't clash with ids of other `RaftNode`s that are or at some point
     * were part of the current Raft cluster the configuration change is
     * committed and the "requesting" `RaftNode` gets notified and can connect.
     * For the cluster configuration change to be committed a majority of the
     * existing cluster must agree on letting the requesting node join.
     */
    private _startProcessingJoinRequests() {
        this._communicationLayer
            .startAcceptingJoinRequests()
            .pipe(takeUntil(this._stopRaft)) // Stop processing join requests when _stopRaft emits its first value
            .subscribe(([id, sendCallback]) => {
                if (this._numRunningJoinRequests >= this._raftConfig.runningJoinRequestsLimit) {
                    // Drop if too many join requests are already running
                } else {
                    this._numRunningJoinRequests++;
                    const proposal = this._stateMachine.createConfChangeProposal("add", id);
                    this._proposeConfChange(proposal)
                        .then((_) => sendCallback())
                        .catch(() => {
                            // Don't accept if error occurred
                        })
                        .finally(() => {
                            this._numRunningJoinRequests--;
                        });
                }
            });
    }

    /**
     * Starts the ticker used inside the Raft loop. The ticker is automatically
     * stopped when `this._stop(...)` is called.
     */
    private _startTicker() {
        const ticker = setInterval(async () => {
            const tickTimeout = new Channel<{}>();
            const tickTimeoutTrigger = setTimeout(() => tickTimeout.close(), this._raftConfig.tickTime);
            switch (await Channel.select([this._ticker.push({}), tickTimeout.shift()])) {
                case this._ticker:
                    tickTimeout.close();
                    clearTimeout(tickTimeoutTrigger);
                    break;
                default:
                    this._logWarning("Tick missed inside RaftNode. Raft loop too slow.");
            }
        }, this._raftConfig.tickTime);

        // Stop ticker when Raft is stopped
        this._stopRaft
            .pipe(take(1))
            .subscribe(() => {
                clearInterval(ticker);
            });
    }

    /**
     * Proposes the given input to the Raft cluster. As soon as a majority of
     * the cluster has accepted it, it counts as committed, is applied to the
     * distributed state machine and the promise returned by this function
     * resolves with the resulting commit.
     *
     * @remark The ported Raft implementation doesn't guarantee that a proposed
     * input will be committed. That is why inside this function inputs are
     * reproposed after a timeout if they take too long to commit.
     *
     * @param proposal The input that will be proposed.
     *
     * @returns A promise resolving to the resulting commit. Is rejected if this
     * node is shut down before it could make sure that the input was committed.
     */
    private _proposeInput(proposal: RaftInputProposal): Promise<RaftInputCommit> {
        return new Promise<RaftInputCommit>((resolve, reject) => {
            // Timer and subscriptions
            let reproposeTimer: NodeJS.Timeout | undefined;
            let waitForCommit: Subscription | undefined;

            // Has to be called before the returned Promise is rejected/resolved
            const cleanupSubscriptionsAndTimer = () => {
                clearInterval(reproposeTimer);
                waitForCommit?.unsubscribe();
            };

            // Start waiting for the committed input beforehand
            waitForCommit = this._committedInputs
                .pipe(
                    filter((commit) => commit.inputId === proposal.inputId),
                    take(1)
                )
                .subscribe({
                    next: (commit) => {
                        if (commit.rejected) {
                            // This node was removed from the cluster before the
                            // proposed input could be committed
                            cleanupSubscriptionsAndTimer();
                            reject();
                        } else {
                            // The proposed input was committed
                            cleanupSubscriptionsAndTimer();
                            resolve(commit);
                        }
                    },
                    complete: () => {
                        // This node is being shut down before it could make
                        // sure that the proposed input was committed
                        cleanupSubscriptionsAndTimer();
                        reject();
                    }
                });

            // Proposes the given input to the Raft cluster
            const propose = () => {
                const commit = this._stateMachine.checkIfInputProposalAlreadyApplied(proposal);
                if (commit !== null) {
                    // The proposed input was committed
                    cleanupSubscriptionsAndTimer();
                    resolve(commit);
                } else {
                    this._node.propose(proposal).then((err) => {
                        if (err !== null) {
                            // This node is being shut down before it could make
                            // sure that the proposed input was committed
                            cleanupSubscriptionsAndTimer();
                            reject();
                        }
                    });
                }
            };

            // Repropose every reproposeInterval milliseconds
            reproposeTimer = setInterval(propose, this._raftConfig.inputReproposeInterval);

            // Propose
            propose();
        });
    }

    /**
     * Proposes a NOOP input internally and resolves after the input was
     * committed. The input cannot be observed by `observeState()`.
     */
    private async _proposeNoopInput() {
        const noopProposal = this._stateMachine.createInputProposal(emptyRaftData, true);
        await this._proposeInput(noopProposal);
    }

    /**
     * Proposes the given cluster change to the Raft cluster. As soon as a
     * majority of the cluster has accepted the change it counts as committed,
     * is applied and the promise returned by this function resolves with the
     * resulting commit.
     *
     * @remark The ported Raft implementation doesn't guarantee that a proposed
     * configuration change will be committed. That is why inside this function
     * configuration changes are reproposed after a timeout if they take too
     * long to commit.
     *
     * @param proposal The proposal that will be made.
     *
     * @returns A promise resolving to the resulting commit. Is rejected if this
     * node is being shut down before it could make sure that the configuration
     * change was committed.
     */
    private _proposeConfChange(proposal: RaftConfChangeProposal): Promise<RaftConfChangeCommit> {
        // Create confChange and provide proposal as context
        const confChange: ConfChange = new ConfChange({
            type:
                proposal.type === "add"
                    ? ConfChangeType.ConfChangeAddNode
                    : ConfChangeType.ConfChangeRemoveNode,
            nodeId: proposal.nodeId,
            context: proposal,
        });

        // Start proposal process
        return new Promise<RaftConfChangeCommit>((resolve, reject) => {
            // Timer and subscriptions
            let reproposeTimer: NodeJS.Timeout | undefined;
            let reproposeAfterConfChange: Subscription | undefined;
            let waitForCommit: Subscription | undefined;

            // Has to be called before the returned Promise is rejected/resolved
            const cleanupSubscriptionsAndTimer = () => {
                clearInterval(reproposeTimer);
                reproposeAfterConfChange?.unsubscribe();
                waitForCommit?.unsubscribe();
            };

            // Start waiting for the committed conf change beforehand
            waitForCommit = this._committedConfChanges
                .pipe(
                    filter((commit) => commit.confChangeId === proposal.confChangeId),
                    take(1)
                )
                .subscribe({
                    next: (commit) => {
                        // The proposed configuration change was committed
                        cleanupSubscriptionsAndTimer();
                        resolve(commit);
                    },
                    complete: () => {
                        // This node is being shut down before it could make
                        // sure that the proposed configuration change was
                        // committed
                        cleanupSubscriptionsAndTimer();
                        reject();
                    }
                });

            // Proposes the given configuration change to the Raft cluster
            const propose = () => {
                const commit = this._stateMachine.checkIfConfChangeProposalAlreadyApplied(proposal);
                if (commit !== null) {
                    // The proposed configuration change was committed
                    cleanupSubscriptionsAndTimer();
                    resolve(commit);
                } else {
                    this._node.proposeConfChange(confChange).then((err) => {
                        if (err !== null) {
                            // This node is being shut down before it could make
                            // sure that the proposed configuration change was
                            // committed
                            cleanupSubscriptionsAndTimer();
                            reject();
                        }
                    });
                }
            };

            // Repropose every reproposeInterval milliseconds
            reproposeTimer = setInterval(propose, this._raftConfig.confChangeReproposeInterval);

            // Repropose immediately if another conf change has been committed.
            // This makes sense because in that case it is likely that the last
            // propose call was ignored. A proposed conf change is ignored while
            // any uncommitted change appears in the leader's log.
            reproposeAfterConfChange = this._committedConfChanges
                .pipe(filter((commit) => commit.confChangeId !== proposal.confChangeId))
                .subscribe(() => propose());

            // Propose
            propose();
        });
    }

    /**
     * Stops processing new node ready updates and received Raft messages a well
     * as the ticker used by raft. Should be called after this node has been
     * removed from the current cluster.
     */
    private async _stop(clearPersistentMemory: boolean): Promise<void> {
        // Stop underlying etcd Raft node
        await this._node.stop();

        // Stop Raft loop inside _startProcessingNodeReadyUpdates()
        await this._done.push({});

        // Stop processing received Raft messages, received join requests and
        // the ticker
        this._stopRaft.next();

        // Close connection to persistent memory and erase its data if necessary
        await this._persistentMemory.close(clearPersistentMemory);

        // Close all channels and complete all subjects in this class+subclasses
        this._done.close();
        this._ticker.close();
        this._committedInputs.complete();
        this._committedConfChanges.complete();
        this._stateMachine.completeSubjects();

        // Signals successful stop
        this._stopRaft.complete();
    }

    private _logInfo(format: string, ...v: any[]) {
        getLogger().infof("Cluster: \"%s\" - Node: \"%s\" - " + format, this._cluster, this._id, v);
    }

    private _logWarning(format: string, ...v: any[]) {
        getLogger().warningf("Cluster: \"%s\" - Node: \"%s\" - " + format, this._cluster, this._id, v);
    }
}

/**
 * Represents the current state of a `RaftNode`. Is actively being updated while
 * the respective `RaftNode` is running. Keeps track of the index of the last
 * applied entry and snapshot and the cluster configuration.
 */
interface RaftNodeState {
    confState: ConfState;
    snapshotIndex: number;
    appliedIndex: number;
}

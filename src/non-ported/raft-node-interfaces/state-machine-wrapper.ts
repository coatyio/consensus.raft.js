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

import { Runtime } from "@coaty/core";
import { Observable, Subject } from "rxjs";

import { RaftStateMachine } from "./state-machine";
import { RaftConfiguration, RaftData } from "../raft-controller";

/**
 * Wrapper for the `RaftStateMachine` interface. Is used inside `RaftNode` to
 * process `RaftConfChangeProposal`s and `RaftInputProposal`s. This additional
 * layer on top of `RaftStateMachine` provides better usability to `RaftNode`
 * while keeping the `RaftStateMachine` interface simple.
 *
 * - Constructs conf change and input proposals
 * - Applies input proposals to the state machine
 * - Tracks conf change proposals
 * - Handles reproposed conf change and input proposals
 * - Provides observable emitting the newest state whenever an input is applied
 * - Provides observable emitting the cluster configuration whenever it changes
 * - Copies/clones all state returned by this class to hinder users from
 *   changing the state from outside through the returned references
 * - Converts the state of this class (state machine + tracked cluster
 *   configuration) to snapshot data and restores it from snapshot data
 *
 * @remark See https://raft.github.io/raft.pdf section 8 for details on
 * reproposed inputs.
 */
export class RaftStateMachineWrapper {
    private readonly _wrapped: RaftStateMachine;
    private readonly _id: string;
    private readonly _cluster: string;
    private readonly _raftConfig: Required<RaftConfiguration>;

    private _stateUpdates = new Subject<RaftData>();
    private _clusterConfigurationUpdates = new Subject<string[]>();

    private _stateUpdatesObs = this._stateUpdates.asObservable();
    private _clusterConfigurationUpdatesObs = this._clusterConfigurationUpdates.asObservable();

    private _lastCommitPerNode: { [nodeId: string]: RaftInputCommit } = {};
    private _currentClusterMembers: string[] = [];

    // TODO: This array will grow bigger with every node leaving the current
    // cluster. To avoid this we would have to implement some form of expiration
    // of entries as described in the Raft thesis (6.3 Implementing linearizable
    // semantics). This requires a minor change in the ported Raft code because
    // the leader must augment each configuration change that it appends to the
    // Raft log with its current time.
    private _previousClusterMembers: string[] = [];

    constructor(wrapped: RaftStateMachine, id: string, cluster: string, raftConfig: Required<RaftConfiguration>) {
        this._wrapped = wrapped;
        this._id = id;
        this._cluster = cluster;
        this._raftConfig = raftConfig;
    }

    /**
     * Returns the current state of the wrapped state machine.
     */
    get state(): RaftData {
        return this._getState();
    }

    /**
     * Returns an observable that emits the state of the wrapped
     * `RaftStateMachine` on every state update.
     */
    get stateUpdates(): Observable<RaftData> {
        return this._stateUpdatesObs;
    }

    /**
     * Returns an array containing the ids of all members of the current
     * cluster.
     */
    get clusterConfiguration(): string[] {
        return this._getClusterConfiguration();
    }

    /**
     * Returns an observable that emits the current cluster configuration on
     * every configuration change.
     */
    get clusterConfigurationUpdates(): Observable<string[]> {
        return this._clusterConfigurationUpdatesObs;
    }

    /**
     * Creates a `RaftInputProposal` out of the provided values that can be
     * proposed to change the state of the replicated state machine (RSM).
     *
     * @param data The input that should be proposed.
     *
     * @param isNoop Specifies whether the proposed input should be handled like
     * a NOOP.
     *
     * @returns The created `RaftInputProposal`.
     */
    createInputProposal(data: RaftData, isNoop: boolean): RaftInputProposal {
        return {
            inputId: Runtime.newUuid(),
            originatorId: this._id,
            data: data,
            isNoop: isNoop,
        };
    }

    /**
     * Applies the proposed input to the wrapped `RaftStateMachine`.
     *
     * @remarks If the proposed input (identified by `input.inputId`) was
     * reproposed and therefore has already been applied, it is NOT applied for
     * a second time. Instead a commit containing the resulting state of the
     * state machine after the first application is returned.
     *
     * @param input The `RaftInputProposal` to be processed.
     *
     * @returns The appropriate `RaftInputCommit` containing the resulting state
     * of the state machine after the proposed input was applied.
     */
    processInputProposal(input: RaftInputProposal): RaftInputCommit {
        if (!this._currentClusterMembers.includes(input.originatorId)) {
            // Reject if the proposing node is no longer part of the cluster
            // This is needed because this._lastCommitPerNode no longer contains
            // an entry for the proposing node if it left the cluster
            const rejectedCommit: RaftInputCommit = {
                inputId: input.inputId,
                resultingState: null,
                rejected: true
            };
            return rejectedCommit;
        } else {
            const existingCommit = this.checkIfInputProposalAlreadyApplied(input);
            if (existingCommit !== null) {
                // Return existing commit if proposed input has already been
                // applied
                return existingCommit;
            } else {
                if (!input.isNoop) {
                    // Apply input to the wrapped state machine
                    this._wrapped.processInput(input.data);

                    // Signal state update
                    this._stateUpdates.next(this._getState());
                }
                // Update _lastCommitPerNode
                const newCommit: RaftInputCommit = {
                    inputId: input.inputId,
                    resultingState: this._getState(),
                    rejected: false
                };
                this._lastCommitPerNode[input.originatorId] = newCommit;

                // Return commit
                return newCommit;
            }
        }
    }

    /**
     * Checks if the provided `input` (identified by `input.inputId`) was
     * already applied. If yes, a `RaftInputCommit` containing the resulting
     * state of the state machine after the first application is returned,
     * otherwise `null`.
     */
    checkIfInputProposalAlreadyApplied(input: RaftInputProposal): RaftInputCommit | null {
        const lastCommit = this._lastCommitPerNode[input.originatorId];
        if (lastCommit && lastCommit.inputId === input.inputId) {
            return lastCommit;
        } else {
            return null;
        }
    }

    /**
     * Creates a `RaftConfChangeProposal` out of the provided values that can be
     * proposed to change the Raft cluster configuration.
     *
     * @param type Defines if a node should be added or removed.
     *
     * @param nodeId Id of the to be added/removed node.
     *
     * @returns The created `RaftConfChangeProposal`.
     */
    createConfChangeProposal(type: "add" | "remove", nodeId: string): RaftConfChangeProposal {
        return {
            confChangeId: Runtime.newUuid(),
            type: type,
            nodeId: nodeId,
        };
    }

    /**
     * Applies the proposed configuration change to the configuration state
     * tracked inside this wrapper.
     *
     * @remarks If the proposed configuration change was reproposed by the same
     * or another node and therefore has already been applied, it is NOT applied
     * for a second time. Instead a commit with `RaftConfChangeCommit.status`
     * set to `noop` is returned. Reproposals are identified by keeping track of
     * the cluster configuration history and taking into account that nodes can
     * not rejoin the cluster after leaving it.
     *
     * @param confChange The `RaftConfChangeProposal` to be processed.
     *
     * @returns The appropriate `RaftConfChangeCommit` containing information on
     * whether the configuration change was applied.
     */
    processConfChangeProposal(confChange: RaftConfChangeProposal): RaftConfChangeCommit {
        const existingCommit = this.checkIfConfChangeProposalAlreadyApplied(confChange);
        if (existingCommit !== null) {
            // Return existing commit if proposed confChange has already been
            // applied
            return existingCommit;
        } else {
            // The configuration needs to be applied
            if (confChange.type === "add") {
                // track node addition
                this._currentClusterMembers.push(confChange.nodeId);
            } else {
                // track node removal
                this._currentClusterMembers = this._currentClusterMembers.filter(
                    (id) => id !== confChange.nodeId
                );
                this._previousClusterMembers.push(confChange.nodeId);
                delete this._lastCommitPerNode[confChange.nodeId];
            }
            this._clusterConfigurationUpdates.next(this._getClusterConfiguration());
            return { confChangeId: confChange.confChangeId, status: "applied" };
        }
    }

    /**
     * Checks if the provided `confChange` was already applied. This is done by
     * keeping track of the cluster configuration history and taking into
     * account that nodes can not rejoin the cluster after leaving it. If the
     * change was already applied a `RaftConfChangeCommit` with
     * `RaftConfChangeCommit.status` set to `noop` is returned, otherwise
     * `null`.
     */
    checkIfConfChangeProposalAlreadyApplied(
        confChange: RaftConfChangeProposal
    ): RaftConfChangeCommit | null {
        if (
            (confChange.type === "add" &&
                (this._currentClusterMembers.includes(confChange.nodeId) ||
                    this._previousClusterMembers.includes(confChange.nodeId))) ||
            (confChange.type === "remove" &&
                this._previousClusterMembers.includes(confChange.nodeId))
        ) {
            // The proposed configuration change was already applied
            return { confChangeId: confChange.confChangeId, status: "noop" };
        } else {
            // The proposed configuration change wasn't applied yet
            return null;
        }
    }

    /**
     * Returns a serializable representation of this `RaftStateMachineWrapper`.
     * The returned data contains the state of the wrapped `RaftStateMachine` as
     * well as all state needed inside this wrapper.
     *
     * @remark "Counterpart" of `setStateFromSnapshotData(...)`.
     */
    toSnapshotData(): RaftData {
        return this._copyJSONCompatibleObj({
            wrapped: this._wrapped.getState(),
            lastCommitPerNode: this._lastCommitPerNode,
            currentClusterMembers: this._currentClusterMembers,
            previousClusterMembers: this._previousClusterMembers,
        });
    }

    /**
     * Set's all state needed inside this wrapper as well as the state of the
     * wrapped `RaftStateMachine` to the values encoded in the provided snapshot
     * data.
     *
     * @remark "Counterpart" of `this.toSnapshotData()`.
     *
     * @param data Will be used to set all state.
     */
    setStateFromSnapshotData(data: RaftData) {
        if (data === null) return;
        const copy = this._copyJSONCompatibleObj(data);
        this._wrapped.setState(copy.wrapped);
        this._lastCommitPerNode = copy.lastCommitPerNode;
        this._currentClusterMembers = copy.currentClusterMembers;
        this._previousClusterMembers = copy.previousClusterMembers;
        this._stateUpdates.next(this._getState());
        this._clusterConfigurationUpdates.next(this._getClusterConfiguration());
    }

    /**
     * Completes the observables for observing state and cluster configuration
     * that were returned by this class.
     */
    completeSubjects() {
        this._stateUpdates.complete();
        this._clusterConfigurationUpdates.complete();
    }

    private _getState(): RaftData {
        return this._copyJSONCompatibleObj(this._wrapped.getState());
    }

    private _getClusterConfiguration(): string[] {
        return this._copyJSONCompatibleObj(this._currentClusterMembers);
    }

    private _copyJSONCompatibleObj(obj: any): any {
        return JSON.parse(JSON.stringify(obj));
    }
}

/**
 * To make changes to the Raft state machine a `RaftInputProposal` has to be
 * proposed using `RaftNode.proposeInput()`.
 */
export interface RaftInputProposal {
    /**
     * Uniquely identifies the input that should be applied.
     */
    inputId: string;

    /**
     * Id of the `RaftNode` that made this proposal.
     */
    originatorId: string;

    /**
     * Actually proposed data. Input to be added to the Raft log and applied to
     * the `RaftStateMachine`.
     */
    data: RaftData;

    /**
     * If set to true the provided input is handled like a NOOP
     */
    isNoop: boolean;
}

/**
 * Each proposed `RaftInputProposal` gets processed by `RaftStateMachineWrapper`
 * and generates a `RaftInputCommit`.
 */
export interface RaftInputCommit {
    /**
     * Uniquely identifies the input that was committed.
     */
    inputId: string;

    /**
     * Resulting state of the Raft state machine after the respective input was
     * applied.
     */
    resultingState: RaftData;

    /**
     * An input is rejected if the proposing node leaves the cluster before the
     * input could be committed.
     */
    rejected: boolean;
}

/**
 * To make changes to the Raft cluster configuration a `RaftConfChangeProposal`
 * has to be proposed using `RaftNode.proposeConfChange()`.
 */
export interface RaftConfChangeProposal {
    /**
     * Uniquely identifies the configuration change that should be applied.
     */
    confChangeId: string;

    /**
     * Defines if the specified node should be added or removed.
     */
    type: "add" | "remove";

    /**
     * Identity of the to be added/removed node.
     */
    nodeId: string;
}

/**
 * Each proposed `RaftConfChangeProposal` gets processed by
 * `RaftStateMachineWrapper` and generates a `RaftConfChangeCommit`.
 */
export interface RaftConfChangeCommit {
    /**
     * Uniquely identifies the configuration change that was committed.
     */
    confChangeId: string;

    /**
     * - Applied: The configuration change was applied for the first time.
     * - Noop: The configuration change had already been applied.
     */
    status: "applied" | "noop";
}

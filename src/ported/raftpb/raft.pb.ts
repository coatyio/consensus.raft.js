// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.
//
// This is a port of Raft – the original work is copyright by "The etcd Authors"
// and licensed under Apache-2.0 similar to the license of this file.

import { confStateEquivalent } from "./confstate";
import { emptyRaftData, RaftData } from "../../non-ported/raft-controller";
import { NullableError } from "../util";

export type rid = string;
export const ridnone = "";

export enum EntryType {
    EntryNormal = 0,

    /**
     * corresponds to pb.ConfChange
     */
    EntryConfChange = 1,

    /**
     * corresponds to pb.ConfChangeV2
     */
    EntryConfChangeV2 = 2,
}

export class Entry {
    term: number;
    index: number;
    type: EntryType;
    data: RaftData;

    constructor(
        param: {
            term?: number;
            index?: number;
            type?: EntryType;
            data?: RaftData;
        } = {}
    ) {
        this.term = param?.term ?? 0;
        this.index = param?.index ?? 0;
        this.type = param?.type ?? EntryType.EntryNormal;
        this.data = param?.data ?? emptyRaftData;
    }
}

export class SnapshotMetadata {
    confState: ConfState;
    index: number;
    term: number;

    constructor(
        param: {
            confState?: ConfState;
            index?: number;
            term?: number;
        } = {}
    ) {
        this.confState = new ConfState(param?.confState);
        this.index = param?.index ?? 0;
        this.term = param?.term ?? 0;
    }
}

export class Snapshot {
    data: RaftData;
    metadata: SnapshotMetadata;

    constructor(
        param: {
            data?: RaftData;
            metadata?: SnapshotMetadata;
        } = {}
    ) {
        this.data = param?.data ?? emptyRaftData;
        this.metadata = new SnapshotMetadata(param?.metadata);
    }

    // IsEmptyHardState returns true if the given HardState is empty.
    isEmptySnap(): boolean {
        return this.metadata.index === 0;
    }
}

/**
 * For description of different message types, see:
 * https://pkg.go.dev/go.etcd.io/etcd/raft/v3#hdr-MessageType
 */
export enum MessageType {
    MsgHup,
    MsgBeat,
    MsgProp,
    MsgApp,
    MsgAppResp,
    MsgVote,
    MsgVoteResp,
    MsgSnap,
    MsgHeartbeat,
    MsgHeartbeatResp,
    MsgUnreachable,
    MsgSnapStatus,
    MsgCheckQuorum,
    MsgTransferLeader,
    MsgTimeoutNow,
    MsgReadIndex,
    MsgReadIndexResp,
    MsgPreVote,
    MsgPreVoteResp
}

export class Message {
    type: MessageType;
    to: rid;
    from: rid;
    term: number;

    /**
     * logTerm is generally used for appending Raft logs to followers. For
     * example, (type=MsgApp,index=100,logTerm=5) means leader appends entries
     * starting at index=101, and the term of entry at index 100 is 5.
     * (type=MsgAppResp,reject=true,index=100,logTerm=5) means follower rejects
     * some entries from its leader as it already has an entry with term 5 at
     * index 100.
     */
    logTerm: number;
    index: number;
    entries: Entry[];
    commit: number;
    snapshot: Snapshot;
    reject: boolean;
    rejectHint: number;
    context: RaftData;

    constructor(
        param: {
            type?: MessageType;
            to?: rid;
            from?: rid;
            term?: number;
            logTerm?: number;
            index?: number;
            entries?: Entry[];
            commit?: number;
            snapshot?: Snapshot;
            reject?: boolean;
            rejectHint?: number;
            context?: RaftData;
        } = {}
    ) {
        this.type = param?.type ?? 0;
        this.to = param?.to ?? ridnone;
        this.from = param?.from ?? ridnone;
        this.term = param?.term ?? 0;
        this.logTerm = param?.logTerm ?? 0;
        this.index = param?.index ?? 0;
        this.entries = param?.entries?.map<Entry>((v) => new Entry(v)) ?? [];
        this.commit = param?.commit ?? 0;
        this.snapshot = new Snapshot(param?.snapshot);
        this.reject = param?.reject ?? false;
        this.rejectHint = param?.rejectHint ?? 0;
        this.context = param?.context ?? emptyRaftData;
    }
}

export class HardState {
    term: number;
    vote: rid;
    commit: number;

    constructor(
        param: {
            term?: number;
            vote?: rid;
            commit?: number;
        } = {}
    ) {
        this.term = param?.term ?? 0;
        this.vote = param?.vote ?? ridnone;
        this.commit = param?.commit ?? 0;
    }

    isHardStateEqual(a: HardState): boolean {
        return this.term === a.term && this.vote === a.vote && this.commit === a.commit;
    }

    /**
     * IsEmptyHardState returns true if the given HardState is empty.
     */
    isEmptyHardState(): boolean {
        return this.isHardStateEqual(new HardState());
    }
}

/**
 * ConfChangeTransition specifies the behavior of a configuration change with
 * respect to joint consensus.
 */
export enum ConfChangeTransition {
    /**
     * Automatically use the simple protocol if possible, otherwise fall back to
     * ConfChangeJointImplicit. Most applications will want to use this.
     */
    ConfChangeTransitionAuto,

    /**
     * Use joint consensus unconditionally, and transition out of them
     * automatically (by proposing a zero configuration change). This option is
     * suitable for applications that want to minimize the time spent in the
     * joint configuration and do not store the joint configuration in the state
     * machine (outside of InitialState).
     */
    ConfChangeTransitionJointImplicit,

    /**
     * Use joint consensus and remain in the joint configuration until the
     * application proposes a no-op configuration change. This is suitable for
     * applications that want to explicitly control the transitions, for example
     * to use a custom payload (via the Context field).
     */
    ConfChangeTransitionJointExplicit,
}

export class ConfState {
    /**
     * The voters in the incoming config. (If the configuration is not joint,
     * then the outgoing config is empty).
     */
    voters: rid[];

    /**
     * The learners in the incoming config.
     */
    learners: rid[];

    /**
     * The voters in the outgoing config.
     */
    votersOutgoing: rid[];

    /**
     * The nodes that will become learners when the outgoing config is removed.
     * These nodes are necessarily currently in nodes_joint (or they would have
     * been added to the incoming config right away).
     */
    learnersNext: rid[];

    /**
     * If set, the config is joint and Raft will automatically transition into
     * the final config (i.e. remove the outgoing config) when this is safe.
     */
    autoLeave: boolean;

    constructor(
        param: {
            voters?: rid[];
            learners?: rid[];
            votersOutgoing?: rid[];
            learnersNext?: rid[];
            autoLeave?: boolean;
        } = {}
    ) {
        this.voters = param?.voters ?? [];
        this.learners = param?.learners ?? [];
        this.votersOutgoing = param?.votersOutgoing ?? [];
        this.learnersNext = param?.learnersNext ?? [];
        this.autoLeave = param?.autoLeave ?? false;
    }

    /**
     * Equivalent returns null if the inputs describe the same configuration. On
     * mismatch, returns a descriptive error showing the differences.
     */
    equivalent(cs2: ConfState): NullableError {
        return confStateEquivalent(this, cs2);
    }
}

export enum ConfChangeType {
    ConfChangeAddNode,
    ConfChangeRemoveNode,
    ConfChangeUpdateNode,
    ConfChangeAddLearnerNode,
}

export class ConfChange implements ConfChangeI {
    type: ConfChangeType;
    nodeId: rid;
    context: RaftData;

    constructor(
        param: {
            type?: ConfChangeType;
            nodeId?: rid;
            context?: RaftData;
        } = {}
    ) {
        this.type = param?.type ?? 0;
        this.nodeId = param?.nodeId ?? ridnone;
        this.context = param?.context ?? emptyRaftData;
    }

    asV1(): [ConfChange, boolean] {
        return [this, true];
    }

    asV2(): ConfChangeV2 {
        return new ConfChangeV2({
            changes: [new ConfChangeSingle({ type: this.type, nodeId: this.nodeId })],
            context: this.context,
        });
    }
}

/**
 * ConfChangeSingle is an individual configuration change operation. Multiple
 * such operations can be carried out atomically via a ConfChangeV2.
 */
export class ConfChangeSingle {
    type: ConfChangeType;
    nodeId: rid;

    constructor(
        param: {
            type?: ConfChangeType;
            nodeId?: rid;
        } = {}
    ) {
        this.type = param.type ?? 0;
        this.nodeId = param.nodeId ?? ridnone;
    }
}

/**
 * ConfChangeV2 messages initiate configuration changes. They support both the
 * simple "one at a time" membership change protocol and full Joint Consensus
 * allowing for arbitrary changes in membership. The supplied context is treated
 * as an opaque payload and can be used to attach an action on the state machine
 * to the application of the config change proposal. Note that contrary to Joint
 * Consensus as outlined in the Raft paper[1], configuration changes become
 * active when they are *applied* to the state machine (not when they are
 * appended to the log). The simple protocol can be used whenever only a single
 * change is made. Non-simple changes require the use of Joint Consensus, for
 * which two configuration changes are run. The first configuration change
 * specifies the desired changes and transitions the Raft group into the joint
 * configuration, in which quorum requires a majority of both the pre-changes
 * and post-changes configuration. Joint Consensus avoids entering fragile
 * intermediate configurations that could compromise survivability. For example,
 * without the use of Joint Consensus and running across three availability
 * zones with a replication factor of three, it is not possible to replace a
 * voter without entering an intermediate configuration that does not survive
 * the outage of one availability zone. The provided ConfChangeTransition
 * specifies how (and whether) Joint Consensus is used, and assigns the task of
 * leaving the joint configuration either to Raft or the application. Leaving
 * the joint configuration is accomplished by proposing a ConfChangeV2 with only
 * and optionally the Context field populated. For details on Raft membership
 * changes, see: [1]:
 * https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
 */
export class ConfChangeV2 implements ConfChangeI {
    transition: ConfChangeTransition;
    changes: ConfChangeSingle[];
    context: RaftData;

    constructor(
        param: {
            transition?: ConfChangeTransition;
            changes?: ConfChangeSingle[];
            context?: RaftData;
        } = {}
    ) {
        this.transition = param.transition ?? 0;
        this.changes = param.changes ?? [];
        this.context = param.context ?? emptyRaftData;
    }

    asV2(): ConfChangeV2 {
        return this;
    }

    asV1(): [ConfChange, boolean] {
        return [new ConfChange(), false];
    }

    enterJoint(): [boolean, boolean] {
        // NB: in theory, more config changes could qualify for the "simple"
        // protocol but it depends on the config on top of which the changes
        // apply. For example, adding two learners is not OK if both nodes are
        // part of the base config (i.e. two voters are turned into learners in
        // the process of applying the conf change). In practice, these
        // distinctions should not matter, so we keep it simple and use Joint
        // Consensus liberally.
        if (
            this.transition !== ConfChangeTransition.ConfChangeTransitionAuto ||
            this.changes.length > 1
        ) {
            // Use Joint Consensus.
            let autoLeave: boolean;
            switch (this.transition) {
                case ConfChangeTransition.ConfChangeTransitionAuto:
                    autoLeave = true;
                    break;
                case ConfChangeTransition.ConfChangeTransitionJointImplicit:
                    autoLeave = true;
                    break;
                case ConfChangeTransition.ConfChangeTransitionJointExplicit:
                    autoLeave = false;
                    break;
                default:
                    throw new Error(`unknown transition: ${JSON.stringify(this)}`);
            }
            return [autoLeave, true];
        }
        return [false, false];
    }

    /**
     * LeaveJoint is true if the configuration change leaves a joint
     * configuration. This is the case if the ConfChangeV2 is zero, with the
     * possible exception of the Context field.
     */
    leaveJoint(): boolean {
        // NB: c is already a copy.
        this.context = emptyRaftData;
        const emptyCC = new ConfChangeV2();
        return this.changes.length === 0 && this.transition === emptyCC.transition;
    }
}

/**
 * ConfChangeI abstracts over ConfChangeV2 and (legacy) ConfChange to allow
 * treating them in a unified manner.
 */
export interface ConfChangeI {
    asV2(): ConfChangeV2;
    asV1(): [ConfChange, boolean];
}

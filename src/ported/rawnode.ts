// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This is a port of Raft – the original work is copyright by "The etcd Authors"
// and licensed under Apache-2.0 similar to the license of this file.

import {
    confChangeToMsg,
    newReady,
    Ready,
    SnapshotStatus,
    SoftState
} from "./node";
import { Raft, RaftConfig } from "./raft";
import {
    ConfChangeI,
    ConfState,
    Entry,
    HardState,
    Message,
    MessageType,
    rid
} from "./raftpb/raft.pb";
import { BasicStatus, Status } from "./status";
import { Progress } from "./tracker/progress";
import { isLocalMsg, isResponseMsg, NullableError } from "./util";
import { RaftData } from "../non-ported/raft-controller";

/**
 * ErrStepLocalMsg is returned when try to step a local raft message
 */
export const errStepLocalMsg = new Error("raft: cannot step raft local message");

/**
 * ErrStepPeerNotFound is returned when try to step a response message but there
 * is no peer found in raft.prs for that node.
 */
export const errStepPeerNotFound = new Error("raft: cannot step as peer not found");

/**
 * RawNode is a thread-unsafe Node. The methods of this struct correspond to the
 * methods of Node and are described more fully there.
 */
export class RawNode {
    raft: Raft;
    prevSoftSt: SoftState | null;
    prevHardSt: HardState;

    constructor(param: {
        // NOTE: The raft parameter is not optional. If not specified it would
        // have to default to "null" because it is modeled as pointer in the go
        // implementation. That would require a lot of unwrapping inside this
        // class which we want to avoid.
        raft: Raft;
        prevSoftSt?: SoftState;
        prevHardSt?: HardState;
    }) {
        this.raft = param.raft;
        this.prevSoftSt = param.prevSoftSt ?? null;
        this.prevHardSt = param.prevHardSt ?? new HardState();
    }

    static NewRawNode(config: RaftConfig): RawNode {
        const r = Raft.NewRaft(config);
        const rn = new RawNode({ raft: r });
        rn.prevSoftSt = r.softState();
        rn.prevHardSt = r.hardState();
        return rn;
    }

    /**
     * Step advances the state machine using the given message.
     */
    step(m: Message): NullableError {
        // ignore unexpected local messages receiving over network
        if (isLocalMsg(m.type)) {
            return errStepLocalMsg;
        }
        const pr = this.raft.prs.progress.get(m.from);
        if (pr !== undefined || !isResponseMsg(m.type)) {
            return this.raft.Step(m);
        }
        return errStepPeerNotFound;
    }

    /**
     * Tick advances the internal logical clock by a single tick.
     */
    tick() {
        this.raft.tick!();
    }

    /**
     * TickQuiesced advances the internal logical clock by a single tick without
     * performing any other state machine processing. It allows the caller to
     * avoid periodic heartbeats and elections when all of the peers in a Raft
     * group are known to be at the same state. Expected usage is to
     * periodically invoke Tick or TickQuiesced depending on whether the group
     * is "active" or "quiesced".
     *
     * WARNING: Be very careful about using this method as it subverts the Raft
     * state machine. You should probably be using Tick instead.
     */
    tickQuiesced() {
        this.raft.electionElapsed++;
    }

    /**
     * Campaign causes this RawNode to transition to candidate state.
     */
    campaign(): NullableError {
        const m: Message = new Message({ type: MessageType.MsgHup });
        return this.raft.Step!(m);
    }

    /**
     * Propose proposes data be appended to the raft log.
     */
    propose(data: RaftData): NullableError {
        const m: Message = new Message({
            type: MessageType.MsgProp,
            from: this.raft.id,
            entries: [new Entry({ data: data })],
        });
        return this.raft.Step(m);
    }

    /**
     * ProposeConfChange proposes a config change. See (Node).ProposeConfChange
     * for details.
     */
    proposeConfChange(cc: ConfChangeI): NullableError {
        const msg = confChangeToMsg(cc);
        return this.raft.Step(msg);
    }

    /**
     * ApplyConfChange applies a config change to the local node. The app must
     * call this when it applies a configuration change, except when it decides
     * to reject the configuration change, in which case no call must take
     * place.
     */
    applyConfChange(cc: ConfChangeI): ConfState {
        // NOTE: Error is ignored here
        return this.raft.applyConfChange(cc.asV2())[0];
    }

    /**
     * Ready returns the outstanding work that the application needs to handle.
     * This includes appending and applying entries or a snapshot, updating the
     * HardState, and sending messages. The returned Ready() *must* be handled
     * and subsequently passed back via Advance().
     */
    ready(): Ready {
        const rd = this.readyWithoutAccept();
        this.acceptReady(rd);
        return rd;
    }

    /**
     * readyWithoutAccept returns a Ready. This is a read-only operation, i.e.
     * there is no obligation that the Ready must be handled.
     */
    readyWithoutAccept(): Ready {
        return newReady(this.raft, this.prevSoftSt, this.prevHardSt);
    }

    /**
     * acceptReady is called when the consumer of the RawNode has decided to go
     * ahead and handle a Ready. Nothing must alter the state of the RawNode
     * between this call and the prior call to Ready().
     */
    acceptReady(rd: Ready) {
        if (rd.softState !== null) {
            this.prevSoftSt = rd.softState;
        }
        if (rd.readStates && rd.readStates.length !== 0) {
            this.raft.readStates = [];
        }
        this.raft.msgs = [];
    }

    /**
     * HasReady called when RawNode user need to check if any Ready pending.
     * Checking logic in this method should be consistent with
     * Ready.containsUpdates().
     */
    hasReady(): boolean {
        if (!this.raft.softState().equal(this.prevSoftSt)) {
            return true;
        }
        const hardSt = this.raft.hardState();
        if (!hardSt.isEmptyHardState() && !hardSt.isHardStateEqual(this.prevHardSt)) {
            return true;
        }
        if (this.raft.raftLog.hasPendingSnapshot()) {
            return true;
        }
        if (
            this.raft.msgs?.length > 0 ||
            this.raft.raftLog?.unstableEntries()?.length > 0 ||
            this.raft.raftLog?.hasNextEnts()
        ) {
            return true;
        }
        if (this.raft.readStates.length !== 0) {
            return true;
        }
        return false;
    }

    /**
     * Advance notifies the RawNode that the application has applied and saved
     * progress in the last Ready results.
     */
    advance(rd: Ready) {
        if (!rd.hardState.isEmptyHardState()) {
            this.prevHardSt = rd.hardState;
        }
        this.raft.advance(rd);
    }

    /**
     * Status returns the current status of the given group. This allocates, see
     * BasicStatus and WithProgress for allocation-friendlier choices.
     */
    status(): Status {
        return Status.getStatus(this.raft);
    }

    /**
     * BasicStatus returns a BasicStatus. Notably this does not contain the
     * Progress map; see WithProgress for an allocation-free way to inspect it.
     */
    basicStatus(): BasicStatus {
        return BasicStatus.getBasicStatus(this.raft);
    }

    /**
     * WithProgress is a helper to introspect the Progress for this node and its
     * peers.
     */
    withProgress(visitor: (id: rid, typ: ProgressType, pr: Progress) => void) {
        this.raft.prs.visit((id: rid, pr: Progress) => {
            const type = pr.isLearner
                ? ProgressType.ProgressTypeLearner
                : ProgressType.ProgressTypePeer;
            pr.inflights = null;
            visitor(id, type, pr);
        });
    }

    /**
     * ReportUnreachable reports the given node is not reachable for the last
     * send.
     */
    reportUnreachable(id: rid) {
        const m: Message = new Message({
            type: MessageType.MsgUnreachable,
            from: id,
        });
        this.raft.Step!(m);
    }

    /**
     * ReportSnapshot reports the status of the sent snapshot.
     */
    reportSnapshot(id: rid, status: SnapshotStatus) {
        const rej = status === SnapshotStatus.SnapshotFailure;
        const m: Message = new Message({
            type: MessageType.MsgSnapStatus,
            from: id,
            reject: rej,
        });
        this.raft.Step!(m);
    }

    /**
     * TransferLeader tries to transfer leadership to the given transferee.
     */
    transferLeader(transferee: rid) {
        const m: Message = new Message({
            type: MessageType.MsgTransferLeader,
            from: transferee,
        });
        this.raft.Step!(m);
    }

    /**
     * ReadIndex requests a read state. The read state will be set in ready.
     * Read State has a read index. Once the application advances further than
     * the read index, any linearizable read requests issued before the read
     * request can be processed safely. The read state will have the same rctx
     * attached.
     */
    readIndex(rctx: RaftData) {
        const e: Entry[] = [new Entry({ data: rctx })];
        const m: Message = new Message({
            type: MessageType.MsgReadIndex,
            entries: e,
        });
        this.raft.Step!(m);
    }
}

/**
 * ProgressType indicates the type of replica a Progress corresponds to.
 */
export enum ProgressType {
    /**
     * ProgressTypePeer accompanies a Progress for a regular peer replica.
     */
    ProgressTypePeer,

    /**
     * ProgressTypeLearner accompanies a Progress for a learner replica.
     */
    ProgressTypeLearner,
}

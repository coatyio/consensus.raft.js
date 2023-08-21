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

import { Inflights } from "./inflights";
import { ProgressStateType, StateTypeToString } from "./state";
import { rid } from "../raftpb/raft.pb";

/**
 * Progress represents a follower’s progress in the view of the leader. Leader
 * maintains progresses of all followers, and sends entries to the follower
 * based on its progress. NB(tbg): Progress is basically a state machine whose
 * transitions are mostly strewn around `*raft.raft`. Additionally, some fields
 * are only used when in a certain State. All of this isn't ideal.
 */
export class Progress {
    match: number;
    next: number;

    /**
     * State defines how the leader should interact with the follower. When in
     * StateProbe, leader sends at most one replication message per heartbeat
     * interval. It also probes actual progress of the follower. When in
     * StateReplicate, leader optimistically increases next to the latest entry
     * sent after sending replication message. This is an optimized state for
     * fast replicating log entries to the follower. When in StateSnapshot,
     * leader should have sent out snapshot before and stops sending any
     * replication message.
     */
    state: ProgressStateType;

    /**
     * PendingSnapshot is used in StateSnapshot. If there is a pending snapshot,
     * the pendingSnapshot will be set to the index of the snapshot. If
     * pendingSnapshot is set, the replication process of this Progress will be
     * paused. raft will not resend snapshot until the pending one is reported
     * to be failed.
     */
    pendingSnapshot: number;

    /**
     * RecentActive is true if the progress is recently active. Receiving any
     * messages from the corresponding follower indicates the progress is
     * active. RecentActive can be reset to false after an election timeout. TO
     * DO(tbg): the leader should always have this set to true.
     */
    recentActive: boolean;

    /**
     * ProbeSent is used while this follower is in StateProbe. When ProbeSent is
     * true, raft should pause sending replication message to this peer until
     * ProbeSent is reset. See ProbeAcked() and IsPaused().
     */
    probeSent: boolean;

    /**
     * Inflights is a sliding window for the inflight messages. Each inflight
     * message contains one or more log entries. The max number of entries per
     * message is defined in raft config as MaxSizePerMsg. Thus inflight
     * effectively limits both the number of inflight messages and the bandwidth
     * each Progress can use. When inflights is Full, no more message should be
     * sent. When a leader sends out a message, the index of the last entry
     * should be added to inflights. The index MUST be added into inflights in
     * order. When a leader receives a reply, the previous inflights should be
     * freed by calling inflights.FreeLE with the index of the last received
     * entry.
     */
    inflights: Inflights | null;

    /**
     * IsLearner is true if this progress is tracked for a learner.
     */
    isLearner: boolean;

    constructor(
        param: {
            match?: number;
            next?: number;
            state?: ProgressStateType;
            pendingSnapshot?: number;
            recentActive?: boolean;
            probeSent?: boolean;
            inflights?: Inflights | null;
            isLearner?: boolean;
        } = {}
    ) {
        this.match = param.match ?? 0;
        this.next = param.next ?? 0;
        this.state = param.state ?? ProgressStateType.StateProbe;
        this.pendingSnapshot = param.pendingSnapshot ?? 0;
        this.recentActive = param.recentActive ?? false;
        this.probeSent = param.probeSent ?? false;
        this.inflights = param.inflights ? new Inflights(param.inflights) : null;
        this.isLearner = param.isLearner ?? false;
    }

    /**
     * ResetState moves the Progress into the specified State, resetting
     * ProbeSent, PendingSnapshot, and Inflights.
     */
    ResetState(state: ProgressStateType) {
        this.probeSent = false;
        this.pendingSnapshot = 0;
        this.state = state;
        this.inflights?.reset();
    }

    /**
     * ProbeAcked is called when this peer has accepted an append. It resets
     * ProbeSent to signal that additional append messages should be sent
     * without further delay.
     */
    ProbeAcked(): void {
        this.probeSent = false;
    }

    /**
     * BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
     * optionally and if larger, the index of the pending snapshot.
     */
    BecomeProbe(): void {
        // If the original state is StateSnapshot, progress knows that the
        // pending snapshot has been sent to this peer successfully, then probes
        // from pendingSnapshot + 1.
        if (this.state === ProgressStateType.StateSnapshot) {
            const pendingSnapshot = this.pendingSnapshot;
            this.ResetState(ProgressStateType.StateProbe);
            this.next = Math.max(this.match + 1, pendingSnapshot + 1);
        } else {
            this.ResetState(ProgressStateType.StateProbe);
            this.next = this.match + 1;
        }
    }

    /**
     * BecomeReplicate transitions into StateReplicate, resetting Next to
     * Match+1.
     */
    BecomeReplicate(): void {
        this.ResetState(ProgressStateType.StateReplicate);
        this.next = this.match + 1;
    }

    /**
     * BecomeSnapshot moves the Progress to StateSnapshot with the specified
     * pending snapshot index.
     */
    BecomeSnapshot(snapshoti: number): void {
        this.ResetState(ProgressStateType.StateSnapshot);
        this.pendingSnapshot = snapshoti;
    }

    /**
     * MaybeUpdate is called when an MsgAppResp arrives from the follower, with
     * the index acked by it. The method returns false if the given n index
     * comes from an outdated message. Otherwise it updates the progress and
     * returns true.
     */
    MaybeUpdate(n: number): boolean {
        let updated: boolean = false;

        if (this.match < n) {
            this.match = n;
            updated = true;
            this.ProbeAcked();
        }

        this.next = Math.max(this.next, n + 1);
        return updated;
    }

    /**
     * OptimisticUpdate signals that appends all the way up to and including
     * index n are in-flight. As a result, Next is increased to n+1.
     */
    OptimisticUpdate(n: number): void {
        this.next = n + 1;
    }

    /**
     * MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection.
     * The arguments are the index of the append message rejected by the
     * follower, and the hint that we want to decrease to. Rejections can happen
     * spuriously as messages are sent out of order or duplicated. In such
     * cases, the rejection pertains to an index that the Progress already knows
     * were previously acknowledged, and false is returned without changing the
     * Progress. If the rejection is genuine, Next is lowered sensibly, and the
     * Progress is cleared for sending log entries.
     */
    MaybeDecrTo(rejected: number, matchHint: number): boolean {
        if (this.state === ProgressStateType.StateReplicate) {
            // The rejection must be stale if the progress has matched and
            // "rejected" is smaller than "match".
            if (rejected <= this.match) {
                return false;
            }
            // Directly decrease next to match + 1.
            //
            // TO DO(tbg): why not use matchHint if it's larger?
            this.next = this.match + 1;
            return true;
        }

        // The rejection must be stale if "rejected" does not match next - 1.
        // This is because non-replicating followers are probed one entry at a
        // time.
        if (this.next - 1 !== rejected) {
            return false;
        }

        this.next = Math.max(Math.min(rejected, matchHint + 1), 1);
        this.probeSent = false;
        return true;
    }

    /**
     * IsPaused returns whether sending log entries to this node has been
     * throttled. This is done when a node has rejected recent MsgApps, is
     * currently waiting for a snapshot, or has reached the MaxInflightMsgs
     * limit. In normal operation, this is false. A throttled node will be
     * contacted less frequently until it has reached a state in which it's able
     * to accept a steady stream of log entries again.
     */
    IsPaused(): boolean {
        switch (this.state) {
            case ProgressStateType.StateProbe:
                return this.probeSent;
            case ProgressStateType.StateReplicate:
                return this.inflights?.full() ?? false;
            case ProgressStateType.StateSnapshot:
                return true;
            default:
                throw new Error("unexpected state");
        }
    }

    toString(): string {
        const result: string[] = [];

        result.push(StateTypeToString(this.state) + " match=" + this.match + " next=" + this.next);

        if (this.isLearner) result.push(" learner");
        if (this.IsPaused()) result.push(" paused");
        if (this.pendingSnapshot > 0) result.push(" pendingSnap=" + this.pendingSnapshot);
        if (!this.recentActive) {
            result.push(" inactive");
        }
        if (this.inflights !== null) {
            const n = this.inflights.count();
            if (n > 0) {
                result.push(" inflight=" + n);
                if (this.inflights.full()) {
                    result.push("[full]");
                }
            }
        }

        return result.join("");
    }
}

export type ProgressMap = Map<rid, Progress>;

/**
 * String prints the ProgressMap in sorted key order, one Progress per line.
 */
export function progressMapToString(map: ProgressMap): string {
    const progressArray: [rid, Progress][] = Array.from(map);
    progressArray.sort(([keyA, valueA], [keyB, valueB]) => {
        if (keyA > keyB) return 1;
        else if (keyA === keyB) return 0;
        else return -1;
    });

    const result: string[] = [];
    for (const [key, value] of progressArray) {
        result.push(key + ": " + value + "\n");
    }
    return result.join("");
}

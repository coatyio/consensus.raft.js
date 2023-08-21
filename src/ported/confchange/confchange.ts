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

import { JointConfig } from "../quorum/joint";
import { MajorityConfig } from "../quorum/majority";
import { ConfChangeSingle, ConfChangeType, rid, ridnone } from "../raftpb/raft.pb";
import { NewInflights } from "../tracker/inflights";
import { Progress, ProgressMap } from "../tracker/progress";
import { Config, ProgressTracker } from "../tracker/tracker";
import { NullableError } from "../util";

/**
 * Changer facilitates configuration changes. It exposes methods to handle
 * simple and joint consensus while performing the proper validation that allows
 * refusing invalid configuration changes before they affect the active
 * configuration.
 */
export class Changer {
    tracker: ProgressTracker;
    lastIndex: number;

    constructor(tracker: ProgressTracker, lastIndex: number) {
        this.tracker = tracker;
        this.lastIndex = lastIndex;
    }

    /**
     * EnterJoint verifies that the outgoing (=right) majority config of the
     * joint config is empty and initializes it with a copy of the incoming
     * (=left) majority config. That is, it transitions from (1 2 3)&&() to (1 2
     * 3)&&(1 2 3). The supplied changes are then applied to the incoming
     * majority config, resulting in a joint configuration that in terms of the
     * Raft thesis[1] (Section 4.3) corresponds to `C_{new,old}`. [1]:
     * https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
     */
    enterJoint(autoLeave: boolean, ...ccs: ConfChangeSingle[]): [Config, ProgressMap, NullableError] {
        const [cfg, prs, err] = this.checkAndCopy();

        if (err !== null) {
            return this.err(err);
        }
        if (joint(cfg)) {
            const err2 = new Error("config is already joint");
            return this.err(err2);
        }
        if (incoming(cfg.voters).length() === 0) {
            // We allow adding nodes to an empty config for convenience (testing
            // and bootstrap), but you can't enter a joint state.
            return this.err(new Error("can't make a zero-voter config joint"));
        }

        // Clear the outgoing config.
        cfg.voters.config[1] = new MajorityConfig();
        // Copy incoming to outgoing
        for (const id of incoming(cfg.voters).map.keys()) {
            outgoing(cfg.voters).map.set(id, {});
        }

        const err3 = this.apply(cfg, prs, ...ccs);
        if (err3 !== null) {
            return this.err(err3);
        }
        cfg.autoLeave = autoLeave;
        return checkAndReturn(cfg, prs);
    }

    /**
     * LeaveJoint transitions out of a joint configuration. It is an error to
     * call this method if the configuration is not joint, i.e. if the outgoing
     * majority config Voters[1] is empty.
     *
     * The outgoing majority config of the joint configuration will be removed,
     * that is, the incoming config is promoted as the sole decision maker. In
     * the notation of the Raft thesis[1] (Section 4.3), this method transitions
     * from `C_{new,old}` into `C_new`.
     *
     * At the same time, any staged learners (LearnersNext) the addition of
     * which was held back by an overlapping voter in the former outgoing config
     * will be inserted into Learners.
     *
     * [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
     */
    leaveJoint(): [Config, ProgressMap, NullableError] {
        const [cfg, prs, err] = this.checkAndCopy();
        if (err !== null) {
            return this.err(err);
        }
        if (!joint(cfg)) {
            const errNotJoint = new Error("can't leave a non-joint config");
            return this.err(errNotJoint);
        }
        if (outgoing(cfg.voters).length() === 0) {
            const errNotJoint = new Error("configuration is not joint: " + cfg);
            return this.err(errNotJoint);
        }
        for (const id of cfg.learnersNext.keys()) {
            cfg.learners.set(id, {});
            const prsAtId: Progress = prs.get(id)!;
            prsAtId.isLearner = true;
        }
        cfg.learnersNext = new Map<rid, object>();

        for (const id of outgoing(cfg.voters).map.keys()) {
            const isVoter: boolean = incoming(cfg.voters).map.has(id);
            const isLearner: boolean = cfg.learners ? cfg.learners.has(id) : false;

            if (!isVoter && !isLearner) {
                prs.delete(id);
            }
        }

        cfg.voters.config[1] = new MajorityConfig();
        cfg.autoLeave = false;

        return checkAndReturn(cfg, prs);
    }

    /**
     * Simple carries out a series of configuration changes that (in aggregate)
     * mutates the incoming majority config Voters[0] by at most one. This
     * method will return an error if that is not the case, if the resulting
     * quorum is zero, or if the configuration is in a joint state (i.e. if
     * there is an outgoing configuration).
     */
    simple(...ccs: ConfChangeSingle[]): [Config, ProgressMap, NullableError] {
        const [cfg, prs, err] = this.checkAndCopy();
        if (err !== null) {
            return this.err(err);
        }
        if (joint(cfg)) {
            const errJoint = new Error("can't apply simple config change in joint config");
            return this.err(errJoint);
        }
        const errApply = this.apply(cfg, prs, ...ccs);
        if (errApply !== null) {
            return this.err(errApply);
        }
        const n = symdiff(incoming(this.tracker.config.voters), incoming(cfg.voters));
        if (n > 1) {
            return [
                new Config({}),
                new Map<rid, Progress>(),
                new Error("more than one voter changed without entering joint config"),
            ];
        }
        return checkAndReturn(cfg, prs);
    }

    /**
     * apply a change to the configuration. By convention, changes to voters are
     * always made to the incoming majority config Voters[0]. Voters[1] is
     * either empty or preserves the outgoing majority configuration while in a
     * joint state.
     */
    apply(cfg: Config, prs: ProgressMap, ...ccs: ConfChangeSingle[]): NullableError {
        for (const cc of ccs) {
            if (cc.nodeId === ridnone) {
                // etcd replaces the NodeID with zero if it decides (downstream
                // of raft) to not apply a change, so we have to have explicit
                // code here to ignore these.
                continue;
            }
            switch (cc.type) {
                case ConfChangeType.ConfChangeAddNode:
                    this.makeVoter(cfg, prs, cc.nodeId!);
                    break;
                case ConfChangeType.ConfChangeAddLearnerNode:
                    this.makeLearner(cfg, prs, cc.nodeId!);
                    break;
                case ConfChangeType.ConfChangeRemoveNode:
                    this.remove(cfg, prs, cc.nodeId!);
                    break;
                case ConfChangeType.ConfChangeUpdateNode:
                    break;
                default:
                    return new Error("unexpected conf type " + cc.type);
            }
        }
        if (incoming(cfg.voters).length() === 0) {
            return new Error("removed all voters");
        }
        return null;
    }

    /**
     * makeVoter adds or promotes the given ID to be a voter in the incoming
     * majority config.
     */
    makeVoter(cfg: Config, prs: ProgressMap, id: rid): void {
        const pr = prs.get(id);
        if (pr === undefined) {
            this.initProgress(cfg, prs, id, false /*isLearner */);
            return;
        }

        pr.isLearner = false;
        cfg.learners = nilAwareDelete(cfg.learners, id);
        cfg.learnersNext = nilAwareDelete(cfg.learnersNext, id);
        incoming(cfg.voters).map.set(id, {});
    }

    /**
     * makeLearner makes the given ID a learner or stages it to be a learner
     * once an active joint configuration is exited.
     *
     * The former happens when the peer is not a part of the outgoing config, in
     * which case we either add a new learner or demote a voter in the incoming
     * config.
     *
     * The latter case occurs when the configuration is joint and the peer is a
     * voter in the outgoing config. In that case, we do not want to add the
     * peer as a learner because then we'd have to track a peer as a voter and
     * learner simultaneously. Instead, we add the learner to LearnersNext, so
     * that it will be added to Learners the moment the outgoing config is
     * removed by LeaveJoint().
     */
    makeLearner(cfg: Config, prs: ProgressMap, id: rid): void {
        const pr = prs.get(id);
        if (pr === undefined) {
            this.initProgress(cfg, prs, id, true /* isLearner */);
            return;
        }
        if (pr.isLearner) {
            return;
        }

        // Remove any existing voter in the incoming config...
        this.remove(cfg, prs, id);
        // ... but save the Progress.
        prs.set(id, pr);
        // Use LearnersNext if we can't add the learner to Learners directly,
        // i.e. if the peer is still tracked as a voter in the outgoing config.
        // It will be turned into a learner in LeaveJoint()

        // Otherwise, add a regular learner right away.
        if (outgoing(cfg.voters).map.has(id)) {
            cfg.learnersNext.set(id, {});
        } else {
            pr.isLearner = true;
            cfg.learners.set(id, {});
        }
    }

    /**
     * remove this peer as a voter or learner from the incoming config.
     */
    remove(cfg: Config, prs: ProgressMap, id: rid): void {
        if (!prs.has(id)) {
            return;
        }

        incoming(cfg.voters).map.delete(id);
        cfg.learners = nilAwareDelete(cfg.learners, id);
        cfg.learnersNext = nilAwareDelete(cfg.learnersNext, id);

        // If the peer is still a voter in the outgoing config, keep the
        // Progress.
        const onRight: boolean = outgoing(cfg.voters).map.has(id);
        if (!onRight) {
            prs.delete(id);
        }
    }

    /**
     * initProgress initializes a new progress for the given node or learner.
     */
    initProgress(cfg: Config, prs: ProgressMap, id: rid, isLearner: boolean): void {
        if (!isLearner) {
            incoming(cfg.voters).map.set(id, {});
        } else {
            cfg.learners.set(id, {});
        }

        const newProgress: Progress = new Progress({
            // Initializing the Progress with the last index means that the
            // follower can be probed (with the last index).
            //
            // TO DO(tbg): seems awfully optimistic. Using the first index would
            // be better. The general expectation here is that the follower has
            // no log at all (and will thus likely need a snapshot), though the
            // app may have applied a snapshot out of band before adding the
            // replica (thus making the first index the better choice).
            next: this.lastIndex,
            match: 0,
            inflights: NewInflights(this.tracker.maxInflight),
            isLearner: isLearner,
            // When a node is first added, we should mark it as recently active.
            // Otherwise, CheckQuorum may cause us to step down if it is invoked
            // before the added node has had a chance to communicate with us.
            recentActive: true,
        });

        prs.set(id, newProgress);
    }

    /**
     * checkAndCopy copies the tracker's config and progress map (deeply enough
     * for the purposes of the Changer) and returns those copies. It returns an
     * error if checkInvariants does.
     */
    private checkAndCopy(): [Config, ProgressMap, NullableError] {
        const cfg = this.tracker.config.clone();
        const prs: ProgressMap = new Map<rid, Progress>();

        for (const [id, pr] of this.tracker.progress) {
            // A shallow copy is enough because we only mutate the Learner
            // field.
            const ppr: Progress = new Progress({
                match: pr.match,
                next: pr.next,
                state: pr.state,
                pendingSnapshot: pr.pendingSnapshot,
                recentActive: pr.recentActive,
                probeSent: pr.probeSent,
                inflights: pr.inflights,
                isLearner: pr.isLearner,
            });

            prs.set(id, ppr);
        }

        return checkAndReturn(cfg, prs);
    }

    /** err returns zero values and an error. */
    private err(err: Error): [Config, ProgressMap, Error] {
        return [new Config({}), new Map<rid, Progress>(), err];
    }
}

/**
 * checkInvariants makes sure that the config and progress are compatible with
 * each other. This is used to check both what the Changer is initialized with,
 * as well as what it returns.
 */
function checkInvariants(cfg: Config, prs: ProgressMap): NullableError {
    // NB: intentionally allow the empty config. In production we'll never see a
    // non-empty config (we prevent it from being created) but we will need to
    // be able to *create* an initial config, for example during bootstrap (or
    // during tests). Instead of having to hand-code this, we allow
    // transitioning from an empty config into any other legal and non-empty
    // config.
    for (const ids of [cfg.voters.ids(), cfg.learners, cfg.learnersNext]) {
        for (const id of ids.keys()) {
            if (!prs.has(id)) {
                return new Error("no progress for " + id);
            }
        }
    }

    // Any staged learner was staged because it could not be directly added due
    // to a conflicting voter in the outgoing config.
    for (const id of cfg.learnersNext.keys()) {
        if (!outgoing(cfg.voters).map.has(id)) {
            return new Error(id + " is in LearnersNext, but not Voters[1]");
        }
        const prsAtId: Progress = prs.get(id)!;
        if (prsAtId.isLearner) {
            return new Error(id + " is in LearnersNext, but is already marked as learner");
        }
    }
    // Conversely Learners and Voters doesn't intersect at all.
    for (const id of cfg.learners.keys()) {
        if (
            outgoing(cfg.voters) && outgoing(cfg.voters).map
                ? outgoing(cfg.voters).map.has(id)
                : false
        ) {
            return new Error(id + " is in Learners and Voters[1]");
        }
        if (incoming(cfg.voters).map.has(id)) {
            return new Error(id + " is in Learners and Voters[0]");
        }
        const prsAtId: Progress = prs.get(id)!;
        if (!prsAtId.isLearner) {
            return new Error(id + " is in Learners, but is not marked as learner");
        }
    }

    if (!joint(cfg)) {
        // NOTE: In TypeScript we DO NOT enforce that empty maps are nil instead
        // of zero.
        if (outgoing(cfg.voters).map.size > 0) {
            return new Error("cfg.Voters[1].map must be null when not joint");
        }
        if (cfg.learnersNext.size > 0) {
            return new Error("cfg.LearnersNext must be null when not joint");
        }
        if (cfg.autoLeave) {
            return new Error("AutoLeave must be false when not joint");
        }
    }

    return null;
}

/**
 * checkAndReturn calls checkInvariants on the input and returns either the
 * resulting error or the input.
 */
function checkAndReturn(cfg: Config, prs: ProgressMap): [Config, ProgressMap, NullableError] {
    const err = checkInvariants(cfg, prs);
    if (err !== null) {
        return [new Config({}), new Map<rid, Progress>(), err];
    }
    return [cfg, prs, null];
}

/**
 * Deletes from a map, nulling the map itself if it is empty after. State of the
 * map after operation is returned.
 */
function nilAwareDelete(m: Map<rid, object>, id: rid): Map<rid, object> {
    m.delete(id);
    return m;
}

/**
 * symdiff returns the count of the symmetric difference between the sets of
 * uint64s, i.e. len( (l - r) \union (r - l)).
 */
function symdiff(l: MajorityConfig, r: MajorityConfig): number {
    let n: number = 0;
    const pairs: [MajorityConfig, MajorityConfig][] = [
        [l, r], // count elems in l but not in r
        [r, l], // count elems in r but not in l
    ];

    for (const p of pairs) {
        for (const id of p[0].map.keys()) {
            if (!p[1].map.has(id)) {
                n++;
            }
        }
    }
    return n;
}

function joint(cfg: Config): boolean {
    return outgoing(cfg.voters).length() > 0;
}

function incoming(voters: JointConfig): MajorityConfig {
    return voters.config[0];
}
function outgoing(voters: JointConfig): MajorityConfig {
    return voters.config[1];
}

/**
 * Describe prints the type and NodeID of the configuration changes as a
 * space-delimited string.
 */
export function Describe(...ccs: ConfChangeSingle[]): string {
    const buf: string[] = [];
    for (const cc of ccs) {
        if (buf.length > 0) {
            buf.push(" ");
        }
        buf.push(cc.type + "(" + cc.nodeId + ")");
    }
    return buf.join("");
}

/**
 * ConfChangesToString is the inverse to ConfChangesFromString.
 */
export function confChangesToString(ccs: ConfChangeSingle[]): string {
    return ccs
        .map((cc: ConfChangeSingle) => {
            switch (cc.type) {
                case ConfChangeType.ConfChangeAddNode:
                    return "v" + cc.nodeId;
                case ConfChangeType.ConfChangeAddLearnerNode:
                    return "l" + cc.nodeId;
                case ConfChangeType.ConfChangeRemoveNode:
                    return "r" + cc.nodeId;
                case ConfChangeType.ConfChangeUpdateNode:
                    return "u" + cc.nodeId;
                default:
                    return "unknown" + cc.nodeId;
            }
        })
        .join(" ");
}

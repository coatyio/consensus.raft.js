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

import { ConfState, rid } from "../raftpb/raft.pb";
import { Progress, ProgressMap } from "./progress";
import { JointConfig } from "../quorum/joint";
import { MajorityConfig } from "../quorum/majority";
import { AckedIndexer, Index, VoteResult } from "../quorum/quorum";

// Config reflects the configuration tracked in a ProgressTracker.
export class Config {
    voters: JointConfig;

    // AutoLeave is true if the configuration is joint and a transition to the
    // incoming configuration should be carried out automatically by Raft when
    // this is possible. If false, the configuration will be joint until the
    // application initiates the transition manually.
    autoLeave: boolean;
    // Learners is a set of IDs corresponding to the learners active in the
    // current configuration.
    //
    // Invariant: Learners and Voters does not intersect, i.e. if a peer is in
    // either half of the joint config, it can't be a learner; if it is a
    // learner it can't be in either half of the joint config. This invariant
    // simplifies the implementation since it allows peers to have clarity about
    // its current role without taking into account joint consensus.
    learners: Map<rid, object>;
    // When we turn a voter into a learner during a joint consensus transition,
    // we cannot add the learner directly when entering the joint state. This is
    // because this would violate the invariant that the intersection of voters
    // and learners is empty. For example, assume a Voter is removed and
    // immediately re-added as a learner (or in other words, it is demoted):
    //
    // Initially, the configuration will be
    //
    //   voters:   {1 2 3} learners: {}
    //
    // and we want to demote 3. Entering the joint configuration, we naively get
    //
    //   voters:   {1 2} & {1 2 3} learners: {3}
    //
    // but this violates the invariant (3 is both voter and learner). Instead,
    // we get
    //
    //   voters:   {1 2} & {1 2 3} learners: {} next_learners: {3}
    //
    // Where 3 is now still purely a voter, but we are remembering the intention
    // to make it a learner upon transitioning into the final configuration:
    //
    //   voters:   {1 2} learners: {3} next_learners: {}
    //
    // Note that next_learners is not used while adding a learner that is not
    // also a voter in the joint config. In this case, the learner is added
    // right away when entering the joint configuration, so that it is caught up
    // as soon as possible.
    learnersNext: Map<rid, object>;

    constructor(
        param: {
            voters?: JointConfig;
            autoLeave?: boolean;
            learners?: Map<rid, object>;
            learnersNext?: Map<rid, object>;
        } = {}
    ) {
        this.voters = param.voters ?? new JointConfig();
        this.autoLeave = param.autoLeave ?? false;
        this.learners = param.learners ?? new Map<rid, object>();
        this.learnersNext = param.learnersNext ?? new Map<rid, object>();
    }

    toString(): string {
        const result: string[] = [];

        result.push("voters=" + this.voters);
        if (this.learners.size > 0) {
            const majConfig = new MajorityConfig();
            majConfig.map = this.learners;
            result.push(" learners=" + majConfig.toString());
        }
        if (this.learnersNext.size > 0) {
            const majConfig = new MajorityConfig();
            majConfig.map = this.learnersNext;
            result.push(" learners_next=" + majConfig.toString());
        }
        if (this.autoLeave) {
            result.push(" autoleave");
        }

        return result.join("");
    }

    // Clone returns a copy of the Config that shares no memory with the
    // original.
    clone(): Config {
        const newVoters: JointConfig = new JointConfig();

        const newMajorityConfig1 = new MajorityConfig();
        newMajorityConfig1.map = deepCopy(this.voters.config[0].map);
        newVoters.config[0] = newMajorityConfig1;

        const newMajorityConfig2 = new MajorityConfig();
        newMajorityConfig2.map = deepCopy(this.voters.config[1].map);
        newVoters.config[1] = newMajorityConfig2;

        return new Config({
            voters: newVoters,
            autoLeave: this.autoLeave,

            // NOTE: deepCopy is fine for now, but methods in the objects in the
            // map are not copied (currently only empty objects, so ok)
            learners: deepCopy(this.learners),
            learnersNext: deepCopy(this.learnersNext),
        });
    }
}

// tslint:disable-next-line:max-line-length source:
// https://stackoverflow.com/questions/28150967/typescript-cloning-object#:~:text=function%20deepCopy(obj,type%20isn%27t%20supported.%22)%3B%0A%7D
// with added Map support
// NOTE: Cannot handle boxed types and objects with reference cycles
export function deepCopy(obj: any): any {
    let copy: any;

    // Handle the 3 simple types, and null or undefined
    if (null === obj || "object" !== typeof obj) return obj;

    // Handle Date
    if (obj instanceof Date) {
        copy = new Date();
        copy.setTime(obj.getTime());
        return copy;
    }

    // Handle Array
    if (Array.isArray(obj)) {
        copy = [];
        for (let i = 0, len = obj.length; i < len; i++) {
            copy[i] = deepCopy(obj[i]);
        }
        return copy;
    }

    // Handle Maps
    if (obj instanceof Map) {
        copy = new Map();
        for (const [key, val] of obj) {
            copy.set(deepCopy(key), deepCopy(val));
        }
        return copy;
    }

    // Handle Object
    if (obj instanceof Object) {
        copy = {};
        for (const attr in obj) {
            if (obj.hasOwnProperty(attr)) copy[attr] = deepCopy(obj[attr]);
        }
        return copy;
    }

    throw new Error("Unable to deepCopy obj. Its type isn't supported.");
}

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
export class ProgressTracker {
    config: Config;

    progress: ProgressMap;

    votes: Map<rid, boolean>;

    maxInflight: number;

    constructor(
        param: {
            config?: Config;
            progress?: ProgressMap;
            votes?: Map<rid, boolean>;
            maxInflight?: number;
        } = {}
    ) {
        this.config = param.config ?? new Config();
        this.progress = param.progress ?? new Map<rid, Progress>();
        this.votes = param.votes ?? new Map<rid, boolean>();
        this.maxInflight = param.maxInflight ?? 0;
    }

    // ConfState returns a ConfState representing the active configuration.
    confState(): ConfState {
        const learnersConfig = new MajorityConfig();
        const learnersNextConfig = new MajorityConfig();
        learnersConfig.map = this.config.learners;
        learnersNextConfig.map = this.config.learnersNext;

        const result: ConfState = new ConfState({
            voters: this.config.voters.config[0].slice(),
            votersOutgoing: this.config.voters.config[1].slice(),
            learners: learnersConfig.slice(),
            learnersNext: learnersNextConfig.slice(),
            autoLeave: this.config.autoLeave,
        });
        return result;
    }

    // IsSingleton returns true if (and only if) there is only one voting member
    // (i.e. the leader) in the current configuration.
    isSingleton(): boolean {
        return (
            this.config.voters.config[0].length() === 1 &&
            this.config.voters.config[1].length() === 0
        );
    }

    // Visit invokes the supplied closure for all tracked progresses in stable
    // order.
    visit(f: (id: rid, pr: Progress) => void) {
        let n = this.progress.size;
        // NOTE: This is hot code that is not yet optimized in TypeScript as it
        // was in the go implementation.
        const ids: rid[] = [];
        for (const id of this.progress.keys()) {
            n--;
            ids[n] = id;
        }
        insertionSort(ids);

        for (const id of ids) {
            f(id, this.progress.get(id)!);
        }
    }

    // QuorumActive returns true if the quorum is active from the view of the
    // local raft state machine. Otherwise, it returns false.
    quorumActive(): boolean {
        const votes = new Map<rid, boolean>();
        this.visit((id: rid, pr: Progress) => {
            if (pr.isLearner) {
                return;
            }
            votes.set(id, pr.recentActive);
        });

        return this.config.voters.voteResult(votes) === VoteResult.VoteWon;
    }

    // VoterNodes returns a sorted slice of voters.
    voterNodes(): rid[] {
        const m: Map<rid, object> = this.config.voters.ids();
        const nodes: rid[] = [];
        for (const id of m.keys()) {
            nodes.push(id);
        }
        nodes.sort();
        return nodes;
    }

    // LearnerNodes returns a sorted slice of learners.
    learnerNodes(): rid[] {
        if (this.config.learners.size === 0) {
            return [];
        }
        const nodes: rid[] = [];
        for (const id of this.config.learners.keys()) {
            nodes.push(id);
        }
        nodes.sort();
        return nodes;
    }

    // ResetVotes prepares for a new round of vote counting via recordVote.
    resetVotes(): void {
        this.votes = new Map<rid, boolean>();
    }

    // RecordVote records that the node with the given id voted for this Raft
    // instance if v == true (and declined it otherwise).
    recordVote(id: rid, v: boolean): void {
        if (!this.votes.has(id)) {
            this.votes.set(id, v);
        }
    }

    // TallyVotes returns the number of granted and rejected Votes, and whether
    // the election outcome is known.
    tallyVotes(): [number, number, VoteResult] {
        let granted: number = 0;
        let rejected: number = 0;
        // Make sure to populate granted/rejected correctly even if the Votes
        // slice contains members no longer part of the configuration. This
        // doesn't really matter in the way the numbers are used (they're
        // informational), but might as well get it right.
        for (const [id, pr] of this.progress) {
            if (pr.isLearner) {
                continue;
            }
            const v: boolean | undefined = this.votes.get(id);
            if (v === undefined) {
                continue;
            }
            if (v) {
                granted++;
            } else {
                rejected++;
            }
        }
        const result = this.config.voters.voteResult(this.votes);
        return [granted, rejected, result];
    }

    // Committed returns the largest log index known to be committed based on
    // what the voting members of the group have acknowledged.
    committed(): number {
        const ackIndexer: MatchAckIndexer = new MatchAckIndexer();
        ackIndexer.map = this.progress;
        return this.config.voters.committedIndex(ackIndexer);
    }
}

function insertionSort(sl: rid[]) {
    const a = 0;
    const b = sl.length;
    for (let i = a + 1; i < b; i++) {
        for (let j = i; j > a && sl[j] < sl[j - 1]; j--) {
            const help = sl[j];
            sl[j] = sl[j - 1];
            sl[j - 1] = help;
        }
    }
}

// MakeProgressTracker initializes a ProgressTracker.
export function MakeProgressTracker(maxInflight: number): ProgressTracker {
    return new ProgressTracker({ maxInflight: maxInflight });
}

class MatchAckIndexer implements AckedIndexer {
    map: Map<rid, Progress> = new Map<rid, Progress>();

    // AckedIndex implements IndexLookuper.
    ackedIndex(id: rid): [Index, boolean] {
        const pr: Progress | undefined = this.map.get(id);
        if (pr === undefined) {
            return [0, false];
        } else {
            return [pr.match, true];
        }
    }
}

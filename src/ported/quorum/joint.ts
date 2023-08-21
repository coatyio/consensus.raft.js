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

import { MajorityConfig } from "./majority";
import { AckedIndexer, Index, VoteResult } from "./quorum";
import { rid } from "../raftpb/raft.pb";

/**
 * JointConfig is a configuration of two groups of (possibly overlapping)
 * majority configurations. Decisions require the support of both majorities.
 */
export class JointConfig {
    config: [MajorityConfig, MajorityConfig] = [new MajorityConfig(), new MajorityConfig()];

    toString(): string {
        if (this.config[1].length() > 0) {
            return this.config[0].toString() + "&&" + this.config[1].toString();
        }
        return this.config[0].toString();
    }

    /**
     * IDs returns a newly initialized map representing the set of voters
     * present in the joint configuration.
     */
    ids(): Map<rid, object> {
        const m = new Map<rid, object>();
        for (const cc of this.config) {
            if (cc.map.size === 0) {
                continue;
            }
            for (const id of cc.map.keys()) {
                m.set(id, {});
            }
        }
        return m;
    }

    /**
     * Describe returns a (multi-line) representation of the commit indexes for
     * the given lookuper.
     */
    describe(l: AckedIndexer): string {
        const config = new MajorityConfig();
        config.map = this.ids();
        return config.describe(l);
    }

    /**
     * CommittedIndex returns the largest committed index for the given joint
     * quorum. An index is jointly committed if it is committed in both
     * constituent majorities.
     */
    committedIndex(l: AckedIndexer): Index {
        const idx0: Index = this.config[0].committedIndex(l);
        const idx1: Index = this.config[1] ? this.config[1].committedIndex(l) : Number.MAX_VALUE;
        if (idx0 < idx1) {
            return idx0;
        }
        return idx1;
    }

    /**
     * VoteResult takes a mapping of voters to yes/no (true/false) votes and
     * returns a result indicating whether the vote is pending, lost, or won. A
     * joint quorum requires both majority quorums to vote in favor.
     */
    voteResult(votes: Map<rid, boolean>): VoteResult {
        const r1 = this.config[0].voteResult(votes);
        // As stated in the comments of MajorityConfig.voteResult, by
        // convention, elections on empty configs win.
        const r2 = this.config[1] ? this.config[1].voteResult(votes) : VoteResult.VoteWon;

        if (r1 === r2) {
            // If they agree, return the agreed state.
            return r1;
        }
        if (r1 === VoteResult.VoteLost || r2 === VoteResult.VoteLost) {
            // If either config has lost, loss is the only possible outcome.
            return VoteResult.VoteLost;
        }
        // One side won, the other one is pending, so the whole outcome is.
        return VoteResult.VotePending;
    }
}

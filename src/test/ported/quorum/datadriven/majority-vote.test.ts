// Copyright 2019 The etcd Authors
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

// This file contains all tests from quorum/testdata/majority_vote.txt This is
// done in code, since there is no equivalent datadriven test framework

import tap from "tap";

import { MajorityConfig } from "../../../../ported/quorum/majority";
import { VoteResult, voteResultToString } from "../../../../ported/quorum/quorum";
import { rid } from "../../../../ported/raftpb/raft.pb";
import { makeLookuper, Vote } from "../util";

tap.test("quorum/datadriven majority_vote", (t) => {
    const testData: { cfg: rid[]; votes: Vote[]; wResult: VoteResult }[] = [
        { cfg: [], votes: [], wResult: VoteResult.VoteWon }, // The empty config always announces a won vote.
        { cfg: ["1"], votes: [Vote.Pending], wResult: VoteResult.VotePending },
        { cfg: ["1"], votes: [Vote.No], wResult: VoteResult.VoteLost },
        { cfg: ["123"], votes: [Vote.Yes], wResult: VoteResult.VoteWon },

        {
            cfg: ["4", "8"],
            votes: [Vote.Pending, Vote.Pending],
            wResult: VoteResult.VotePending,
        },
        // With two voters, a single rejection loses the vote.
        {
            cfg: ["4", "8"],
            votes: [Vote.No, Vote.Pending],
            wResult: VoteResult.VoteLost,
        },
        {
            cfg: ["4", "8"],
            votes: [Vote.Yes, Vote.Pending],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["4", "8"],
            votes: [Vote.No, Vote.Yes],
            wResult: VoteResult.VoteLost,
        },
        {
            cfg: ["4", "8"],
            votes: [Vote.Yes, Vote.Yes],
            wResult: VoteResult.VoteWon,
        },

        {
            cfg: ["2", "4", "7"],
            votes: [Vote.Pending, Vote.Pending, Vote.Pending],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["2", "4", "7"],
            votes: [Vote.No, Vote.Pending, Vote.Pending],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["2", "4", "7"],
            votes: [Vote.Yes, Vote.Pending, Vote.Pending],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["2", "4", "7"],
            votes: [Vote.No, Vote.No, Vote.Pending],
            wResult: VoteResult.VoteLost,
        },
        {
            cfg: ["2", "4", "7"],
            votes: [Vote.Yes, Vote.No, Vote.Pending],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["2", "4", "7"],
            votes: [Vote.Yes, Vote.Yes, Vote.Pending],
            wResult: VoteResult.VoteWon,
        },
        {
            cfg: ["2", "4", "7"],
            votes: [Vote.Yes, Vote.Yes, Vote.No],
            wResult: VoteResult.VoteWon,
        },
        {
            cfg: ["2", "4", "7"],
            votes: [Vote.No, Vote.Yes, Vote.No],
            wResult: VoteResult.VoteLost,
        },
        // Test some random example with seven nodes (why not).
        {
            cfg: ["1", "2", "3", "4", "5", "6", "7"],
            votes: [
                Vote.Yes,
                Vote.Yes,
                Vote.No,
                Vote.Yes,
                Vote.Pending,
                Vote.Pending,
                Vote.Pending,
            ],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["1", "2", "3", "4", "5", "6", "7"],
            votes: [Vote.Pending, Vote.Yes, Vote.Yes, Vote.Pending, Vote.No, Vote.Yes, Vote.No],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["1", "2", "3", "4", "5", "6", "7"],
            votes: [Vote.Yes, Vote.Yes, Vote.No, Vote.Yes, Vote.Pending, Vote.No, Vote.Yes],
            wResult: VoteResult.VoteWon,
        },
        {
            cfg: ["1", "2", "3", "4", "5", "6", "7"],
            votes: [Vote.Yes, Vote.Yes, Vote.Pending, Vote.No, Vote.Yes, Vote.No, Vote.No],
            wResult: VoteResult.VotePending,
        },
        {
            cfg: ["1", "2", "3", "4", "5", "6", "7"],
            votes: [Vote.Yes, Vote.Yes, Vote.No, Vote.Yes, Vote.No, Vote.No, Vote.No],
            wResult: VoteResult.VoteLost,
        },
    ];

    for (let i = 0; i < testData.length; i++) {
        const tt = testData[i];

        // Build majority config
        const majConfig: MajorityConfig = new MajorityConfig();
        // every id in cfg needs to vote
        if (tt.cfg.length !== tt.votes.length) {
            t.fail(
                "#" + i + ": Error in the test data definition. Cfg and voter length are not equal"
            );
        }

        for (const id of tt.cfg) {
            majConfig.map.set(id, {});
        }

        const ll = makeLookuper(tt.votes, tt.cfg, []);
        const l: Map<rid, boolean> = new Map<rid, boolean>();
        for (const [id, v] of ll.map) {
            l.set(id, v !== Vote.No); // Indices with pending votes are not added to ll in makeLookuper.
        }

        // Test a majority quorum.
        const result = majConfig.voteResult(l);

        t.equal(
            result,
            tt.wResult,
            "#" +
            i +
            ": found " +
            voteResultToString(result) +
            ", want " +
            voteResultToString(tt.wResult)
        );
    }

    t.end();
});

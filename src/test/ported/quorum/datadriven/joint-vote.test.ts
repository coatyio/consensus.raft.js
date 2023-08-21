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

// This file contains all tests from quorum/testdata/joint_vote.txt This is done
// in code, since there is no equivalent datadriven test framework

import tap from "tap";

import { JointConfig } from "../../../../ported/quorum/joint";
import { MajorityConfig } from "../../../../ported/quorum/majority";
import { VoteResult, voteResultToString } from "../../../../ported/quorum/quorum";
import { rid } from "../../../../ported/raftpb/raft.pb";
import { makeLookuper, Vote } from "../util";

tap.test("quorum/datadriven joint_vote", (t) => {
    const testData: {
        cfg: rid[];
        cfgj: rid[];
        votes: Vote[];
        wResult: VoteResult;
    }[] = [
            // Empty joint config wins all votes. This isn't used in production.
            { cfg: [], cfgj: [], votes: [], wResult: VoteResult.VoteWon },
            // More examples with close to trivial configurations
            {
                cfg: ["1"],
                cfgj: [],
                votes: [Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            { cfg: ["1"], cfgj: [], votes: [Vote.Yes], wResult: VoteResult.VoteWon },
            { cfg: ["1"], cfgj: [], votes: [Vote.No], wResult: VoteResult.VoteLost },
            {
                cfg: ["1"],
                cfgj: ["1"],
                votes: [Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            { cfg: ["1"], cfgj: ["1"], votes: [Vote.Yes], wResult: VoteResult.VoteWon },
            { cfg: ["1"], cfgj: ["1"], votes: [Vote.No], wResult: VoteResult.VoteLost },

            {
                cfg: ["1"],
                cfgj: ["2"],
                votes: [Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1"],
                cfgj: ["2"],
                votes: [Vote.Yes, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1"],
                cfgj: ["2"],
                votes: [Vote.Yes, Vote.Yes],
                wResult: VoteResult.VoteWon,
            },
            {
                cfg: ["1"],
                cfgj: ["2"],
                votes: [Vote.Yes, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1"],
                cfgj: ["2"],
                votes: [Vote.No, Vote.Pending],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1"],
                cfgj: ["2"],
                votes: [Vote.No, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1"],
                cfgj: ["2"],
                votes: [Vote.No, Vote.Yes],
                wResult: VoteResult.VoteLost,
            },

            // Two node configs
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                votes: [Vote.Pending, Vote.Pending, Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                votes: [Vote.Yes, Vote.Pending, Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.No, Vote.Pending],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.No, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.Yes, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.Yes, Vote.Yes],
                wResult: VoteResult.VoteWon,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                votes: [Vote.Pending, Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                votes: [Vote.Pending, Vote.No, Vote.Pending],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                votes: [Vote.Yes, Vote.Yes, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                votes: [Vote.Yes, Vote.Yes, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                votes: [Vote.Yes, Vote.Yes, Vote.Yes],
                wResult: VoteResult.VoteWon,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                votes: [Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                votes: [Vote.Yes, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                votes: [Vote.Yes, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                votes: [Vote.No, Vote.Pending],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                votes: [Vote.No, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            // Simple example for overlapping three node configs.
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                votes: [Vote.Pending, Vote.Pending, Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                votes: [Vote.Pending, Vote.No, Vote.Pending, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                votes: [Vote.Pending, Vote.No, Vote.No, Vote.Pending],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                votes: [Vote.Pending, Vote.Yes, Vote.Yes, Vote.Pending],
                wResult: VoteResult.VoteWon,
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.No, Vote.Pending],
                wResult: VoteResult.VotePending,
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.No, Vote.No],
                wResult: VoteResult.VoteLost,
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                votes: [Vote.Yes, Vote.Yes, Vote.No, Vote.Yes],
                wResult: VoteResult.VoteWon,
            },
        ];

    for (let i = 0; i < testData.length; i++) {
        const tt = testData[i];

        // Build two majority configs
        const majConfig: MajorityConfig = new MajorityConfig();
        for (const id of tt.cfg) {
            majConfig.map.set(id, {});
        }
        const newConfig: MajorityConfig = new MajorityConfig();
        for (const id of tt.cfgj) {
            newConfig.map.set(id, {});
        }

        // Every voter id needs to vote
        const jointConfig: JointConfig = new JointConfig();
        jointConfig.config = [majConfig, newConfig];
        const voterIds = jointConfig.ids();
        if (voterIds.size !== tt.votes.length) {
            t.fail(
                "#" +
                i +
                ": Error in the test data definition. Voter ids and votes lengths are not equal"
            );
        }

        const ll = makeLookuper(tt.votes, tt.cfg, tt.cfgj);
        const l: Map<rid, boolean> = new Map<rid, boolean>();
        for (const [id, v] of ll.map) {
            l.set(id, v !== Vote.No); // Indices with pending votes are not added to ll in makeLookuper.
        }

        // Run a joint quorum test case.
        const testJointConfig = new JointConfig();
        testJointConfig.config = [majConfig, newConfig];
        const r = testJointConfig.voteResult(l);

        // Interchanging the majorities shouldn't make a difference. If it does,
        // print.
        const testJointConfig2 = new JointConfig();
        testJointConfig2.config = [newConfig, majConfig];
        const ar = testJointConfig2.voteResult(l);
        t.equal(
            r,
            ar,
            "#" +
            i +
            ": Interchanging majorities shouldn't make a difference: " +
            voteResultToString(r) +
            " should be equal: " +
            voteResultToString(ar)
        );

        t.equal(
            r,
            tt.wResult,
            "#" +
            i +
            ": found " +
            voteResultToString(r) +
            ", want " +
            voteResultToString(tt.wResult)
        );
    }

    t.end();
});

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

import tap from "tap";

import { ConfState } from "../../../ported/raftpb/raft.pb";

tap.test("ConfState.equivalent()", (t) => {
    const tests: {
        cs1: ConfState;
        cs2: ConfState;
        ok: boolean;
    }[] = [
            // Reordered voters and learners.
            {
                cs1: new ConfState({
                    voters: ["1", "2", "3"],
                    learners: ["5", "4", "6"],
                    votersOutgoing: ["9", "8", "7"],
                    learnersNext: ["10", "20", "15"],
                }),
                cs2: new ConfState({
                    voters: ["1", "2", "3"],
                    learners: ["4", "5", "6"],
                    votersOutgoing: ["7", "9", "8"],
                    learnersNext: ["20", "10", "15"],
                }),
                ok: true,
            },
            // Non-equivalent voters.
            {
                cs1: new ConfState({
                    voters: ["2", "1", "3"],
                }),
                cs2: new ConfState({
                    voters: ["1", "2", "3", "4"],
                }),
                ok: false,
            },
            {
                cs1: new ConfState({
                    voters: ["1", "4", "3"],
                }),
                cs2: new ConfState({
                    voters: ["2", "1", "3"],
                }),
                ok: false,
            },
            // Non-equivalent learners.
            {
                cs1: new ConfState({
                    learners: ["1", "2", "4", "3"],
                }),
                cs2: new ConfState({
                    learners: ["2", "1", "3"],
                }),
                ok: false,
            },
            // Sensitive to AutoLeave flag.
            {
                cs1: new ConfState({
                    autoLeave: true,
                }),
                cs2: new ConfState(),
                ok: false,
            },
        ];

    tests.forEach((tt, i, _) => {
        const result = tt.cs1.equivalent(tt.cs2);
        t.equal(result === null, tt.ok, "#" + i + ": check ok");
    });

    t.end();
});

process.exit();

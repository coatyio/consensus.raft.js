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

// This file contains all tests from quorum/testdata/joint_commit.txt This is
// done in code, since there is no equivalent datadriven test framework

import tap from "tap";

import { JointConfig } from "../../../../ported/quorum/joint";
import { MajorityConfig } from "../../../../ported/quorum/majority";
import { Index, indexToString } from "../../../../ported/quorum/quorum";
import { rid } from "../../../../ported/raftpb/raft.pb";
import { makeLookuper } from "../util";

tap.test("quorum/datadriven joint_commit", (t) => {
    // 0 in idxs are placeholders for voters whose information is not known
    const testData: {
        cfg: rid[];
        cfgj: rid[];
        idxs: Index[];
        wResult: string;
    }[] = [
            // No difference between a simple majority quorum and a simple majority
            // quorum joint with an empty majority quorum.
            //
            // Note that by specifying cfgj explicitly we tell the test harness to
            // treat the input as a joint quorum and not a majority quorum. If we
            // didn't specify cfgj=zero the test would pass just the same, but it
            // wouldn't be exercising the joint quorum path.
            {
                cfg: ["1", "2", "3"],
                cfgj: [],
                idxs: [100, 101, 99],
                wResult:
                    "       idx\n" +
                    "x>     100    (id=1)\n" +
                    "xx>    101    (id=2)\n" +
                    ">       99    (id=3)\n" +
                    "100\n",
            },
            // Joint nonoverlapping singleton quorums.
            {
                cfg: ["1"],
                cfgj: ["2"],
                idxs: [0, 0],
                wResult: "      idx\n" + "?       0    (id=1)\n" + "?       0    (id=2)\n" + "0\n",
            },
            // Voter 1 has 100 committed, 2 nothing. This means we definitely won't
            //  commit past 100.
            {
                cfg: ["1"],
                cfgj: ["2"],
                idxs: [100, 0],
                wResult: "      idx\n" + "x>    100    (id=1)\n" + "?       0    (id=2)\n" + "0\n",
            },
            // Committed index collapses once both majorities do, to the lower
            // index.
            {
                cfg: ["1"],
                cfgj: ["2"],
                idxs: [13, 100],
                wResult: "      idx\n" + ">      13    (id=1)\n" + "x>    100    (id=2)\n" + "13\n",
            },
            // Joint overlapping (i.e. identical) singleton quorum.
            {
                cfg: ["1"],
                cfgj: ["1"],
                idxs: [0],
                wResult: "     idx\n" + "?      0    (id=1)\n" + "0\n",
            },
            {
                cfg: ["1"],
                cfgj: ["1"],
                idxs: [100],
                wResult: "     idx\n" + ">    100    (id=1)\n" + "100\n",
            },

            // Two-node config joint with non-overlapping single node config

            {
                cfg: ["1", "3"],
                cfgj: ["2"],
                idxs: [0, 0, 0],
                wResult:
                    "       idx\n" +
                    "?        0    (id=1)\n" +
                    "?        0    (id=2)\n" +
                    "?        0    (id=3)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "3"],
                cfgj: ["2"],
                idxs: [100, 0, 0],
                wResult:
                    "       idx\n" +
                    "xx>    100    (id=1)\n" +
                    "?        0    (id=2)\n" +
                    "?        0    (id=3)\n" +
                    "0\n",
            },
            // 1 has 100 committed, 2 has 50 (collapsing half of the joint quorum to
            // 50).
            {
                cfg: ["1", "3"],
                cfgj: ["2"],
                idxs: [100, 0, 50],
                wResult:
                    "       idx\n" +
                    "xx>    100    (id=1)\n" +
                    "x>      50    (id=2)\n" +
                    "?        0    (id=3)\n" +
                    "0\n",
            },
            // 2 reports 45, collapsing the other half (to 45).
            {
                cfg: ["1", "3"],
                cfgj: ["2"],
                idxs: [100, 45, 50],
                wResult:
                    "       idx\n" +
                    "xx>    100    (id=1)\n" +
                    "x>      50    (id=2)\n" +
                    ">       45    (id=3)\n" +
                    "45\n",
            },

            // Two-node config with overlapping single-node config.

            {
                cfg: ["1", "2"],
                cfgj: ["2"],
                idxs: [0, 0],
                wResult: "      idx\n" + "?       0    (id=1)\n" + "?       0    (id=2)\n" + "0\n",
            },
            // 1 reports 100.
            {
                cfg: ["1", "2"],
                cfgj: ["2"],
                idxs: [100, 0],
                wResult: "      idx\n" + "x>    100    (id=1)\n" + "?       0    (id=2)\n" + "0\n",
            },
            // 2 reports 100.
            {
                cfg: ["1", "2"],
                cfgj: ["2"],
                idxs: [0, 100],
                wResult: "      idx\n" + "?       0    (id=1)\n" + "x>    100    (id=2)\n" + "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2"],
                idxs: [50, 100],
                wResult: "      idx\n" + ">      50    (id=1)\n" + "x>    100    (id=2)\n" + "50\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2"],
                idxs: [100, 50],
                wResult: "      idx\n" + "x>    100    (id=1)\n" + ">      50    (id=2)\n" + "50\n",
            },

            // Joint non-overlapping two-node configs.

            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                idxs: [50, 0, 0, 0],
                wResult:
                    "        idx\n" +
                    "xxx>     50    (id=1)\n" +
                    "?         0    (id=2)\n" +
                    "?         0    (id=3)\n" +
                    "?         0    (id=4)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                idxs: [50, 0, 49, 0],
                wResult:
                    "        idx\n" +
                    "xxx>     50    (id=1)\n" +
                    "?         0    (id=2)\n" +
                    "xx>      49    (id=3)\n" +
                    "?         0    (id=4)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                idxs: [50, 48, 49, 0],
                wResult:
                    "        idx\n" +
                    "xxx>     50    (id=1)\n" +
                    "x>       48    (id=2)\n" +
                    "xx>      49    (id=3)\n" +
                    "?         0    (id=4)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["3", "4"],
                idxs: [50, 48, 49, 47],
                wResult:
                    "        idx\n" +
                    "xxx>     50    (id=1)\n" +
                    "x>       48    (id=2)\n" +
                    "xx>      49    (id=3)\n" +
                    ">        47    (id=4)\n" +
                    "47\n",
            },

            // Joint overlapping two-node configs.

            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                idxs: [0, 0, 0],
                wResult:
                    "       idx\n" +
                    "?        0    (id=1)\n" +
                    "?        0    (id=2)\n" +
                    "?        0    (id=3)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                idxs: [100, 0, 0],
                wResult:
                    "       idx\n" +
                    "xx>    100    (id=1)\n" +
                    "?        0    (id=2)\n" +
                    "?        0    (id=3)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                idxs: [0, 100, 0],
                wResult:
                    "       idx\n" +
                    "?        0    (id=1)\n" +
                    "xx>    100    (id=2)\n" +
                    "?        0    (id=3)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                idxs: [0, 100, 99],
                wResult:
                    "       idx\n" +
                    "?        0    (id=1)\n" +
                    "xx>    100    (id=2)\n" +
                    "x>      99    (id=3)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["2", "3"],
                idxs: [101, 100, 99],
                wResult:
                    "       idx\n" +
                    "xx>    101    (id=1)\n" +
                    "x>     100    (id=2)\n" +
                    ">       99    (id=3)\n" +
                    "99\n",
            },

            // Joint identical two-node configs.

            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                idxs: [0, 0],
                wResult: "      idx\n" + "?       0    (id=1)\n" + "?       0    (id=2)\n" + "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                idxs: [0, 40],
                wResult: "      idx\n" + "?       0    (id=1)\n" + "x>     40    (id=2)\n" + "0\n",
            },
            {
                cfg: ["1", "2"],
                cfgj: ["1", "2"],
                idxs: [41, 40],
                wResult: "      idx\n" + "x>     41    (id=1)\n" + ">      40    (id=2)\n" + "40\n",
            },

            // Joint disjoint three-node configs.

            {
                cfg: ["1", "2", "3"],
                cfgj: ["4", "5", "6"],
                idxs: [0, 0, 0, 0, 0, 0],
                wResult:
                    "          idx\n" +
                    "?           0    (id=1)\n" +
                    "?           0    (id=2)\n" +
                    "?           0    (id=3)\n" +
                    "?           0    (id=4)\n" +
                    "?           0    (id=5)\n" +
                    "?           0    (id=6)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["4", "5", "6"],
                idxs: [100, 0, 0, 0, 0, 0],
                wResult:
                    "          idx\n" +
                    "xxxxx>    100    (id=1)\n" +
                    "?           0    (id=2)\n" +
                    "?           0    (id=3)\n" +
                    "?           0    (id=4)\n" +
                    "?           0    (id=5)\n" +
                    "?           0    (id=6)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["4", "5", "6"],
                idxs: [100, 0, 0, 90, 0, 0],
                wResult:
                    "          idx\n" +
                    "xxxxx>    100    (id=1)\n" +
                    "?           0    (id=2)\n" +
                    "?           0    (id=3)\n" +
                    "xxxx>      90    (id=4)\n" +
                    "?           0    (id=5)\n" +
                    "?           0    (id=6)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["4", "5", "6"],
                idxs: [100, 99, 0, 0, 0, 0],
                wResult:
                    "          idx\n" +
                    "xxxxx>    100    (id=1)\n" +
                    "xxxx>      99    (id=2)\n" +
                    "?           0    (id=3)\n" +
                    "?           0    (id=4)\n" +
                    "?           0    (id=5)\n" +
                    "?           0    (id=6)\n" +
                    "0\n",
            },
            //  First quorum <= 99, second one <= 97. Both quorums guarantee that 90
            //  is committed.
            {
                cfg: ["1", "2", "3"],
                cfgj: ["4", "5", "6"],
                idxs: [0, 99, 90, 97, 95, 0],
                wResult:
                    "          idx\n" +
                    "?           0    (id=1)\n" +
                    "xxxxx>     99    (id=2)\n" +
                    "xx>        90    (id=3)\n" +
                    "xxxx>      97    (id=4)\n" +
                    "xxx>       95    (id=5)\n" +
                    "?           0    (id=6)\n" +
                    "90\n",
            },
            // First quorum collapsed to 92. Second one already had at least 95
            //  committed, so the result also collapses.
            {
                cfg: ["1", "2", "3"],
                cfgj: ["4", "5", "6"],
                idxs: [92, 99, 90, 97, 95, 0],
                wResult:
                    "          idx\n" +
                    "xx>        92    (id=1)\n" +
                    "xxxxx>     99    (id=2)\n" +
                    "x>         90    (id=3)\n" +
                    "xxxx>      97    (id=4)\n" +
                    "xxx>       95    (id=5)\n" +
                    "?           0    (id=6)\n" +
                    "92\n",
            },
            // Second quorum collapses, but nothing changes in the output.
            {
                cfg: ["1", "2", "3"],
                cfgj: ["4", "5", "6"],
                idxs: [92, 99, 90, 97, 95, 77],
                wResult:
                    "          idx\n" +
                    "xx>        92    (id=1)\n" +
                    "xxxxx>     99    (id=2)\n" +
                    "x>         90    (id=3)\n" +
                    "xxxx>      97    (id=4)\n" +
                    "xxx>       95    (id=5)\n" +
                    ">          77    (id=6)\n" +
                    "92\n",
            },

            // Joint overlapping three-node configs.

            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [0, 0, 0, 0, 0],
                wResult:
                    "         idx\n" +
                    "?          0    (id=1)\n" +
                    "?          0    (id=2)\n" +
                    "?          0    (id=3)\n" +
                    "?          0    (id=4)\n" +
                    "?          0    (id=5)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [100, 0, 0, 0, 0],
                wResult:
                    "         idx\n" +
                    "xxxx>    100    (id=1)\n" +
                    "?          0    (id=2)\n" +
                    "?          0    (id=3)\n" +
                    "?          0    (id=4)\n" +
                    "?          0    (id=5)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [100, 101, 0, 0, 0],
                wResult:
                    "         idx\n" +
                    "xxx>     100    (id=1)\n" +
                    "xxxx>    101    (id=2)\n" +
                    "?          0    (id=3)\n" +
                    "?          0    (id=4)\n" +
                    "?          0    (id=5)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [100, 101, 100, 0, 0],
                wResult:
                    "         idx\n" +
                    "xx>      100    (id=1)\n" +
                    "xxxx>    101    (id=2)\n" +
                    ">        100    (id=3)\n" +
                    "?          0    (id=4)\n" +
                    "?          0    (id=5)\n" +
                    "0\n",
            },
            // Second quorum could commit either 98 or 99, but first quorum is open.
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [0, 100, 0, 99, 98],
                wResult:
                    "         idx\n" +
                    "?          0    (id=1)\n" +
                    "xxxx>    100    (id=2)\n" +
                    "?          0    (id=3)\n" +
                    "xxx>      99    (id=4)\n" +
                    "xx>       98    (id=5)\n" +
                    "0\n",
            },
            // Additionally, first quorum can commit either 100 or 99
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [0, 100, 99, 99, 98],
                wResult:
                    "         idx\n" +
                    "?          0    (id=1)\n" +
                    "xxxx>    100    (id=2)\n" +
                    "xx>       99    (id=3)\n" +
                    ">         99    (id=4)\n" +
                    "x>        98    (id=5)\n" +
                    "98\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [1, 100, 99, 99, 98],
                wResult:
                    "         idx\n" +
                    ">          1    (id=1)\n" +
                    "xxxx>    100    (id=2)\n" +
                    "xx>       99    (id=3)\n" +
                    ">         99    (id=4)\n" +
                    "x>        98    (id=5)\n" +
                    "98\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "4", "5"],
                idxs: [100, 100, 99, 99, 98],
                wResult:
                    "         idx\n" +
                    "xxx>     100    (id=1)\n" +
                    ">        100    (id=2)\n" +
                    "x>        99    (id=3)\n" +
                    ">         99    (id=4)\n" +
                    ">         98    (id=5)\n" +
                    "99\n",
            },

            // More overlap.

            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                idxs: [0, 0, 0, 0],
                wResult:
                    "        idx\n" +
                    "?         0    (id=1)\n" +
                    "?         0    (id=2)\n" +
                    "?         0    (id=3)\n" +
                    "?         0    (id=4)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                idxs: [0, 100, 99, 0],
                wResult:
                    "        idx\n" +
                    "?         0    (id=1)\n" +
                    "xxx>    100    (id=2)\n" +
                    "xx>      99    (id=3)\n" +
                    "?         0    (id=4)\n" +
                    "99\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                idxs: [98, 100, 99, 0],
                wResult:
                    "        idx\n" +
                    "x>       98    (id=1)\n" +
                    "xxx>    100    (id=2)\n" +
                    "xx>      99    (id=3)\n" +
                    "?         0    (id=4)\n" +
                    "99\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                idxs: [100, 100, 99, 0],
                wResult:
                    "        idx\n" +
                    "xx>     100    (id=1)\n" +
                    ">       100    (id=2)\n" +
                    "x>       99    (id=3)\n" +
                    "?         0    (id=4)\n" +
                    "99\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                idxs: [100, 100, 99, 98],
                wResult:
                    "        idx\n" +
                    "xx>     100    (id=1)\n" +
                    ">       100    (id=2)\n" +
                    "x>       99    (id=3)\n" +
                    ">        98    (id=4)\n" +
                    "99\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                idxs: [100, 0, 0, 101],
                wResult:
                    "        idx\n" +
                    "xx>     100    (id=1)\n" +
                    "?         0    (id=2)\n" +
                    "?         0    (id=3)\n" +
                    "xxx>    101    (id=4)\n" +
                    "0\n",
            },
            {
                cfg: ["1", "2", "3"],
                cfgj: ["2", "3", "4"],
                idxs: [100, 99, 0, 101],
                wResult:
                    "        idx\n" +
                    "xx>     100    (id=1)\n" +
                    "x>       99    (id=2)\n" +
                    "?         0    (id=3)\n" +
                    "xxx>    101    (id=4)\n" +
                    "99\n",
            },
            // Identical. This is also exercised in the test harness, so it's listed
            // here only briefly.
            {
                cfg: ["1", "2", "3"],
                cfgj: ["1", "2", "3"],
                idxs: [50, 45, 0],
                wResult:
                    "       idx\n" +
                    "xx>     50    (id=1)\n" +
                    "x>      45    (id=2)\n" +
                    "?        0    (id=3)\n" +
                    "45\n",
            },
        ];

    for (let i = 0; i < testData.length; i++) {
        const tt = testData[i];
        const actualResult = [];

        // Build majority config
        const majConfig: MajorityConfig = new MajorityConfig();
        for (const id of tt.cfg) {
            majConfig.map.set(id, {});
        }
        const newConfig: MajorityConfig = new MajorityConfig();
        for (const id of tt.cfgj) {
            newConfig.map.set(id, {});
        }

        {
            const input = tt.idxs;
            const helpConfig = new JointConfig();
            helpConfig.config = [majConfig, newConfig];
            const voters = helpConfig.ids();
            if (voters.size !== input.length) {
                t.fail(
                    "#" +
                    i +
                    ": Error in the test data definition. Voter ids and votes lengths are not equal"
                );
            }
        }

        const l = makeLookuper(tt.idxs, tt.cfg, tt.cfgj);

        const cc = new JointConfig();
        cc.config = [majConfig, newConfig];
        actualResult.push(cc.describe(l));
        const idx = cc.committedIndex(l);

        // Interchanging the majorities shouldn't make a difference. If it does,
        // print.
        const interchangedConfig = new JointConfig();
        interchangedConfig.config = [newConfig, majConfig];
        const aIdx = interchangedConfig.committedIndex(l);
        if (aIdx !== idx) {
            actualResult.push(indexToString(aIdx) + " <-- via symmetry\n");
        }

        actualResult.push(indexToString(idx) + "\n");

        const resultString = actualResult.join("");
        t.equal(
            resultString,
            tt.wResult,
            "#" + i + ": Actual Result:\n" + resultString + "\nwanted:\n" + tt.wResult
        );
    }

    t.end();
});

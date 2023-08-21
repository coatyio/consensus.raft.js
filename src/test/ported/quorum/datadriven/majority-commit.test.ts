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

// This file contains all tests from quorum/testdata/majority_commit.txt This is
// done in code, since there is no equivalent datadriven test framework

import tap from "tap";

import { JointConfig } from "../../../../ported/quorum/joint";
import { MajorityConfig } from "../../../../ported/quorum/majority";
import { AckedIndexer, Index, indexToString, MapAckIndexer } from "../../../../ported/quorum/quorum";
import { rid } from "../../../../ported/raftpb/raft.pb";
import { alternativeMajorityCommittedIndex, makeLookuper } from "../util";

tap.test("quorum/datadriven majority_commit", (t) => {
    // 0 in idxs are placeholders for voters whose information is not known
    const testData: { cfg: rid[]; idxs: Index[]; wResult: string }[] = [
        // The empty quorum commits "everything". This is useful for its use in
        // joint quorums.
        {
            cfg: [],
            idxs: [],
            wResult: "<empty majority quorum>∞\n",
        },
        // A single voter quorum is not final when no index is known.
        {
            cfg: ["1"],
            idxs: [0],
            wResult: "     idx\n" + "?      0    (id=1)\n" + "0\n",
        },
        // When an index is known, that's the committed index, and that's final.
        {
            cfg: ["1"],
            idxs: [12],
            wResult: "     idx\n" + ">     12    (id=1)\n" + "12\n",
        },

        // With two nodes, start out similarly.
        {
            cfg: ["1", "2"],
            idxs: [0, 0],
            wResult: "      idx\n" + "?       0    (id=1)\n" + "?       0    (id=2)\n" + "0\n",
        },
        // The first committed index becomes known (for n1). Nothing changes in
        // the output because idx=12 is not known to be on a quorum (which is
        // both nodes).
        {
            cfg: ["1", "2"],
            idxs: [12, 0],
            wResult: "      idx\n" + "x>     12    (id=1)\n" + "?       0    (id=2)\n" + "0\n",
        },
        // The second index comes in and finalize the decision. The result will
        // be the smaller of the two indexes.
        {
            cfg: ["1", "2"],
            idxs: [12, 5],
            wResult: "      idx\n" + "x>     12    (id=1)\n" + ">       5    (id=2)\n" + "5\n",
        },

        // No surprises for three nodes.
        {
            cfg: ["1", "2", "3"],
            idxs: [0, 0, 0],
            wResult:
                "       idx\n" +
                "?        0    (id=1)\n" +
                "?        0    (id=2)\n" +
                "?        0    (id=3)\n" +
                "0\n",
        },

        {
            cfg: ["1", "2", "3"],
            idxs: [12, 0, 0],
            wResult:
                "       idx\n" +
                "xx>     12    (id=1)\n" +
                "?        0    (id=2)\n" +
                "?        0    (id=3)\n" +
                "0\n",
        },
        //  We see a committed index, but a higher committed index for the last
        // pending votes could change (increment) the outcome, so not final yet.
        {
            cfg: ["1", "2", "3"],
            idxs: [12, 5, 0],
            wResult:
                "       idx\n" +
                "xx>     12    (id=1)\n" +
                "x>       5    (id=2)\n" +
                "?        0    (id=3)\n" +
                "5\n",
        },
        // a) the case in which it does:
        {
            cfg: ["1", "2", "3"],
            idxs: [12, 5, 6],
            wResult:
                "       idx\n" +
                "xx>     12    (id=1)\n" +
                ">        5    (id=2)\n" +
                "x>       6    (id=3)\n" +
                "6\n",
        },
        // b) the case in which it does not:
        {
            cfg: ["1", "2", "3"],
            idxs: [12, 5, 4],
            wResult:
                "       idx\n" +
                "xx>     12    (id=1)\n" +
                "x>       5    (id=2)\n" +
                ">        4    (id=3)\n" +
                "5\n",
        },
        // c) a different case in which the last index is pending but it has no
        // chance of swaying the outcome (because nobody in the current quorum
        // agrees on anything higher than the candidate):
        {
            cfg: ["1", "2", "3"],
            idxs: [5, 5, 0],
            wResult:
                "       idx\n" +
                "x>       5    (id=1)\n" +
                ">        5    (id=2)\n" +
                "?        0    (id=3)\n" +
                "5\n",
        },
        // c) continued: Doesn't matter what shows up last. The result is final.
        {
            cfg: ["1", "2", "3"],
            idxs: [5, 5, 12],
            wResult:
                "       idx\n" +
                ">        5    (id=1)\n" +
                ">        5    (id=2)\n" +
                "xx>     12    (id=3)\n" +
                "5\n",
        },
        // With all committed idx known, the result is final.
        {
            cfg: ["1", "2", "3"],
            idxs: [100, 101, 103],
            wResult:
                "       idx\n" +
                ">      100    (id=1)\n" +
                "x>     101    (id=2)\n" +
                "xx>    103    (id=3)\n" +
                "101\n",
        },

        // Some more complicated examples. Similar to case c) above. The result
        // is already final because no index higher than 103 is one short of
        // quorum.
        {
            cfg: ["1", "2", "3", "4", "5"],
            idxs: [101, 104, 103, 103, 0],
            wResult:
                "         idx\n" +
                "x>       101    (id=1)\n" +
                "xxxx>    104    (id=2)\n" +
                "xx>      103    (id=3)\n" +
                ">        103    (id=4)\n" +
                "?          0    (id=5)\n" +
                "103\n",
        },
        // A similar case which is not final because another vote for >= 103
        // would change the outcome.
        {
            cfg: ["1", "2", "3", "4", "5"],
            idxs: [101, 102, 103, 103, 0],
            wResult:
                "         idx\n" +
                "x>       101    (id=1)\n" +
                "xx>      102    (id=2)\n" +
                "xxx>     103    (id=3)\n" +
                ">        103    (id=4)\n" +
                "?          0    (id=5)\n" +
                "102\n",
        },
    ];

    for (let i = 0; i < testData.length; i++) {
        const tt = testData[i];
        const actualResult = [];

        // Build majority config
        const majConfig: MajorityConfig = new MajorityConfig();
        // every id in cfg needs to have a committed index (or 0 as placeholder
        // for unknown)
        if (tt.cfg.length !== tt.idxs.length) {
            t.fail(
                "#" + i + ": Error in the test data definition. Cfg and idx length are not equal"
            );
        }

        for (const id of tt.cfg) {
            majConfig.map.set(id, {});
        }

        const l = makeLookuper(tt.idxs, tt.cfg, []);

        const idx = majConfig.committedIndex(l);
        actualResult.push(majConfig.describe(l));

        // These alternative computations should return the same result. If not,
        // print to the output.
        let aIdx: Index = alternativeMajorityCommittedIndex(majConfig, l);
        if (aIdx !== idx) {
            actualResult.push(indexToString(aIdx) + "<-- via alternative computation\n");
        }
        // Joining a majority with the empty majority should give same result.
        const helperJointConfig = new JointConfig();
        helperJointConfig.config = [majConfig, new MajorityConfig()];
        aIdx = helperJointConfig.committedIndex(l);
        if (aIdx !== idx) {
            actualResult.push(indexToString(aIdx) + "<-- via zero-joint quorum\n");
        }
        // Joining a majority with itself should give same result.
        const helperJointConfig2 = new JointConfig();
        helperJointConfig2.config = [majConfig, majConfig];
        aIdx = helperJointConfig2.committedIndex(l);
        if (aIdx !== idx) {
            actualResult.push(indexToString(aIdx) + " <-- via self-joint quorum\n");
        }

        function overlay(
            c: MajorityConfig,
            overlayL: AckedIndexer,
            id: rid,
            overlayIdx: Index
        ): AckedIndexer {
            const ll = new MapAckIndexer();
            for (const iid of c.map.keys()) {
                if (iid === id) {
                    ll.map.set(iid, overlayIdx);
                } else {
                    const [ackedIndex, ok] = overlayL.ackedIndex(iid);
                    if (ok) {
                        ll.map.set(iid, ackedIndex);
                    }
                }
            }
            return ll;
        }

        for (const id of majConfig.map.keys()) {
            const [iidx, _] = l.ackedIndex(id);
            if (idx > iidx && iidx > 0) {
                // If the committed index was definitely above the currently
                // inspected idx, the result shouldn't change if we lower it
                // further.
                let lo = overlay(majConfig, l, id, iidx - 1);
                aIdx = majConfig.committedIndex(lo);
                if (aIdx !== idx) {
                    actualResult.push(indexToString(aIdx) + " <-- overlaying " + id + "->" + iidx);
                }
                lo = overlay(majConfig, l, id, 0);
                aIdx = majConfig.committedIndex(lo);
                if (aIdx !== idx) {
                    actualResult.push(indexToString(aIdx) + " <-- overlaying " + id + "->0");
                }
            }
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

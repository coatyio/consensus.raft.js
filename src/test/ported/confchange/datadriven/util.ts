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

import { Changer } from "../../../../ported/confchange/confchange";
import { ConfChangeSingle, ConfChangeType, rid } from "../../../../ported/raftpb/raft.pb";
import { ProgressMap, progressMapToString } from "../../../../ported/tracker/progress";
import { Config, MakeProgressTracker } from "../../../../ported/tracker/tracker";
import { NullableError } from "../../../../ported/util";

export type TestCommand = "simple" | "enterJoint" | "leaveJoint";

export type TestArgType = "v" | "l" | "r" | "u";

export type TestDataType = {
    cmd: TestCommand;
    autoLeave?: boolean;
    args?: [TestArgType, rid][];
    wResult: string;
}[];

/**
 * Executes the tests as specified by testData. t Must be a tap.Test object
 * @param testData The data for the tests to execute
 * @param t The test object
 */
export function executeTest(testData: TestDataType, t: any): void {
    const tr = MakeProgressTracker(10);
    const c = new Changer(tr, 0); // incremented in this test with each cmd

    // The test data use the commands
    // - simple: run a simple conf change (i.e. no joint consensus),
    // - enter-joint: enter a joint config, and
    // - leave-joint: leave a joint config. The first two take a list of config
    //   changes, which have the following syntax:
    // - vn: make n a voter,
    // - ln: make n a learner,
    // - rn: remove n, and
    // - un: update n.
    for (let i = 0; i < testData.length; i++) {
        const tt = testData[i];

        const confChanges: ConfChangeSingle[] = [];
        for (const [argType, id] of tt.args !== undefined ? tt.args : []) {
            const confChangeSingle = new ConfChangeSingle();

            switch (argType) {
                case "v":
                    confChangeSingle.type = ConfChangeType.ConfChangeAddNode;
                    break;
                case "l":
                    confChangeSingle.type = ConfChangeType.ConfChangeAddLearnerNode;
                    break;
                case "r":
                    confChangeSingle.type = ConfChangeType.ConfChangeRemoveNode;
                    break;
                case "u":
                    confChangeSingle.type = ConfChangeType.ConfChangeUpdateNode;
                    break;
                default:
                    t.fail("#" + i + ": Unknown Arg Type: " + argType);
            }

            confChangeSingle.nodeId = id;
            confChanges.push(confChangeSingle);
        }

        let cfg: Config | null = null;
        let prs: ProgressMap | null = null;
        let err: NullableError = null;

        switch (tt.cmd) {
            case "simple":
                [cfg, prs, err] = c.simple(...confChanges);
                break;
            case "enterJoint":
                const autoLeave = tt.autoLeave !== undefined ? tt.autoLeave : false;
                [cfg, prs, err] = c.enterJoint(autoLeave, ...confChanges);
                break;
            case "leaveJoint":
                t.ok(
                    confChanges.length === 0,
                    "#" + i + ": Error: leaveJoint takes no arguments, but found: " + confChanges
                );
                [cfg, prs, err] = c.leaveJoint();
                break;
            default:
                t.fail("#" + i + ": Unknown Cmd: " + tt.cmd);
        }

        if (err !== null) {
            // If an error is returned, check if it is the correct one
            t.equal(
                err.message + "\n",
                tt.wResult,
                "#" +
                i +
                ": found error with message: " +
                (err.message + "\n") +
                ", wanted " +
                tt.wResult
            );
        } else {
            c.tracker.config = cfg!;
            c.tracker.progress = prs!;

            const resultString =
                c.tracker.config.toString() + "\n" + progressMapToString(c.tracker.progress);
            t.equal(
                resultString,
                tt.wResult,
                "#" + i + ": found: " + resultString + ", wanted " + tt.wResult
            );
        }

        c.lastIndex++;
    }
}

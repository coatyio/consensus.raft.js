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

import { executeTest, TestDataType } from "./util";

tap.test("confchange/datadriven joint_idempotency", (t) => {
    const testData: TestDataType = [
        // Verify that operations upon entering the joint state are idempotent,
        // i.e. removing an absent node is fine, etc.
        {
            cmd: "simple",
            args: [["v", "1"]],
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=0\n",
        },
        {
            cmd: "enterJoint",
            args: [
                ["r", "1"],
                ["r", "2"],
                ["r", "9"],
                ["v", "2"],
                ["v", "3"],
                ["v", "4"],
                ["v", "2"],
                ["v", "3"],
                ["v", "4"],
                ["l", "2"],
                ["l", "2"],
                ["r", "4"],
                ["r", "4"],
                ["l", "1"],
                ["l", "1"],
            ],
            wResult:
                "voters=(3)&&(1) learners=(2) learners_next=(1)\n" +
                "1: StateProbe match=0 next=0\n" +
                "2: StateProbe match=0 next=1 learner\n" +
                "3: StateProbe match=0 next=1\n",
        },
        {
            cmd: "leaveJoint",
            wResult:
                "voters=(3) learners=(1 2)\n" +
                "1: StateProbe match=0 next=0 learner\n" +
                "2: StateProbe match=0 next=1 learner\n" +
                "3: StateProbe match=0 next=1\n",
        },
    ];

    executeTest(testData, t);

    t.end();
});

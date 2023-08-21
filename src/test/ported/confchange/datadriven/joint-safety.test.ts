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

tap.test("confchange/datadriven joint_safety", (t) => {
    const testData: TestDataType = [
        {
            cmd: "leaveJoint",
            wResult: "can't leave a non-joint config\n",
        },
        {
            cmd: "enterJoint",
            wResult: "can't make a zero-voter config joint\n",
        },
        {
            cmd: "enterJoint",
            args: [["v", "1"]],
            wResult: "can't make a zero-voter config joint\n",
        },
        {
            cmd: "simple",
            args: [["v", "1"]],
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=3\n",
        },
        {
            cmd: "leaveJoint",
            wResult: "can't leave a non-joint config\n",
        },
        // Can enter into joint config.
        {
            cmd: "enterJoint",
            wResult: "voters=(1)&&(1)\n" + "1: StateProbe match=0 next=3\n",
        },
        {
            cmd: "enterJoint",
            wResult: "config is already joint\n",
        },
        {
            cmd: "leaveJoint",
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=3\n",
        },
        {
            cmd: "leaveJoint",
            wResult: "can't leave a non-joint config\n",
        },
        // Can enter again, this time with some ops.
        {
            cmd: "enterJoint",
            args: [
                ["r", "1"],
                ["v", "2"],
                ["v", "3"],
                ["l", "4"],
            ],
            wResult:
                "voters=(2 3)&&(1) learners=(4)\n" +
                "1: StateProbe match=0 next=3\n" +
                "2: StateProbe match=0 next=9\n" +
                "3: StateProbe match=0 next=9\n" +
                "4: StateProbe match=0 next=9 learner\n",
        },
        {
            cmd: "enterJoint",
            wResult: "config is already joint\n",
        },
        {
            cmd: "enterJoint",
            args: [["v", "12"]],
            wResult: "config is already joint\n",
        },
        {
            cmd: "simple",
            args: [["l", "15"]],
            wResult: "can't apply simple config change in joint config\n",
        },
        {
            cmd: "leaveJoint",
            wResult:
                "voters=(2 3) learners=(4)\n" +
                "2: StateProbe match=0 next=9\n" +
                "3: StateProbe match=0 next=9\n" +
                "4: StateProbe match=0 next=9 learner\n",
        },
        {
            cmd: "simple",
            args: [["l", "9"]],
            wResult:
                "voters=(2 3) learners=(4 9)\n" +
                "2: StateProbe match=0 next=9\n" +
                "3: StateProbe match=0 next=9\n" +
                "4: StateProbe match=0 next=9 learner\n" +
                "9: StateProbe match=0 next=14 learner\n",
        },
    ];

    executeTest(testData, t);

    t.end();
});

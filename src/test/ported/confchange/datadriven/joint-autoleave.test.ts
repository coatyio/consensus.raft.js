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

tap.test("confchange/datadriven joint_autoleave", (t) => {
    const testData: TestDataType = [
        // Test the autoleave argument to EnterJoint. It defaults to false in
        // the datadriven tests. The flag has no associated semantics in this
        // package, it is simply passed through.
        {
            cmd: "simple",
            args: [["v", "1"]],
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=0\n",
        },
        // Autoleave is reflected in the config.
        {
            cmd: "enterJoint",
            autoLeave: true,
            args: [
                ["v", "2"],
                ["v", "3"],
            ],
            wResult:
                "voters=(1 2 3)&&(1) autoleave\n" +
                "1: StateProbe match=0 next=0\n" +
                "2: StateProbe match=0 next=1\n" +
                "3: StateProbe match=0 next=1\n",
        },
        // Can't enter-joint twice, even if autoleave changes.
        {
            cmd: "enterJoint",
            autoLeave: false,
            args: [],
            wResult: "config is already joint\n",
        },
        {
            cmd: "leaveJoint",
            wResult:
                "voters=(1 2 3)\n" +
                "1: StateProbe match=0 next=0\n" +
                "2: StateProbe match=0 next=1\n" +
                "3: StateProbe match=0 next=1\n",
        },
    ];

    executeTest(testData, t);

    t.end();
});

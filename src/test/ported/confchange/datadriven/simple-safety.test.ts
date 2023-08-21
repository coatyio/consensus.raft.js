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

tap.test("confchange/datadriven simple_safety", (t) => {
    const testData: TestDataType = [
        {
            cmd: "simple",
            args: [["l", "1"]],
            wResult: "removed all voters\n",
        },
        {
            cmd: "simple",
            args: [["v", "1"]],
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=1\n",
        },
        {
            cmd: "simple",
            args: [
                ["v", "2"],
                ["l", "3"],
            ],
            wResult:
                "voters=(1 2) learners=(3)\n" +
                "1: StateProbe match=0 next=1\n" +
                "2: StateProbe match=0 next=2\n" +
                "3: StateProbe match=0 next=2 learner\n",
        },
        {
            cmd: "simple",
            args: [
                ["r", "1"],
                ["v", "5"],
            ],
            wResult: "more than one voter changed without entering joint config\n",
        },
        {
            cmd: "simple",
            args: [
                ["r", "1"],
                ["r", "2"],
            ],
            wResult: "removed all voters\n",
        },
        {
            cmd: "simple",
            args: [
                ["v", "3"],
                ["v", "4"],
            ],
            wResult: "more than one voter changed without entering joint config\n",
        },
        {
            cmd: "simple",
            args: [
                ["l", "1"],
                ["v", "5"],
            ],
            wResult: "more than one voter changed without entering joint config\n",
        },
        {
            cmd: "simple",
            args: [
                ["l", "1"],
                ["l", "2"],
            ],
            wResult: "removed all voters\n",
        },
        {
            cmd: "simple",
            args: [
                ["l", "2"],
                ["l", "3"],
                ["l", "4"],
                ["l", "5"],
            ],
            wResult:
                "voters=(1) learners=(2 3 4 5)\n" +
                "1: StateProbe match=0 next=1\n" +
                "2: StateProbe match=0 next=2 learner\n" +
                "3: StateProbe match=0 next=2 learner\n" +
                "4: StateProbe match=0 next=8 learner\n" +
                "5: StateProbe match=0 next=8 learner\n",
        },
        {
            cmd: "simple",
            args: [["r", "1"]],
            wResult: "removed all voters\n",
        },
        {
            cmd: "simple",
            args: [
                ["r", "2"],
                ["r", "3"],
                ["r", "4"],
                ["r", "5"],
            ],
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=1\n",
        },
    ];

    executeTest(testData, t);

    t.end();
});

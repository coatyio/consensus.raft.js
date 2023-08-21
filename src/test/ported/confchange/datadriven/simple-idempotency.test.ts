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

tap.test("confchange/datadriven simple_idempotency", (t) => {
    const testData: TestDataType = [
        {
            cmd: "simple",
            args: [["v", "1"]],
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=0\n",
        },
        {
            cmd: "simple",
            args: [["v", "1"]],
            wResult: "voters=(1)\n" + "1: StateProbe match=0 next=0\n",
        },
        {
            cmd: "simple",
            args: [["v", "2"]],
            wResult:
                "voters=(1 2)\n" +
                "1: StateProbe match=0 next=0\n" +
                "2: StateProbe match=0 next=2\n",
        },
        {
            cmd: "simple",
            args: [["l", "1"]],
            wResult:
                "voters=(2) learners=(1)\n" +
                "1: StateProbe match=0 next=0 learner\n" +
                "2: StateProbe match=0 next=2\n",
        },
        {
            cmd: "simple",
            args: [["l", "1"]],
            wResult:
                "voters=(2) learners=(1)\n" +
                "1: StateProbe match=0 next=0 learner\n" +
                "2: StateProbe match=0 next=2\n",
        },
        {
            cmd: "simple",
            args: [["r", "1"]],
            wResult: "voters=(2)\n" + "2: StateProbe match=0 next=2\n",
        },
        {
            cmd: "simple",
            args: [["r", "1"]],
            wResult: "voters=(2)\n" + "2: StateProbe match=0 next=2\n",
        },
        {
            cmd: "simple",
            args: [["v", "3"]],
            wResult:
                "voters=(2 3)\n" +
                "2: StateProbe match=0 next=2\n" +
                "3: StateProbe match=0 next=7\n",
        },
        {
            cmd: "simple",
            args: [["r", "3"]],
            wResult: "voters=(2)\n" + "2: StateProbe match=0 next=2\n",
        },
        {
            cmd: "simple",
            args: [["r", "3"]],
            wResult: "voters=(2)\n" + "2: StateProbe match=0 next=2\n",
        },
        {
            cmd: "simple",
            args: [["r", "4"]],
            wResult: "voters=(2)\n" + "2: StateProbe match=0 next=2\n",
        },
    ];

    executeTest(testData, t);

    t.end();
});

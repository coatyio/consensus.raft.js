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

import { Inflights, NewInflights } from "../../../ported/tracker/inflights";

tap.test("tracker/inflights Add()", (t) => {
    // no rotating case
    const inflight = new Inflights({ size: 10, buffer: new Array(10).fill(0) });

    for (let i = 0; i < 5; i++) {
        inflight.add(i);
    }
    const wantIn = new Inflights({
        size: 10,
        start: 0,
        _count: 5,
        buffer: [0, 1, 2, 3, 4, 0, 0, 0, 0, 0],
    });

    t.strictSame(inflight, wantIn, "inflight = " + inflight + ", want " + wantIn);
    for (let i = 5; i < 10; i++) {
        inflight.add(i);
    }

    const wantIn2 = new Inflights({
        size: 10,
        start: 0,
        _count: 10,
        buffer: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    });

    t.strictSame(inflight, wantIn2, "inflight = " + inflight + ", want " + wantIn2);

    // rotating case
    const inflight2 = new Inflights({
        size: 10,
        start: 5,
        buffer: new Array(10).fill(0),
    });

    for (let i = 0; i < 5; i++) {
        inflight2.add(i);
    }

    const wantIn21 = new Inflights({
        size: 10,
        start: 5,
        _count: 5,
        buffer: [0, 0, 0, 0, 0, 0, 1, 2, 3, 4],
    });

    t.strictSame(inflight2, wantIn21, "inflight = " + inflight2 + ", want " + wantIn21);

    for (let i = 5; i < 10; i++) {
        inflight2.add(i);
    }

    const wantIn22 = new Inflights({
        size: 10,
        start: 5,
        _count: 10,
        buffer: [5, 6, 7, 8, 9, 0, 1, 2, 3, 4],
    });

    t.strictSame(inflight2, wantIn22, "inflight = " + inflight2 + ", want " + wantIn22);

    t.end();
});

tap.test("tracker/inflights FreeTo()", (t) => {
    // no rotating case
    const inflight: Inflights = NewInflights(10);
    for (let i = 0; i < 10; i++) {
        inflight.add(i);
    }

    inflight.freeLE(4);

    const wantIn: Inflights = new Inflights({
        size: 10,
        start: 5,
        _count: 5,
        buffer: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    });

    t.strictSame(inflight, wantIn, "inflights= " + inflight + ", want " + wantIn);

    inflight.freeLE(8);

    const wantIn2 = new Inflights({
        size: 10,
        start: 9,
        _count: 1,
        buffer: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    });

    t.strictSame(inflight, wantIn2, "inflights= " + inflight + ", want " + wantIn2);

    // rotating case
    for (let i = 10; i < 15; i++) {
        inflight.add(i);
    }

    inflight.freeLE(12);

    const wantIn3 = new Inflights({
        size: 10,
        start: 3,
        _count: 2,
        buffer: [10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
    });

    t.strictSame(inflight, wantIn3, "inflights = " + inflight + ", want " + wantIn3);

    inflight.freeLE(14);

    const wantIn4 = new Inflights({
        size: 10,
        start: 0,
        _count: 0,
        buffer: [10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
    });

    t.strictSame(inflight, wantIn4, "inflights = " + inflight + ", want " + wantIn4);

    t.end();
});

tap.test("tracker/inflights FreeFirstOne()", (t) => {
    const inflight = NewInflights(10);

    for (let i = 0; i < 10; i++) {
        inflight.add(i);
    }

    inflight.freeFirstOne();

    const wantIn = new Inflights({
        size: 10,
        start: 1,
        _count: 9,
        buffer: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    });

    t.strictSame(inflight, wantIn, "inflights = " + inflight + ", want " + wantIn);

    t.end();
});

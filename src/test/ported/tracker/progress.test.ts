// Copyright 2015 The etcd Authors
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

import { NewInflights } from "../../../ported/tracker/inflights";
import { Progress } from "../../../ported/tracker/progress";
import { ProgressStateType } from "../../../ported/tracker/state";

tap.test("tracker/progress toString()", (t) => {
    const ins = NewInflights(1);
    ins.add(123);
    const pr: Progress = new Progress({
        match: 1,
        next: 2,
        state: ProgressStateType.StateSnapshot,
        pendingSnapshot: 123,
        recentActive: false,
        probeSent: true,
        isLearner: true,
        inflights: ins,
    });
    const exp =
        "StateSnapshot match=1 next=2 learner paused pendingSnap=123 inactive inflight=1[full]";
    const act = pr.toString();

    t.equal(exp, act, "assert equality of string representations");

    t.end();
});

tap.test("tracker/progress isPaused()", (t) => {
    const tests: { state: ProgressStateType; paused: boolean; w: boolean }[] = [
        { state: ProgressStateType.StateProbe, paused: false, w: false },
        { state: ProgressStateType.StateProbe, paused: true, w: true },
        { state: ProgressStateType.StateReplicate, paused: false, w: false },
        { state: ProgressStateType.StateReplicate, paused: true, w: false },
        { state: ProgressStateType.StateSnapshot, paused: false, w: true },
        { state: ProgressStateType.StateSnapshot, paused: true, w: true },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];
        const p = new Progress({
            state: tt.state,
            probeSent: tt.paused,
            inflights: NewInflights(256),
        });

        const g = p.IsPaused();
        t.equal(g, tt.w, "#" + i + ": paused= " + g + ", want " + tt.w);
    }

    t.end();
});

// TestProgressResume ensures that MaybeUpdate and MaybeDecrTo will reset
// ProbeSent.
tap.test("tracker/progress progressResume()", (t) => {
    const p = new Progress({
        next: 2,
        probeSent: true,
    });

    p.MaybeDecrTo(1, 1);
    t.notOk(p.probeSent, "paused= " + p.probeSent + ", want false");

    p.probeSent = true;
    p.MaybeUpdate(2);

    t.notOk(p.probeSent, "paused= " + p.probeSent + ", want false");

    t.end();
});

tap.test("tracker/progress becomeProbe()", (t) => {
    const match = 1;
    const tests: { p: Progress; wnext: number }[] = [
        {
            p: new Progress({
                state: ProgressStateType.StateReplicate,
                match: match,
                next: 5,
                inflights: NewInflights(256),
            }),
            wnext: 2,
        },
        {
            // snapshot finish
            p: new Progress({
                state: ProgressStateType.StateSnapshot,
                match: match,
                next: 5,
                pendingSnapshot: 10,
                inflights: NewInflights(256),
            }),
            wnext: 11,
        },
        {
            // snapshot failure
            p: new Progress({
                state: ProgressStateType.StateSnapshot,
                match: match,
                next: 5,
                pendingSnapshot: 0,
                inflights: NewInflights(256),
            }),
            wnext: 2,
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        tt.p.BecomeProbe();

        t.equal(
            tt.p.state,
            ProgressStateType.StateProbe,
            "#" + i + ": state = " + tt.p.state + ", want " + ProgressStateType.StateProbe
        );
        t.equal(tt.p.match, match, "#" + i + ": match = " + tt.p.match + ", want " + match);
        t.equal(tt.p.next, tt.wnext, "#" + i + ": next = " + tt.p.next + ", want " + tt.wnext);
    }

    t.end();
});

tap.test("tracker/progress BecomeReplicate()", (t) => {
    const p = new Progress({
        state: ProgressStateType.StateProbe,
        match: 1,
        next: 5,
        inflights: NewInflights(256),
    });
    p.BecomeReplicate();

    t.equal(
        p.state,
        ProgressStateType.StateReplicate,
        "state = " + p.state + ", want " + ProgressStateType.StateReplicate
    );
    t.equal(p.match, 1, "match = " + p.match + ", want " + 1);

    const w = p.match + 1;
    t.equal(p.next, w, "next = " + p.next + ", want " + w);

    t.end();
});

tap.test("tracker/progress becomeSnapshot()", (t) => {
    const p = new Progress({
        state: ProgressStateType.StateProbe,
        match: 1,
        next: 5,
        inflights: NewInflights(256),
    });
    p.BecomeSnapshot(10);

    t.equal(
        p.state,
        ProgressStateType.StateSnapshot,
        "state = " + p.state + ", want " + ProgressStateType.StateSnapshot
    );
    t.equal(p.match, 1, "match = " + p.match + ", want " + 1);
    t.equal(p.pendingSnapshot, 10, "pendingSnapshot = " + p.pendingSnapshot + ", want " + 10);

    t.end();
});

tap.test("tracker/progress update()", (t) => {
    const prevM = 3;
    const prevN = 5;
    const tests: { update: number; wm: number; wn: number; wok: boolean }[] = [
        { update: prevM - 1, wm: prevM, wn: prevN, wok: false }, // do no decrease match, next
        { update: prevM, wm: prevM, wn: prevN, wok: false }, // do not decrease net
        { update: prevM + 1, wm: prevM + 1, wn: prevN, wok: true }, // increase match, do not decrease next
        { update: prevM + 2, wm: prevM + 2, wn: prevN + 1, wok: true }, // increase match, next
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const p = new Progress({ match: prevM, next: prevN });
        const ok = p.MaybeUpdate(tt.update);

        t.equal(ok, tt.wok, "#" + i + ": ok= " + ok + ", want " + tt.wok);
        t.equal(p.match, tt.wm, "#" + i + ": match= " + p.match + ", want " + tt.wm);
        t.equal(p.next, tt.wn, "#" + i + ": next= " + p.next + ", want " + tt.wn);
    }

    t.end();
});

tap.test("tracker/progress maybeDecr()", (t) => {
    const tests: {
        state: ProgressStateType;
        m: number;
        n: number;
        rejected: number;
        last: number;
        w: boolean;
        wn: number;
    }[] = [
            {
                // state replicate and rejected is not greater than match
                state: ProgressStateType.StateReplicate,
                m: 5,
                n: 10,
                rejected: 5,
                last: 5,
                w: false,
                wn: 10,
            },
            {
                // state replicate and rejected is not greater than match
                state: ProgressStateType.StateReplicate,
                m: 5,
                n: 10,
                rejected: 4,
                last: 4,
                w: false,
                wn: 10,
            },
            {
                // state replicate and rejected is greater than match directly
                // decrease to match+1
                state: ProgressStateType.StateReplicate,
                m: 5,
                n: 10,
                rejected: 9,
                last: 9,
                w: true,
                wn: 6,
            },
            {
                // next-1 != rejected is always false
                state: ProgressStateType.StateProbe,
                m: 0,
                n: 0,
                rejected: 0,
                last: 0,
                w: false,
                wn: 0,
            },
            {
                // next-1 != rejected is always false
                state: ProgressStateType.StateProbe,
                m: 0,
                n: 10,
                rejected: 5,
                last: 5,
                w: false,
                wn: 10,
            },
            {
                // next>1 = decremented by 1
                state: ProgressStateType.StateProbe,
                m: 0,
                n: 10,
                rejected: 9,
                last: 9,
                w: true,
                wn: 9,
            },
            {
                // next>1 = decremented by 1
                state: ProgressStateType.StateProbe,
                m: 0,
                n: 2,
                rejected: 1,
                last: 1,
                w: true,
                wn: 1,
            },
            {
                // next<=1 = reset to 1
                state: ProgressStateType.StateProbe,
                m: 0,
                n: 1,
                rejected: 0,
                last: 0,
                w: true,
                wn: 1,
            },
            {
                // decrease to min(rejected, last+1)
                state: ProgressStateType.StateProbe,
                m: 0,
                n: 10,
                rejected: 9,
                last: 2,
                w: true,
                wn: 3,
            },
            {
                // rejected < 1, reset to 1
                state: ProgressStateType.StateProbe,
                m: 0,
                n: 10,
                rejected: 9,
                last: 0,
                w: true,
                wn: 1,
            },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const p = new Progress({
            state: tt.state,
            match: tt.m,
            next: tt.n,
        });

        const g = p.MaybeDecrTo(tt.rejected, tt.last);
        t.equal(g, tt.w, "#" + i + ": maybeDecrTo= " + g + ", want " + tt.w);

        const gm = p.match;
        t.equal(gm, tt.m, "#" + i + ": match= " + gm + ", want " + tt.m);

        const gn = p.next;
        t.equal(gn, tt.wn, "#" + i + ": next= " + gn + ", want " + tt.wn);
    }

    t.end();
});

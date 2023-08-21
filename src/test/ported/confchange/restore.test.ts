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

import { Changer } from "../../../ported/confchange/confchange";
import { Restore } from "../../../ported/confchange/restore";
import { confStateEquivalent } from "../../../ported/raftpb/confstate";
import { ConfState, rid } from "../../../ported/raftpb/raft.pb";
import { MakeProgressTracker } from "../../../ported/tracker/tracker";

// Generate creates a random (valid) ConfState.
function generateRandom(): ConfState {
    const conv = (sl: number[]): rid[] => {
        // We want IDs but the incoming slice is zero-indexed, so add one to
        // each
        const out = [];
        for (const i of sl) {
            out.push((i + 1).toString());
        }
        return out;
    };

    const cs = new ConfState();
    // NB: never generate the empty ConfState, that one should be unit tested.
    const nVoters = 1 + Math.floor(Math.random() * 5);

    const nLearners = Math.floor(Math.random() * 5);
    // The number of voters that are in the outgoing config but not in the
    // incoming one. (We'll additionally retain a random number of the incoming
    // voters below).
    const nRemovedVoters = Math.floor(Math.random() * 3);

    // Voters, learners, and removed voters must not overlap. A "removed voter"
    // is one that we have in the outgoing config but not the incoming one.
    let ids = [];
    for (let i = 0; i < 2 * (nVoters + nLearners + nRemovedVoters); i++) {
        ids.push(i);
    }
    shuffle(ids);

    ids = conv(ids);

    cs.voters = ids.slice(0, nVoters);
    ids = ids.slice(nVoters, ids.length);

    if (nLearners > 0) {
        cs.learners = ids.slice(0, nLearners);
        ids = ids.slice(nLearners, ids.length);
    }

    // Roll the dice on how many of the incoming voters we decide were also
    // previously voters.
    //
    // NB: this code avoids creating non-nil empty slices (here and below).
    const nOutgoingRetainedVoters = Math.floor(Math.random() * (nVoters + 1));
    if (nOutgoingRetainedVoters > 0 || nRemovedVoters > 0) {
        cs.votersOutgoing = cs.voters.slice(0, nOutgoingRetainedVoters);
        cs.votersOutgoing.push(...ids.slice(0, nRemovedVoters));
    }
    // Only outgoing voters that are not also incoming voters can be in
    // LearnersNext (they represent demotions).
    if (nRemovedVoters > 0) {
        const nLearnersNext = Math.floor(Math.random() * (nRemovedVoters + 1));
        if (nLearnersNext > 0) {
            cs.learnersNext = ids.slice(0, nLearnersNext);
        }
    }

    cs.autoLeave = cs.votersOutgoing.length > 0 && Math.floor(Math.random() * 2) === 1;
    return cs;
}

tap.test("restore", (t) => {
    const maxCount = 1000;

    const f = (cs: ConfState): boolean => {
        const chg: Changer = new Changer(MakeProgressTracker(20), 10);
        chg.tracker = MakeProgressTracker(20);
        chg.lastIndex = 10;

        const [cfg, prs, err] = Restore(chg, cs);
        if (!t.equal(err, null, "Error from Restore: " + err + ", want: null")) {
            return false;
        }

        chg.tracker.config = cfg;
        chg.tracker.progress = prs;

        for (const sl of [cs.voters, cs.learners, cs.votersOutgoing, cs.learnersNext]) {
            if (sl) {
                sl.sort();
            }
        }

        const cs2 = chg.tracker.confState();
        // NB: cs.Equivalent does the same "sorting" dance internally, but let's
        // test it a bit here instead of relying on it.
        const equivalent12 = confStateEquivalent(cs, cs2);
        const equivalent21 = confStateEquivalent(cs2, cs);

        if (
            t.strictSame(
                cs,
                cs2,
                "Actual: " + JSON.stringify(cs) + ", want " + JSON.stringify(cs2)
            ) &&
            t.ok(equivalent12 === null, "Actual: " + equivalent12 + ", want null") &&
            t.ok(equivalent21 === null, "Actual: " + equivalent21 + ", want null")
        ) {
            return true; // success
        }

        t.fail("before: " + JSON.stringify(cs) + "\nafter " + JSON.stringify(cs2));
        return false;
    };

    const ids = (...sl: number[]): rid[] => {
        return sl.map(x => x.toString());
    };

    const testData: ConfState[] = [
        new ConfState(),
        new ConfState({ voters: ids(1, 2, 3) }),
        new ConfState({ voters: ids(1, 2, 3), learners: ids(4, 5, 6) }),
        new ConfState({
            voters: ids(1, 2, 3),
            learners: ids(5),
            votersOutgoing: ids(1, 2, 4, 6),
            learnersNext: ids(4),
        }),
    ];

    for (const cs of testData) {
        if (!f(cs)) {
            t.fail("Error occurred");
        }
    }

    for (let i = 0; i < maxCount; i++) {
        const confstate: ConfState = generateRandom();
        const success: boolean = f(confstate);
        t.ok(success, "Actual " + success + ", want true");
    }

    t.end();
});

// Fisher-Yates algorithm for permuting an array
function shuffle(array: any[]): void {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * i);
        const temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
}

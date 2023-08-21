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

import { Changer, Describe } from "../../../ported/confchange/confchange";
import { ConfChangeSingle, ConfChangeType, rid } from "../../../ported/raftpb/raft.pb";
import { progressMapToString } from "../../../ported/tracker/progress";
import { MakeProgressTracker } from "../../../ported/tracker/tracker";
import { NullableError } from "../../../ported/util";


tap.test("confchange quick_test", (t) => {
    const maxCount = 1000;

    // Log the first couple of runs to give some indication of things working as
    // intended
    const infoCount = 5;

    function runWithJoint(c: Changer, confChanges: ConfChangeSingle[]): NullableError {
        let [cfg, prs, err] = c.enterJoint(false /* autoLeave */, ...confChanges);
        if (err !== null) {
            return err;
        }
        // Also do this with autoLeave on, just to check that we'd get the same
        // result.
        const [cfgAutoLeave, prsAutoLeave, errAutoLeave] = c.enterJoint(
            true /* autoLeave */,
            ...confChanges
        );
        if (errAutoLeave !== null) {
            return errAutoLeave;
        }
        cfgAutoLeave.autoLeave = false;

        t.strictSame(cfg, cfgAutoLeave, "cfg: " + cfg + ", cfgAutoleave: " + cfgAutoLeave);
        t.strictSame(prs, prsAutoLeave, "prs: " + prs + ", prsAutoLeave: " + prsAutoLeave);

        c.tracker.config = cfg;
        c.tracker.progress = prs;
        const [cfg2, prs2, err2] = c.leaveJoint();
        if (err2 !== null) {
            return err2;
        }
        // Reset back to the main branch with autoLeave=false
        c.tracker.config = cfg;
        c.tracker.progress = prs;
        [cfg, prs, err] = c.leaveJoint();
        if (err !== null) {
            return err;
        }

        t.strictSame(cfg, cfg2, "cfg: " + cfg + ", cfg2: " + cfg2);
        t.strictSame(prs, prs2, "prs: " + prs + ", prs2: " + prs2);

        c.tracker.config = cfg;
        c.tracker.progress = prs;
        return null;
    }

    function runWithSimple(c: Changer, confChanges: ConfChangeSingle[]): NullableError {
        for (const confChange of confChanges) {
            const [cfg, prs, err] = c.simple(confChange);
            if (err !== null) {
                return err;
            }
            c.tracker.config = cfg;
            c.tracker.progress = prs;
        }
        return null;
    }

    type TestFunc = (c: Changer, confChanges: ConfChangeSingle[]) => NullableError;

    const wrapper = (
        invoke: TestFunc
    ): ((
        setup: ConfChangeSingle[],
        confChanges: ConfChangeSingle[]
    ) => [Changer | null, NullableError]) => {
        return (
            setup: ConfChangeSingle[],
            confChanges: ConfChangeSingle[]
        ): [Changer | null, NullableError] => {
            const tr = MakeProgressTracker(10);
            const c: Changer = new Changer(tr, 10);

            let err = runWithSimple(c, setup);
            if (err !== null) {
                return [null, err];
            }

            err = invoke(c, confChanges);
            return [c, err];
        };
    };

    let n = 0;
    const f1 = (setup: ConfChangeSingle[], confChanges: ConfChangeSingle[]): Changer => {
        const [c, err] = wrapper(runWithSimple)(setup, confChanges);
        if (err !== null) {
            t.fail("" + err);
        }
        if (n < infoCount) {
            console.log("initial setup:" + Describe(...setup));
            console.log("changes:" + Describe(...confChanges));
            console.log(c!.tracker.config);
            console.log(progressMapToString(c!.tracker.progress));
        }
        n++;
        return c!;
    };

    const f2 = (setup: ConfChangeSingle[], confChanges: ConfChangeSingle[]): Changer => {
        const [c, err] = wrapper(runWithJoint)(setup, confChanges);
        if (err !== null) {
            t.fail("" + err);
        }
        return c!;
    };

    for (let i = 0; i < maxCount; i++) {
        const setup = generateInitialChanges();
        const confChanges = generateConfChanges();

        const result1 = f1(setup, confChanges);
        const result2 = f2(setup, confChanges);
        t.strictSame(result1, result2, "result simple: " + result1 + ", result joint: " + result2);
    }

    t.end();
});

function generateConfChangeType(): ConfChangeType {
    switch (Math.floor(Math.random() * 4)) {
        case 0:
            return ConfChangeType.ConfChangeAddLearnerNode;
        case 1:
            return ConfChangeType.ConfChangeAddNode;
        case 2:
            return ConfChangeType.ConfChangeRemoveNode;
        case 3:
            return ConfChangeType.ConfChangeUpdateNode;
        default:
            throw new Error("Unreachable");
    }
}

function generateConfChangesHelper(
    num: () => number,
    id: () => rid,
    typ: () => ConfChangeType
): ConfChangeSingle[] {
    const confChanges: ConfChangeSingle[] = [];
    const n = num();
    for (let i = 0; i < n; i++) {
        const newCC: ConfChangeSingle = {
            type: typ(),
            nodeId: id(),
        };
        confChanges.push(newCC);
    }
    return confChanges;
}

function generateConfChanges(): ConfChangeSingle[] {
    const num = () => {
        return 1 + Math.floor(Math.random() * 9);
    };
    const id = () => {
        return (1 + num()).toString();
    };
    const typ = generateConfChangeType;
    return generateConfChangesHelper(num, id, typ);
}

function generateInitialChanges(): ConfChangeSingle[] {
    const num = () => {
        return 1 + Math.floor(Math.random() * 5);
    };
    const id = () => {
        return num().toString();
    };
    const typ = () => {
        return ConfChangeType.ConfChangeAddNode;
    };

    // NodeID one is special - it's in the initial config and will be a voter
    // always (this is to avoid uninteresting edge cases where the simple conf
    // changes can't easily make progress).
    const confChanges: ConfChangeSingle[] = [
        { type: ConfChangeType.ConfChangeAddNode, nodeId: "1" },
        ...generateConfChangesHelper(num, id, typ),
    ];
    return confChanges;
}

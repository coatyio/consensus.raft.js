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

import { newLog } from "../../ported/log";
import { getLogger } from "../../ported/logger";
import { noLimit } from "../../ported/raft";
import { Entry, Snapshot, SnapshotMetadata } from "../../ported/raftpb/raft.pb";
import { ErrCompacted, MemoryStorage } from "../../ported/storage";
import { NullableError } from "../../ported/util";

tap.test("log FindConflict()", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ index: 1, term: 1 }),
        new Entry({ index: 2, term: 2 }),
        new Entry({ index: 3, term: 3 }),
    ];
    const tests: { ents: Entry[]; wconflict: number }[] = [
        // no conflict, empty ent
        { ents: [], wconflict: 0 },
        // no conflict
        {
            ents: [
                new Entry({ index: 1, term: 1 }),
                new Entry({ index: 2, term: 2 }),
                new Entry({ index: 3, term: 3 }),
            ],
            wconflict: 0,
        },
        {
            ents: [new Entry({ index: 2, term: 2 }), new Entry({ index: 3, term: 3 })],
            wconflict: 0,
        },
        { ents: [new Entry({ index: 3, term: 3 })], wconflict: 0 },
        // no conflict, but has new entries
        {
            ents: [
                new Entry({ index: 1, term: 1 }),
                new Entry({ index: 2, term: 2 }),
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 4 }),
            ],
            wconflict: 4,
        },
        {
            ents: [
                new Entry({ index: 2, term: 2 }),
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 4 }),
            ],
            wconflict: 4,
        },
        {
            ents: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 4 }),
            ],
            wconflict: 4,
        },
        {
            ents: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 4 })],
            wconflict: 4,
        },
        // conflicts with existing entries
        {
            ents: [new Entry({ index: 1, term: 4 }), new Entry({ index: 2, term: 4 })],
            wconflict: 1,
        },
        {
            ents: [
                new Entry({ index: 2, term: 1 }),
                new Entry({ index: 3, term: 4 }),
                new Entry({ index: 4, term: 4 }),
            ],
            wconflict: 2,
        },
        {
            ents: [
                new Entry({ index: 3, term: 1 }),
                new Entry({ index: 4, term: 2 }),
                new Entry({ index: 5, term: 4 }),
                new Entry({ index: 6, term: 4 }),
            ],
            wconflict: 3,
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const raftLog = newLog(MemoryStorage.NewMemoryStorage(), getLogger());
        raftLog.append(...previousEnts);

        const gconflict = raftLog.findConflict(tt.ents);
        t.equal(
            gconflict,
            tt.wconflict,
            "#" + i + ": conflict = " + gconflict + ", want " + tt.wconflict
        );
    }

    t.end();
});

tap.test("log isUpToDate()", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ index: 1, term: 1 }),
        new Entry({ index: 2, term: 2 }),
        new Entry({ index: 3, term: 3 }),
    ];
    const raftLog = newLog(MemoryStorage.NewMemoryStorage(), getLogger());
    raftLog.append(...previousEnts);

    const tests: { lastIndex: number; term: number; wUpToDate: boolean }[] = [
        // greater term, ignore lastIndex
        { lastIndex: raftLog.lastIndex() - 1, term: 4, wUpToDate: true },
        { lastIndex: raftLog.lastIndex(), term: 4, wUpToDate: true },
        { lastIndex: raftLog.lastIndex() + 1, term: 4, wUpToDate: true },
        // smaller term, ignore lastIndex
        { lastIndex: raftLog.lastIndex() - 1, term: 2, wUpToDate: false },
        { lastIndex: raftLog.lastIndex(), term: 2, wUpToDate: false },
        { lastIndex: raftLog.lastIndex() + 1, term: 2, wUpToDate: false },
        // equal term, equal or lager lastIndex wins
        { lastIndex: raftLog.lastIndex() - 1, term: 3, wUpToDate: false },
        { lastIndex: raftLog.lastIndex(), term: 3, wUpToDate: true },
        { lastIndex: raftLog.lastIndex() + 1, term: 3, wUpToDate: true },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const gUpToDate = raftLog.isUpToDate(tt.lastIndex, tt.term);
        t.equal(
            gUpToDate,
            tt.wUpToDate,
            "#" + i + ": uptodate = " + gUpToDate + ", want " + tt.wUpToDate
        );
    }

    t.end();
});

tap.test("log Append()", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ index: 1, term: 1 }),
        new Entry({ index: 2, term: 2 }),
    ];
    const tests: {
        ents: Entry[];
        windex: number;
        wents: Entry[];
        wunstable: number;
    }[] = [
            {
                ents: [],
                windex: 2,
                wents: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
                wunstable: 3,
            },
            {
                ents: [new Entry({ index: 3, term: 2 })],
                windex: 3,
                wents: [
                    new Entry({ index: 1, term: 1 }),
                    new Entry({ index: 2, term: 2 }),
                    new Entry({ index: 3, term: 2 }),
                ],
                wunstable: 3,
            },
            // conflicts with index 1
            {
                ents: [new Entry({ index: 1, term: 2 })],
                windex: 1,
                wents: [new Entry({ index: 1, term: 2 })],
                wunstable: 1,
            },
            // conflicts with index 2
            {
                ents: [new Entry({ index: 2, term: 3 }), new Entry({ index: 3, term: 3 })],
                windex: 3,
                wents: [
                    new Entry({ index: 1, term: 1 }),
                    new Entry({ index: 2, term: 3 }),
                    new Entry({ index: 3, term: 3 }),
                ],
                wunstable: 2,
            },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const storage = MemoryStorage.NewMemoryStorage();
        storage.Append(previousEnts);
        const raftLog = newLog(storage, getLogger());

        const index = raftLog.append(...tt.ents);

        t.equal(index, tt.windex, "#" + i + ": lastIndex = " + index + ", want " + tt.windex);

        const [g, err] = raftLog.entries(1, noLimit);

        t.equal(err, null, "#" + i + ": unexpected error " + err);
        t.strictSame(g, tt.wents, "#" + i + ": logEnts = " + g + ", want " + tt.wents);

        const goff = raftLog.unstable.offset;
        t.equal(goff, tt.wunstable, "#" + i + ": unstable = " + goff + ", want " + tt.wunstable);
    }

    t.end();
});

// TestLogMaybeAppend ensures: If the given (index, term) matches with the
// existing log:
//  1. If an existing entry conflicts with a new one (same index but different
//     terms), delete the existing entry and all that follow it 2.Append any new
//     entries not already in the log If the given (index, term) does not match
//     with the existing log: return false
tap.test("log MaybeAppend()", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ index: 1, term: 1 }),
        new Entry({ index: 2, term: 2 }),
        new Entry({ index: 3, term: 3 }),
    ];
    const lastindex = 3;
    const lastterm = 3;
    const commit = 1;
    const tests: {
        logterm: number;
        index: number;
        committed: number;
        ents: Entry[];
        wlasti: number;
        wappend: boolean;
        wcommit: number;
        wpanic: boolean;
    }[] = [
            // not match: term is different
            {
                logterm: lastterm - 1,
                index: lastindex,
                committed: lastindex,
                ents: [new Entry({ index: lastindex + 1, term: 4 })],
                wlasti: 0,
                wappend: false,
                wcommit: commit,
                wpanic: false,
            },
            // not match: index out of bound
            {
                logterm: lastterm,
                index: lastindex + 1,
                committed: lastindex,
                ents: [new Entry({ index: lastindex + 2, term: 4 })],
                wlasti: 0,
                wappend: false,
                wcommit: commit,
                wpanic: false,
            },
            // match with the last existing entry
            {
                logterm: lastterm,
                index: lastindex,
                committed: lastindex,
                ents: [],
                wlasti: lastindex,
                wappend: true,
                wcommit: lastindex,
                wpanic: false,
            },
            {
                logterm: lastterm,
                index: lastindex,
                committed: lastindex + 1,
                ents: [],
                wlasti: lastindex,
                wappend: true,
                wcommit: lastindex,
                wpanic: false, // do not increase commit higher than lastnewi
            },
            {
                logterm: lastterm,
                index: lastindex,
                committed: lastindex - 1,
                ents: [],
                wlasti: lastindex,
                wappend: true,
                wcommit: lastindex - 1,
                wpanic: false, // commit up to the commit in the message
            },
            {
                logterm: lastterm,
                index: lastindex,
                committed: 0,
                ents: [],
                wlasti: lastindex,
                wappend: true,
                wcommit: commit,
                wpanic: false, // commit do not decrease
            },
            {
                logterm: 0,
                index: 0,
                committed: lastindex,
                ents: [],
                wlasti: 0,
                wappend: true,
                wcommit: commit,
                wpanic: false, // commit do not decrease
            },
            {
                logterm: lastterm,
                index: lastindex,
                committed: lastindex,
                ents: [new Entry({ index: lastindex + 1, term: 4 })],
                wlasti: lastindex + 1,
                wappend: true,
                wcommit: lastindex,
                wpanic: false,
            },
            {
                logterm: lastterm,
                index: lastindex,
                committed: lastindex + 1,
                ents: [new Entry({ index: lastindex + 1, term: 4 })],
                wlasti: lastindex + 1,
                wappend: true,
                wcommit: lastindex + 1,
                wpanic: false,
            },
            {
                logterm: lastterm,
                index: lastindex,
                committed: lastindex + 2,
                ents: [new Entry({ index: lastindex + 1, term: 4 })],
                wlasti: lastindex + 1,
                wappend: true,
                wcommit: lastindex + 1,
                wpanic: false, // do not increase commit higher than lastnewi
            },
            {
                logterm: lastterm,
                index: lastindex,
                committed: lastindex + 2,
                ents: [
                    new Entry({ index: lastindex + 1, term: 4 }),
                    new Entry({ index: lastindex + 2, term: 4 }),
                ],
                wlasti: lastindex + 2,
                wappend: true,
                wcommit: lastindex + 2,
                wpanic: false,
            },
            // match with the the entry in the middle
            {
                logterm: lastterm - 1,
                index: lastindex - 1,
                committed: lastindex,
                ents: [new Entry({ index: lastindex, term: 4 })],
                wlasti: lastindex,
                wappend: true,
                wcommit: lastindex,
                wpanic: false,
            },
            {
                logterm: lastterm - 2,
                index: lastindex - 2,
                committed: lastindex,
                ents: [new Entry({ index: lastindex - 1, term: 4 })],
                wlasti: lastindex - 1,
                wappend: true,
                wcommit: lastindex - 1,
                wpanic: false,
            },
            {
                logterm: lastterm - 3,
                index: lastindex - 3,
                committed: lastindex,
                ents: [new Entry({ index: lastindex - 2, term: 4 })],
                wlasti: lastindex - 2,
                wappend: true,
                wcommit: lastindex - 2,
                wpanic: true, // conflict with existing committed entry
            },
            {
                logterm: lastterm - 2,
                index: lastindex - 2,
                committed: lastindex,
                ents: [
                    new Entry({ index: lastindex - 1, term: 4 }),
                    new Entry({ index: lastindex, term: 4 }),
                ],
                wlasti: lastindex,
                wappend: true,
                wcommit: lastindex,
                wpanic: false,
            },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const raftLog = newLog(MemoryStorage.NewMemoryStorage(), getLogger());
        raftLog.append(...previousEnts);
        raftLog.committed = commit;

        try {
            const [glasti, gappend] = raftLog.maybeAppend(
                tt.index,
                tt.logterm,
                tt.committed,
                ...tt.ents
            );
            const gcommit = raftLog.committed;

            t.equal(
                glasti,
                tt.wlasti,
                "#" + i + ": lastindex = " + glasti + ", want + " + tt.wlasti
            );
            t.equal(
                gappend,
                tt.wappend,
                "#" + i + ": append = " + gappend + ", want " + tt.wappend
            );
            t.equal(
                gcommit,
                tt.wcommit,
                "#" + i + ": committed = " + gcommit + ", want " + tt.wcommit
            );

            if (gappend && tt.ents.length !== 0) {
                const [gents, err] = raftLog.slice(
                    raftLog.lastIndex() - tt.ents.length + 1,
                    raftLog.lastIndex() + 1,
                    noLimit
                );
                t.equal(err, null, "assert that error is null");
                t.strictSame(
                    tt.ents,
                    gents,
                    "#" + i + ": appended entries = " + gents + ", want " + tt.ents
                );
            }
        } catch (error) {
            t.ok(tt.wpanic, i + ": panic = " + true + ", want " + tt.wpanic);
        }
    }

    t.end();
});

// TestCompactionSideEffects ensures that all the log related functionality
// works correctly after a compaction.
tap.test("log CompactionSideEffects()", (t) => {
    let i: number;

    // Populate the log with 1000 entries; 750 in stable storage and 250 in
    // unstable.
    const lastIndex = 1000;
    const unstableIndex = 750;
    const lastTerm = lastIndex;
    const storage = MemoryStorage.NewMemoryStorage();

    for (i = 1; i <= unstableIndex; i++) {
        storage.Append([new Entry({ term: i, index: i })]);
    }
    const raftLog = newLog(storage, getLogger());
    for (i = unstableIndex; i < lastIndex; i++) {
        raftLog.append(new Entry({ term: i + 1, index: i + 1 }));
    }

    const ok = raftLog.maybeCommit(lastIndex, lastTerm);

    t.ok(ok, "assert maybeCommit returns true");

    raftLog.appliedTo(raftLog.committed);

    const offset = 500;
    storage.Compact(offset);

    t.equal(
        raftLog.lastIndex(),
        lastIndex,
        "lastIndex = " + raftLog.lastIndex() + ", want " + lastIndex
    );

    for (let j = offset; j <= raftLog.lastIndex(); j++) {
        t.equal(
            mustTerm(...raftLog.term(j)),
            j,
            "term(" + j + ") = " + mustTerm(...raftLog.term(j)) + ", want " + j
        );
    }

    for (let j = offset; j <= raftLog.lastIndex(); j++) {
        const matchesTerm = raftLog.matchTerm(j, j);
        t.ok(matchesTerm, "matchTerm(" + j + ") = " + matchesTerm + ", want true");
    }

    const unstableEnts = raftLog.unstableEntries();
    const g = unstableEnts.length;

    t.equal(g, 250, "unstableEntries.length = " + g + ", want = " + 250);
    t.equal(unstableEnts[0].index, 751, "Index = " + unstableEnts[0].index + ", want = " + 751);

    const prev = raftLog.lastIndex();
    raftLog.append(
        new Entry({
            index: raftLog.lastIndex() + 1,
            term: raftLog.lastIndex() + 1,
        })
    );

    t.equal(
        raftLog.lastIndex(),
        prev + 1,
        "lastIndex = " + raftLog.lastIndex() + ", want = " + prev + 1
    );

    const [ents, err] = raftLog.entries(raftLog.lastIndex(), noLimit);

    t.equal(err, null, "error: " + err + ", want: null");
    t.equal(ents.length, 1, "entries.length = " + ents.length + ", want = " + 1);

    t.end();
});

tap.test("log HasNextEnts()", (t) => {
    const snap = new Snapshot({
        metadata: new SnapshotMetadata({ term: 1, index: 3 }),
    });
    const ents: Entry[] = [
        new Entry({ term: 1, index: 4 }),
        new Entry({ term: 1, index: 5 }),
        new Entry({ term: 1, index: 6 }),
    ];
    const tests: { applied: number; hasNext: boolean }[] = [
        { applied: 0, hasNext: true },
        { applied: 3, hasNext: true },
        { applied: 4, hasNext: true },
        { applied: 5, hasNext: false },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const storage = MemoryStorage.NewMemoryStorage();
        storage.ApplySnapshot(snap);
        const raftLog = newLog(storage, getLogger());
        raftLog.append(...ents);
        raftLog.maybeCommit(5, 1);
        raftLog.appliedTo(tt.applied);

        const hasNext = raftLog.hasNextEnts();
        t.equal(hasNext, tt.hasNext, "#" + i + ": hasNext = " + hasNext + ", want " + tt.hasNext);
    }

    t.end();
});

tap.test("log NextEnts()", (t) => {
    const snap = new Snapshot({
        metadata: new SnapshotMetadata({ term: 1, index: 3 }),
    });
    const ents: Entry[] = [
        new Entry({ term: 1, index: 4 }),
        new Entry({ term: 1, index: 5 }),
        new Entry({ term: 1, index: 6 }),
    ];
    const tests: { applied: number; wents: Entry[] }[] = [
        { applied: 0, wents: ents.slice(0, 2) },
        { applied: 3, wents: ents.slice(0, 2) },
        { applied: 4, wents: ents.slice(1, 2) },
        { applied: 5, wents: [] },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const storage = MemoryStorage.NewMemoryStorage();
        storage.ApplySnapshot(snap);
        const raftLog = newLog(storage, getLogger());
        raftLog.append(...ents);
        raftLog.maybeCommit(5, 1);
        raftLog.appliedTo(tt.applied);

        const nents = raftLog.nextEnts();

        t.strictSame(nents, tt.wents, "#" + i + ": nents = " + nents + ", want " + tt.wents);
    }

    t.end();
});

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
tap.test("log UnstableEnts()", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 2, index: 2 }),
    ];
    const tests: { unstable: number; wents: Entry[] }[] = [
        { unstable: 3, wents: [] },
        { unstable: 1, wents: previousEnts },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        // append stable entries to storage
        const storage = MemoryStorage.NewMemoryStorage();
        storage.Append(previousEnts.slice(0, tt.unstable - 1));

        // append unstable entries to raftlog
        const raftLog = newLog(storage, getLogger());
        raftLog.append(...previousEnts.slice(tt.unstable - 1));

        const ents = raftLog.unstableEntries();
        const l = ents.length;
        if (l > 0) {
            raftLog.stableTo(ents[l - 1].index, ents[l - 1].term);
        }

        t.strictSame(
            ents,
            tt.wents,
            "#" + i + ": unstableEnts = " + ents + ", want " + tt.wents
        );

        const w = previousEnts[previousEnts.length - 1].index + 1;
        const g = raftLog.unstable.offset;

        t.equal(g, w, "#" + i + ": unstable = " + g + ", want " + w);
    }

    t.end();
});

tap.test("log CommitTo()", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 2, index: 2 }),
        new Entry({ term: 3, index: 3 }),
    ];
    const commit = 2;
    const tests: { commit: number; wcommit: number; wpanic: boolean }[] = [
        { commit: 3, wcommit: 3, wpanic: false },
        { commit: 1, wcommit: 2, wpanic: false }, // never decrease
        { commit: 4, wcommit: 0, wpanic: true }, // commit out of range -> panic
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        try {
            const raftLog = newLog(MemoryStorage.NewMemoryStorage(), getLogger());
            raftLog.append(...previousEnts);
            raftLog.committed = commit;
            raftLog.commitTo(tt.commit);

            t.equal(
                raftLog.committed,
                tt.wcommit,
                "#" + i + ": committed = " + raftLog.committed + ", want " + tt.wcommit
            );
        } catch (error) {
            t.ok(tt.wpanic, i + ": panic = " + true + ", want " + tt.wpanic);
        }
    }

    t.end();
});

tap.test("log StableTo()", (t) => {
    const tests: { stablei: number; stablet: number; wunstable: number }[] = [
        { stablei: 1, stablet: 1, wunstable: 2 },
        { stablei: 2, stablet: 2, wunstable: 3 },
        { stablei: 2, stablet: 1, wunstable: 1 }, // bad term
        { stablei: 3, stablet: 1, wunstable: 1 }, // bad index
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const raftLog = newLog(MemoryStorage.NewMemoryStorage(), getLogger());
        raftLog.append(...[new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })]);
        raftLog.stableTo(tt.stablei, tt.stablet);

        t.equal(
            raftLog.unstable.offset,
            tt.wunstable,
            "#" + i + ": unstable = " + raftLog.unstable.offset + ", want " + tt.wunstable
        );
    }

    t.end();
});

tap.test("log StableToWithSnap()", (t) => {
    const snapi = 5;
    const snapt = 2;
    const tests: {
        stablei: number;
        stablet: number;
        newEnts: Entry[];
        wunstable: number;
    }[] = [
            {
                stablei: snapi + 1,
                stablet: snapt,
                newEnts: [],
                wunstable: snapi + 1,
            },
            { stablei: snapi, stablet: snapt, newEnts: [], wunstable: snapi + 1 },
            {
                stablei: snapi - 1,
                stablet: snapt,
                newEnts: [],
                wunstable: snapi + 1,
            },

            {
                stablei: snapi + 1,
                stablet: snapt + 1,
                newEnts: [],
                wunstable: snapi + 1,
            },
            {
                stablei: snapi,
                stablet: snapt + 1,
                newEnts: [],
                wunstable: snapi + 1,
            },
            {
                stablei: snapi - 1,
                stablet: snapt + 1,
                newEnts: [],
                wunstable: snapi + 1,
            },
            {
                stablei: snapi + 1,
                stablet: snapt,
                newEnts: [new Entry({ index: snapi + 1, term: snapt })],
                wunstable: snapi + 2,
            },
            {
                stablei: snapi,
                stablet: snapt,
                newEnts: [new Entry({ index: snapi + 1, term: snapt })],
                wunstable: snapi + 1,
            },
            {
                stablei: snapi - 1,
                stablet: snapt,
                newEnts: [new Entry({ index: snapi + 1, term: snapt })],
                wunstable: snapi + 1,
            },

            {
                stablei: snapi + 1,
                stablet: snapt + 1,
                newEnts: [new Entry({ index: snapi + 1, term: snapt })],
                wunstable: snapi + 1,
            },
            {
                stablei: snapi,
                stablet: snapt + 1,
                newEnts: [new Entry({ index: snapi + 1, term: snapt })],
                wunstable: snapi + 1,
            },
            {
                stablei: snapi - 1,
                stablet: snapt + 1,
                newEnts: [new Entry({ index: snapi + 1, term: snapt })],
                wunstable: snapi + 1,
            },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const s = MemoryStorage.NewMemoryStorage();
        const snap = new Snapshot({
            metadata: new SnapshotMetadata({ index: snapi, term: snapt }),
        });
        s.ApplySnapshot(snap);
        const raftLog = newLog(s, getLogger());
        raftLog.append(...tt.newEnts);
        raftLog.stableTo(tt.stablei, tt.stablet);

        t.equal(
            raftLog.unstable.offset,
            tt.wunstable,
            "#" + i + ": unstable = " + raftLog.unstable.offset + ", want " + tt.wunstable
        );
    }

    t.end();
});

// TestCompaction ensures that the number of log entries is correct after
// compactions.
tap.test("log Compaction()", (t) => {
    const tests: {
        lastindex: number;
        compact: number[];
        wleft: number[];
        wallow: boolean;
    }[] = [
            // out of upper bound
            { lastindex: 1000, compact: [1001], wleft: [-1], wallow: false },
            {
                lastindex: 1000,
                compact: [300, 500, 800, 900],
                wleft: [700, 500, 200, 100],
                wallow: true,
            },
            // out of lower bound
            {
                lastindex: 1000,
                compact: [300, 299],
                wleft: [700, -1],
                wallow: false,
            },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        try {
            const storage = MemoryStorage.NewMemoryStorage();
            for (let j = 1; j <= tt.lastindex; j++) {
                storage.Append([new Entry({ index: j })]);
            }

            const raftLog = newLog(storage, getLogger());
            raftLog.maybeCommit(tt.lastindex, 0);
            raftLog.appliedTo(raftLog.committed);

            for (let j = 0; j < tt.compact.length; j++) {
                const err = storage.Compact(tt.compact[j]);

                if (err !== null) {
                    t.notOk(
                        tt.wallow,
                        "#" + i + "." + j + " allow = " + false + ", want " + tt.wallow
                    );
                    continue;
                }

                t.equal(
                    raftLog.allEntries().length,
                    tt.wleft[j],
                    "#" +
                    i +
                    "." +
                    j +
                    " len = " +
                    raftLog.allEntries().length +
                    ", want " +
                    tt.wleft[j]
                );
            }
        } catch (error) {
            t.notOk(tt.wallow, i + ": allow = " + false + ", want " + tt.wallow + ": " + error);
        }
    }

    t.end();
});

tap.test("log Restore()", (t) => {
    const index = 1000;
    const term = 1000;
    const snap = new SnapshotMetadata({ index: index, term: term });
    const storage = MemoryStorage.NewMemoryStorage();
    storage.ApplySnapshot(new Snapshot({ metadata: snap }));
    const raftLog = newLog(storage, getLogger());

    t.equal(raftLog.allEntries().length, 0, "length = " + raftLog.allEntries().length + ", want 0");
    t.equal(
        raftLog.firstIndex(),
        index + 1,
        "firstIndex = " + raftLog.firstIndex() + ", want " + index + 1
    );
    t.equal(raftLog.committed, index, "committed = " + raftLog.committed + ", want " + index);
    t.equal(
        raftLog.unstable.offset,
        index + 1,
        "unstable = " + raftLog.unstable.offset + ", want " + index + 1
    );
    t.equal(
        mustTerm(...raftLog.term(index)),
        term,
        "term = " + mustTerm(...raftLog.term(index)) + ", want " + term
    );

    t.end();
});

tap.test("log IsOutOfBounds()", (t) => {
    const offset = 100;
    const num = 100;
    const storage = MemoryStorage.NewMemoryStorage();
    const snap = new Snapshot({
        metadata: new SnapshotMetadata({ index: offset }),
    });
    storage.ApplySnapshot(snap);
    const l = newLog(storage, getLogger());

    for (let i = 1; i <= num; i++) {
        l.append(new Entry({ index: i + offset }));
    }

    const first = offset + 1;
    const tests: {
        lo: number;
        hi: number;
        wpanic: boolean;
        wErrCompacted: boolean;
    }[] = [
            {
                lo: first - 2,
                hi: first + 1,
                wpanic: false,
                wErrCompacted: true,
            },
            {
                lo: first - 1,
                hi: first + 1,
                wpanic: false,
                wErrCompacted: true,
            },
            {
                lo: first,
                hi: first,
                wpanic: false,
                wErrCompacted: false,
            },
            {
                lo: first + Math.floor(num / 2),
                hi: first + Math.floor(num / 2),
                wpanic: false,
                wErrCompacted: false,
            },
            {
                lo: first + num - 1,
                hi: first + num - 1,
                wpanic: false,
                wErrCompacted: false,
            },
            {
                lo: first + num,
                hi: first + num,
                wpanic: false,
                wErrCompacted: false,
            },
            {
                lo: first + num,
                hi: first + num + 1,
                wpanic: true,
                wErrCompacted: false,
            },
            {
                lo: first + num + 1,
                hi: first + num + 1,
                wpanic: true,
                wErrCompacted: false,
            },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        try {
            const err = l.mustCheckOutOfBounds(tt.lo, tt.hi);

            t.notOk(tt.wpanic, "#" + i + ": panic = " + false + ", want " + false);
            t.notOk(
                tt.wErrCompacted && !(err instanceof ErrCompacted),
                "#" + i + ": err = " + err + ", want " + ErrCompacted
            );
            t.notOk(
                !tt.wErrCompacted && err !== null,
                "#" + i + "unexpected err " + err + ", want null"
            );
        } catch (error) {
            t.ok(tt.wpanic, i + ": panic = " + true + ", want " + tt.wpanic + ": " + error);
        }
    }

    t.end();
});

tap.test("log Term()", (t) => {
    let i: number;
    const offset = 100;
    const num = 100;

    const storage = MemoryStorage.NewMemoryStorage();
    const snap = new Snapshot({
        metadata: new SnapshotMetadata({ index: offset, term: 1 }),
    });
    storage.ApplySnapshot(snap);

    const l = newLog(storage, getLogger());
    for (i = 1; i < num; i++) {
        l.append(new Entry({ index: offset + i, term: i }));
    }

    const tests: { index: number; w: number }[] = [
        { index: offset - 1, w: 0 },
        { index: offset, w: 1 },
        { index: offset + Math.floor(num / 2), w: Math.floor(num / 2) },
        { index: offset + num - 1, w: num - 1 },
        { index: offset + num, w: 0 },
    ];

    for (let j = 0; j < tests.length; j++) {
        const tt = tests[j];

        const term = mustTerm(...l.term(tt.index));
        t.equal(term, tt.w, "#" + j + ": at = " + term + ", want " + tt.w);
    }

    t.end();
});

tap.test("log TermWithUnstableSnapshot()", (t) => {
    const storagesnapi = 100;
    const unstablesnapi = storagesnapi + 5;

    const storage = MemoryStorage.NewMemoryStorage();
    const snap = new Snapshot({
        metadata: new SnapshotMetadata({ index: storagesnapi, term: 1 }),
    });
    storage.ApplySnapshot(snap);
    const l = newLog(storage, getLogger());
    l.restore(
        new Snapshot({
            metadata: new SnapshotMetadata({ index: unstablesnapi, term: 1 }),
        })
    );

    const tests: { index: number; w: number }[] = [
        // cannot get term from storage
        { index: storagesnapi, w: 0 },
        // cannot get term from the gap between storage ents and unstable
        // snapshot
        { index: storagesnapi + 1, w: 0 },
        { index: unstablesnapi - 1, w: 0 },
        // get term from unstable snapshot index
        { index: unstablesnapi, w: 1 },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const term = mustTerm(...l.term(tt.index));
        t.equal(term, tt.w, "#" + i + ": at = " + term + ", want " + tt.w);
    }

    t.end();
});

tap.test("log Slice()", (t) => {
    let i: number;
    const offset = 100;
    const num = 100;
    const last = offset + num;
    const half = Math.floor(offset + num / 2);
    const halfe: Entry = new Entry({ index: half, term: half });
    const halfeSize = JSON.stringify(halfe).length;

    const storage = MemoryStorage.NewMemoryStorage();
    const snap = new Snapshot({
        metadata: new SnapshotMetadata({ index: offset }),
    });
    storage.ApplySnapshot(snap);

    for (i = 1; i < Math.floor(num / 2); i++) {
        storage.Append([new Entry({ index: offset + i, term: offset + i })]);
    }

    const l = newLog(storage, getLogger());
    for (i = Math.floor(num / 2); i < num; i++) {
        l.append(new Entry({ index: offset + i, term: offset + i }));
    }

    const tests: {
        from: number;
        to: number;
        limit: number;

        w: Entry[];
        wpanic: boolean;
    }[] = [
            // test no limit
            {
                from: offset - 1,
                to: offset + 1,
                limit: noLimit,
                w: [],
                wpanic: false,
            },
            {
                from: offset,
                to: offset + 1,
                limit: noLimit,
                w: [],
                wpanic: false,
            },
            {
                from: half - 1,
                to: half + 1,
                limit: noLimit,
                w: [
                    new Entry({ index: half - 1, term: half - 1 }),
                    new Entry({ index: half, term: half }),
                ],
                wpanic: false,
            },
            {
                from: half,
                to: half + 1,
                limit: noLimit,
                w: [new Entry({ index: half, term: half })],
                wpanic: false,
            },
            {
                from: last - 1,
                to: last,
                limit: noLimit,
                w: [new Entry({ index: last - 1, term: last - 1 })],
                wpanic: false,
            },
            {
                from: last,
                to: last + 1,
                limit: noLimit,
                w: [],
                wpanic: true,
            },

            // test limit
            {
                from: half - 1,
                to: half + 1,
                limit: 0,
                w: [new Entry({ index: half - 1, term: half - 1 })],
                wpanic: false,
            },
            {
                from: half - 1,
                to: half + 1,
                limit: halfeSize + 1,
                w: [new Entry({ index: half - 1, term: half - 1 })],
                wpanic: false,
            },
            {
                from: half - 2,
                to: half + 1,
                limit: halfeSize + 1,
                w: [new Entry({ index: half - 2, term: half - 2 })],
                wpanic: false,
            },
            {
                from: half - 1,
                to: half + 1,
                limit: halfeSize * 2,
                w: [
                    new Entry({ index: half - 1, term: half - 1 }),
                    new Entry({ index: half, term: half }),
                ],
                wpanic: false,
            },
            {
                from: half - 1,
                to: half + 2,
                limit: halfeSize * 3,
                w: [
                    new Entry({ index: half - 1, term: half - 1 }),
                    new Entry({ index: half, term: half }),
                    new Entry({ index: half + 1, term: half + 1 }),
                ],
                wpanic: false,
            },
            {
                from: half,
                to: half + 2,
                limit: halfeSize,
                w: [new Entry({ index: half, term: half })],
                wpanic: false,
            },
            {
                from: half,
                to: half + 2,
                limit: halfeSize * 2,
                w: [
                    new Entry({ index: half, term: half }),
                    new Entry({ index: half + 1, term: half + 1 }),
                ],
                wpanic: false,
            },
        ];

    for (let j = 0; j < tests.length; j++) {
        const tt = tests[j];

        try {
            const [g, err] = l.slice(tt.from, tt.to, tt.limit);

            t.notOk(
                tt.from <= offset && !(err instanceof ErrCompacted),
                "#" + j + ": err = " + err + ", want " + ErrCompacted
            );
            t.notOk(tt.from > offset && err !== null, "#" + j + ": error " + err + ", want: null");
            t.strictSame(
                g,
                tt.w,
                "#" + j + ": from " + tt.from + " to " + tt.to + " = " + g + ", want " + tt.w
            );
        } catch (error) {
            t.ok(tt.wpanic, j + ": panic = " + true + ", want: " + tt.wpanic + " " + error);
        }
    }

    t.end();
});

export function mustTerm(term: number, err: NullableError): number {
    if (err !== null) {
        throw err;
    }
    return term;
}

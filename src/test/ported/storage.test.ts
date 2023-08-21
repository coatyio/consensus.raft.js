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

import { RaftData } from "../../non-ported/raft-controller";
import { noLimit } from "../../ported/raft";
import { ConfState, Entry, Snapshot, SnapshotMetadata } from "../../ported/raftpb/raft.pb";
import { ErrCompacted, ErrSnapOutOfDate, ErrUnavailable, MemoryStorage } from "../../ported/storage";
import { NullableError } from "../../ported/util";

tap.test("storage Term()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 3, term: 3 }),
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
    ];

    const tests: { i: number; werr: NullableError; wterm: number; wpanic: boolean }[] = [
        { i: 2, werr: new ErrCompacted(), wterm: 0, wpanic: false },
        { i: 3, werr: null, wterm: 3, wpanic: false },
        { i: 4, werr: null, wterm: 4, wpanic: false },
        { i: 5, werr: null, wterm: 5, wpanic: false },
        { i: 6, werr: new ErrUnavailable(), wterm: 0, wpanic: false },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const s = new MemoryStorage({ ents: ents });

        try {
            const [term, err] = s.Term(tt.i);
            t.strictSame(err, tt.werr, "#" + i + ": check returned error");
            t.equal(term, tt.wterm, "#" + i + ": check returned term");
        } catch (error) {
            t.notOk(tt.wpanic, "#" + i + ": check if error is thrown");
        }
    }

    t.end();
});

tap.test("storage Entries()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 3, term: 3 }),
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
        new Entry({ index: 6, term: 6 }),
    ];
    const entSize = JSON.stringify(ents[0]).length;

    const tests: {
        lo: number;
        hi: number;
        maxsize: number;
        werr: NullableError;
        wentries: Entry[];
    }[] = [
            {
                lo: 2,
                hi: 6,
                maxsize: noLimit,
                werr: new ErrCompacted(),
                wentries: [],
            },
            {
                lo: 3,
                hi: 4,
                maxsize: noLimit,
                werr: new ErrCompacted(),
                wentries: [],
            },
            {
                lo: 4,
                hi: 5,
                maxsize: noLimit,
                werr: null,
                wentries: [new Entry({ index: 4, term: 4 })],
            },
            {
                lo: 4,
                hi: 6,
                maxsize: noLimit,
                werr: null,
                wentries: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 5 })],
            },
            // tslint:disable-next-line: max-line-length
            {
                lo: 4,
                hi: 7,
                maxsize: noLimit,
                werr: null,
                wentries: [
                    new Entry({ index: 4, term: 4 }),
                    new Entry({ index: 5, term: 5 }),
                    new Entry({ index: 6, term: 6 }),
                ],
            },
            // even if maxsize is zero, the first entry should be returned
            {
                lo: 4,
                hi: 7,
                maxsize: 0,
                werr: null,
                wentries: [new Entry({ index: 4, term: 4 })],
            },
            // limit to 2
            {
                lo: 4,
                hi: 7,
                maxsize: 2 * entSize,
                werr: null,
                wentries: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 5 })],
            },
            // limit to 2
            {
                lo: 4,
                hi: 7,
                maxsize: 2 * entSize + Math.floor(entSize / 2),
                werr: null,
                wentries: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 5 })],
            },
            // tslint:disable-next-line: max-line-length
            {
                lo: 4,
                hi: 7,
                maxsize: 3 * entSize - 1,
                werr: null,
                wentries: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 5 })],
            },
            // all tslint:disable-next-line: max-line-length
            {
                lo: 4,
                hi: 7,
                maxsize: 3 * entSize,
                werr: null,
                wentries: [
                    new Entry({ index: 4, term: 4 }),
                    new Entry({ index: 5, term: 5 }),
                    new Entry({ index: 6, term: 6 }),
                ],
            },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];
        const s = new MemoryStorage({ ents: ents });

        const [entries, err] = s.Entries(tt.lo, tt.hi, tt.maxsize);
        t.strictSame(err, tt.werr, "#" + i + ": check returned error");
        t.strictSame(entries, tt.wentries, "#" + i + ": check returned entries");
    }

    t.end();
});

tap.test("storage LastIndex()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 3, term: 3 }),
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
    ];
    const s: MemoryStorage = new MemoryStorage({ ents: ents });

    let [last, err] = s.LastIndex();
    t.equal(err, null, "check returned error");
    t.equal(last, 5, "check returned lastindex");

    s.Append([new Entry({ index: 6, term: 5 })]);
    [last, err] = s.LastIndex();
    t.equal(err, null, "check returned error");
    t.equal(last, 6, "check returned lastindex");

    t.end();
});

tap.test("storage FirstIndex()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 3, term: 3 }),
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
    ];
    const s = new MemoryStorage({ ents: ents });

    let [first, err] = s.FirstIndex();
    t.equal(err, null, "check returned error");
    t.equal(first, 4, "check returned firstindex");

    s.Compact(4);
    [first, err] = s.FirstIndex();
    t.equal(err, null, "check returned error");
    t.equal(first, 5, "check returned firstindex");

    t.end();
});

tap.test("storage Compact()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 3, term: 3 }),
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
    ];
    const tests: {
        i: number;
        werr: NullableError;
        windex: number;
        wterm: number;
        wlen: number;
    }[] = [
            { i: 2, werr: new ErrCompacted(), windex: 3, wterm: 3, wlen: 3 },
            { i: 3, werr: new ErrCompacted(), windex: 3, wterm: 3, wlen: 3 },
            { i: 4, werr: null, windex: 4, wterm: 4, wlen: 2 },
            { i: 5, werr: null, windex: 5, wterm: 5, wlen: 1 },
        ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];
        const s = new MemoryStorage({ ents: ents });

        const err = s.Compact(tt.i);
        t.strictSame(err, tt.werr, "#" + i + ": check returned error");
        t.equal(s.ents[0].index, tt.windex, "#" + i + ": check index");
        t.equal(s.ents[0].term, tt.wterm, "#" + i + ": check term");
        t.equal(s.ents.length, tt.wlen, "#" + i + ": check length");
    }

    t.end();
});

tap.test("storage CreateSnapshot()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 3, term: 3 }),
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
    ];
    const cs: ConfState = new ConfState({ voters: ["1", "2", "3"] });
    const data: RaftData = "data";

    const tests: { i: number; werr: NullableError; wsnap: Snapshot }[] = [
        {
            i: 4,
            werr: null,
            wsnap: new Snapshot({
                data: data,
                metadata: new SnapshotMetadata({
                    index: 4,
                    term: 4,
                    confState: cs,
                }),
            }),
        },
        {
            i: 5,
            werr: null,
            wsnap: new Snapshot({
                data: data,
                metadata: new SnapshotMetadata({
                    index: 5,
                    term: 5,
                    confState: cs,
                }),
            }),
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];
        const s = new MemoryStorage({ ents: ents });

        const [snap, err] = s.CreateSnapshot(tt.i, cs, data);
        t.strictSame(err, tt.werr, "#" + i + ": check returned error");
        t.strictSame(snap, tt.wsnap, "#" + i + ": check returned snapshot");
    }

    t.end();
});

tap.test("storage Append()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 3, term: 3 }),
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
    ];
    const tests: { entries: Entry[]; werr: NullableError; wentries: Entry[] }[] = [
        {
            entries: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
            werr: null,
            wentries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
            ],
        },
        {
            entries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
            ],
            werr: null,
            wentries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
            ],
        },
        {
            entries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 6 }),
                new Entry({ index: 5, term: 6 }),
            ],
            werr: null,
            wentries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 6 }),
                new Entry({ index: 5, term: 6 }),
            ],
        },
        {
            entries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
                new Entry({ index: 6, term: 5 }),
            ],
            werr: null,
            wentries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
                new Entry({ index: 6, term: 5 }),
            ],
        },
        // truncate incoming entries, truncate the existing entries and append
        {
            entries: [
                new Entry({ index: 2, term: 3 }),
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 5 }),
            ],
            werr: null,
            wentries: [new Entry({ index: 3, term: 3 }), new Entry({ index: 4, term: 5 })],
        },
        // truncate the existing entries and append
        {
            entries: [new Entry({ index: 4, term: 5 })],
            werr: null,
            wentries: [new Entry({ index: 3, term: 3 }), new Entry({ index: 4, term: 5 })],
        },
        // direct append
        {
            entries: [new Entry({ index: 6, term: 5 })],
            werr: null,
            // tslint:disable-next-line: max-line-length
            wentries: [
                new Entry({ index: 3, term: 3 }),
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
                new Entry({ index: 6, term: 5 }),
            ],
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];
        const s = new MemoryStorage({ ents: ents });
        const err = s.Append(tt.entries);

        t.strictSame(err, tt.werr, "#" + i + ": check returned error");
        t.strictSame(s.ents, tt.wentries, "#" + i + ": check entries");
    }

    t.end();
});

tap.test("storage ApplySnapshot()", (t) => {
    const cs: ConfState = new ConfState({ voters: ["1", "2", "3"] });
    const data: RaftData = "data";

    const tests: Snapshot[] = [
        new Snapshot({
            data: data,
            metadata: new SnapshotMetadata({ index: 4, term: 4, confState: cs }),
        }),
        new Snapshot({
            data: data,
            metadata: new SnapshotMetadata({ index: 3, term: 3, confState: cs }),
        }),
    ];
    const s = MemoryStorage.NewMemoryStorage();

    // Apply Snapshot successful
    let i = 0;
    let tt = tests[i];
    let err = s.ApplySnapshot(tt);
    t.strictSame(err, null, "#" + i + ": check returned error");

    // Apply Snapshot fails due to ErrSnapOutOfDate
    i = 1;
    tt = tests[i];
    err = s.ApplySnapshot(tt);
    t.strictSame(err, new ErrSnapOutOfDate(), "#" + i + ": check returned error");

    t.end();
});

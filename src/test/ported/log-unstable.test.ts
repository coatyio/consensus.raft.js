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

import { Unstable } from "../../ported/log-unstable";
import { getLogger } from "../../ported/logger";
import { Entry, Snapshot, SnapshotMetadata } from "../../ported/raftpb/raft.pb";

tap.test("unstable maybeFirstIndex()", (t) => {
    const tests: {
        entries: Entry[];
        offset: number;
        snap?: Snapshot;

        wok: boolean;
        windex: number;
    }[] = [
            // no snapshot
            {
                entries: [new Entry(new Entry({ index: 5, term: 1 }))],
                offset: 5,
                snap: undefined,
                wok: false,
                windex: 0,
            },
            {
                entries: [],
                offset: 0,
                snap: undefined,
                wok: false,
                windex: 0,
            },
            // has snapshot
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                wok: true,
                windex: 5,
            },
            {
                entries: [],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                wok: true,
                windex: 5,
            },
        ];

    tests.forEach((tt, i, _) => {
        const u = new Unstable({
            entries: tt.entries,
            offset: tt.offset,
            snapshot: tt.snap,
            logger: getLogger(),
        });
        const [index, ok] = u.maybeFirstIndex();

        t.equal(ok, tt.wok, "#" + i + ": check ok");
        t.equal(index, tt.windex, "#" + i + ": check index");
    });

    t.end();
});

tap.test("unstable maybeLastIndex()", (t) => {
    const tests: {
        entries: Entry[];
        offset: number;
        snap?: Snapshot;

        wok: boolean;
        windex: number;
    }[] = [
            // last in entries
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                wok: true,
                windex: 5,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                wok: true,
                windex: 5,
            },
            // last in snapshot
            {
                entries: [],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                wok: true,
                windex: 4,
            },
            // empty unstable
            {
                entries: [],
                offset: 0,
                snap: undefined,
                wok: false,
                windex: 0,
            },
        ];

    tests.forEach((tt, i, _) => {
        const u = new Unstable({
            entries: tt.entries,
            offset: tt.offset,
            snapshot: tt.snap,
            logger: getLogger(),
        });
        const [index, ok] = u.maybeLastIndex();

        t.equal(ok, tt.wok, "#" + i + ": check ok");
        t.equal(index, tt.windex, "#" + i + ": check index");
    });

    t.end();
});

tap.test("unstable maybeTerm()", (t) => {
    const tests: {
        entries: Entry[];
        offset: number;
        snap?: Snapshot;
        index: number;

        wok: boolean;
        wterm: number;
    }[] = [
            // term from entries
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                index: 5,
                wok: true,
                wterm: 1,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                index: 6,
                wok: false,
                wterm: 0,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                index: 4,
                wok: false,
                wterm: 0,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 5,
                wok: true,
                wterm: 1,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 6,
                wok: false,
                wterm: 0,
            },
            // term from snapshot
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 4,
                wok: true,
                wterm: 1,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 3,
                wok: false,
                wterm: 0,
            },
            {
                entries: [],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 5,
                wok: false,
                wterm: 0,
            },
            {
                entries: [],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 4,
                wok: true,
                wterm: 1,
            },
            {
                entries: [],
                offset: 0,
                snap: undefined,
                index: 5,
                wok: false,
                wterm: 0,
            },
        ];

    tests.forEach((tt, i, _) => {
        const u = new Unstable({
            entries: tt.entries,
            offset: tt.offset,
            snapshot: tt.snap,
            logger: getLogger(),
        });
        const [term, ok] = u.maybeTerm(tt.index);

        t.equal(ok, tt.wok, "#" + i + ": check ok");
        t.equal(term, tt.wterm, "#" + i + ": check term");
    });

    t.end();
});

tap.test("unstable restore()", (t) => {
    const u = new Unstable({
        entries: [new Entry({ index: 5, term: 1 })],
        offset: 5,
        snapshot: new Snapshot({
            metadata: new SnapshotMetadata({ index: 4, term: 1 }),
        }),
        logger: getLogger(),
    });
    const s = new Snapshot({
        metadata: new SnapshotMetadata({ index: 6, term: 2 }),
    });
    u.restore(s);

    t.equal(u.offset, s.metadata.index + 1, "check offset");
    t.equal(u.entries.length, 0, "check entries length");
    t.strictSame(u.snapshot, s, "check snapshot");
    t.end();
});

tap.test("unstable stableTo()", (t) => {
    const tests: {
        entries: Entry[];
        offset: number;
        snap?: Snapshot;
        index: number;
        term: number;

        woffset: number;
        wlen: number;
    }[] = [
            {
                entries: [],
                offset: 0,
                snap: undefined,
                index: 5,
                term: 1,
                woffset: 0,
                wlen: 0,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                index: 5, // stable to the first entry
                term: 1, // stable to the first entry
                woffset: 6,
                wlen: 0,
            },
            {
                entries: [new Entry({ index: 5, term: 1 }), new Entry({ index: 6, term: 1 })],
                offset: 5,
                snap: undefined,
                index: 5, // stable to the first entry
                term: 1, // stable to the first entry
                woffset: 6,
                wlen: 1,
            },
            {
                entries: [new Entry({ index: 6, term: 2 })],
                offset: 6,
                snap: undefined,
                index: 6, // stable to the first entry and term mismatch
                term: 1, // stable to the first entry and term mismatch
                woffset: 6,
                wlen: 1,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                index: 4, // stable to old entry
                term: 1, // stable to old entry
                woffset: 5,
                wlen: 1,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                index: 4, // stable to old entry
                term: 2, // stable to old entry
                woffset: 5,
                wlen: 1,
            },
            // with snapshot
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 5, // stable to the first entry
                term: 1, // stable to the first entry
                woffset: 6,
                wlen: 0,
            },
            {
                entries: [new Entry({ index: 5, term: 1 }), new Entry({ index: 6, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 5, // stable to the first entry
                term: 1, // stable to the first entry
                woffset: 6,
                wlen: 1,
            },
            {
                entries: [new Entry({ index: 6, term: 2 })],
                offset: 6,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 5, term: 1 }),
                }),
                index: 6, // stable to the first entry and term mismatch
                term: 1, // stable to the first entry and term mismatch
                woffset: 6,
                wlen: 1,
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 1 }),
                }),
                index: 4, // stable to snapshot
                term: 1, // stable to snapshot
                woffset: 5,
                wlen: 1,
            },
            {
                entries: [new Entry({ index: 5, term: 2 })],
                offset: 5,
                snap: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 4, term: 2 }),
                }),
                index: 4, // stable to old entry
                term: 1, // stable to old entry
                woffset: 5,
                wlen: 1,
            },
        ];
    tests.forEach((tt, i, _) => {
        const u = new Unstable({
            entries: tt.entries,
            offset: tt.offset,
            snapshot: tt.snap,
            logger: getLogger(),
        });
        u.stableTo(tt.index, tt.term);

        t.equal(u.offset, tt.woffset, "#" + i + ": check offset");
        t.equal(u.entries.length, tt.wlen, "#" + i + ": check entries length");
    });

    t.end();
});

tap.test("unstable truncateAndAppend()", (t) => {
    const tests: {
        entries: Entry[];
        offset: number;
        snap?: Snapshot;
        toappend: Entry[];

        woffset: number;
        wentries: Entry[];
    }[] = [
            // append to the end
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                toappend: [new Entry({ index: 6, term: 1 }), new Entry({ index: 7, term: 1 })],
                woffset: 5,
                wentries: [
                    new Entry({ index: 5, term: 1 }),
                    new Entry({ index: 6, term: 1 }),
                    new Entry({ index: 7, term: 1 }),
                ],
            },
            // replace the unstable entries
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                toappend: [new Entry({ index: 5, term: 2 }), new Entry({ index: 6, term: 2 })],
                woffset: 5,
                wentries: [new Entry({ index: 5, term: 2 }), new Entry({ index: 6, term: 2 })],
            },
            {
                entries: [new Entry({ index: 5, term: 1 })],
                offset: 5,
                snap: undefined,
                toappend: [
                    new Entry({ index: 4, term: 2 }),
                    new Entry({ index: 5, term: 2 }),
                    new Entry({ index: 6, term: 2 }),
                ],
                woffset: 4,
                wentries: [
                    new Entry({ index: 4, term: 2 }),
                    new Entry({ index: 5, term: 2 }),
                    new Entry({ index: 6, term: 2 }),
                ],
            },
            // truncate the existing entries and append
            {
                entries: [
                    new Entry({ index: 5, term: 1 }),
                    new Entry({ index: 6, term: 1 }),
                    new Entry({ index: 7, term: 1 }),
                ],
                offset: 5,
                snap: undefined,
                toappend: [new Entry({ index: 6, term: 2 })],
                woffset: 5,
                wentries: [new Entry({ index: 5, term: 1 }), new Entry({ index: 6, term: 2 })],
            },
            {
                entries: [
                    new Entry({ index: 5, term: 1 }),
                    new Entry({ index: 6, term: 1 }),
                    new Entry({ index: 7, term: 1 }),
                ],
                offset: 5,
                snap: undefined,
                toappend: [new Entry({ index: 7, term: 2 }), new Entry({ index: 8, term: 2 })],
                woffset: 5,
                wentries: [
                    new Entry({ index: 5, term: 1 }),
                    new Entry({ index: 6, term: 1 }),
                    new Entry({ index: 7, term: 2 }),
                    new Entry({ index: 8, term: 2 }),
                ],
            },
        ];

    tests.forEach((tt, i, _) => {
        const u = new Unstable({
            entries: tt.entries,
            offset: tt.offset,
            snapshot: tt.snap,
            logger: getLogger(),
        });
        u.truncateAndAppend(tt.toappend);

        t.equal(u.offset, tt.woffset, "#" + i + ": check offset");
        t.strictSame(u.entries, tt.wentries, "#" + i + ": check entries");
    });

    t.end();
});

process.exit();

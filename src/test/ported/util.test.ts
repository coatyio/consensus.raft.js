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
import { Entry, EntryType, MessageType } from "../../ported/raftpb/raft.pb";
import { describeEntry, EntryFormatter, isLocalMsg, limitSize } from "../../ported/util";

const testFormatter: EntryFormatter = (data: RaftData) => {
    return String(data).toUpperCase(); // We know that data is of type string
};

// NOTE: In TypeScript, special characters like \x00 are not shown in the
// converted string
tap.test("util describeEntry()", (t) => {
    const entry: Entry = new Entry({
        term: 1,
        index: 2,
        type: EntryType.EntryNormal,
        data: "hello\x00world",
    });

    const defaultFormatted = describeEntry(entry, null);

    t.equal(
        defaultFormatted,
        "1/2 EntryNormal hello\x00world",
        'expected default output: "1/2 EntryNormal hello\x00world" found ' + defaultFormatted
    );

    const customFormatted = describeEntry(entry, testFormatter);

    t.equal(
        customFormatted,
        "1/2 EntryNormal HELLO\x00WORLD",
        'expected output: "1/2 EntryNormal HELLO\x00WORLD" found ' + customFormatted
    );

    t.end();
});

tap.test("util limitSize()", (t) => {
    const ents: Entry[] = [
        new Entry({ index: 4, term: 4 }),
        new Entry({ index: 5, term: 5 }),
        new Entry({ index: 6, term: 6 }),
    ];
    const fstEntSize = JSON.stringify(ents[0]).length;
    const sndEntSize = fstEntSize;
    const thdEntSize = fstEntSize;

    const tests: { maxSize: number; wentries: Entry[] }[] = [
        {
            maxSize: noLimit,
            wentries: [
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
                new Entry({ index: 6, term: 6 }),
            ],
        },
        // even if maxsize is zero, the first entry should be returned
        { maxSize: 0, wentries: [new Entry({ index: 4, term: 4 })] },
        // limit to 2
        {
            maxSize: fstEntSize + sndEntSize,
            wentries: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 5 })],
        },
        // limit to 2
        {
            maxSize: fstEntSize + sndEntSize + Math.floor(thdEntSize / 2),
            wentries: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 5 })],
        },
        {
            maxSize: fstEntSize + sndEntSize + thdEntSize - 1,
            wentries: [new Entry({ index: 4, term: 4 }), new Entry({ index: 5, term: 5 })],
        },
        // all
        {
            maxSize: fstEntSize + sndEntSize + thdEntSize,
            wentries: [
                new Entry({ index: 4, term: 4 }),
                new Entry({ index: 5, term: 5 }),
                new Entry({ index: 6, term: 6 }),
            ],
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const found = limitSize(ents, tt.maxSize);
        t.strictSame(
            found,
            tt.wentries,
            "#" + i + ": entries = " + found + ", want " + tt.wentries
        );
    }

    t.end();
});

tap.test("util isLocalMsg()", (t) => {
    const tests: { msgt: MessageType; isLocal: boolean }[] = [
        { msgt: MessageType.MsgHup, isLocal: true },
        { msgt: MessageType.MsgBeat, isLocal: true },
        { msgt: MessageType.MsgUnreachable, isLocal: true },
        { msgt: MessageType.MsgSnapStatus, isLocal: true },
        { msgt: MessageType.MsgCheckQuorum, isLocal: true },
        { msgt: MessageType.MsgTransferLeader, isLocal: false },
        { msgt: MessageType.MsgProp, isLocal: false },
        { msgt: MessageType.MsgApp, isLocal: false },
        { msgt: MessageType.MsgAppResp, isLocal: false },
        { msgt: MessageType.MsgVote, isLocal: false },
        { msgt: MessageType.MsgVoteResp, isLocal: false },
        { msgt: MessageType.MsgSnap, isLocal: false },
        { msgt: MessageType.MsgHeartbeat, isLocal: false },
        { msgt: MessageType.MsgHeartbeatResp, isLocal: false },
        { msgt: MessageType.MsgTimeoutNow, isLocal: false },
        { msgt: MessageType.MsgReadIndex, isLocal: false },
        { msgt: MessageType.MsgReadIndexResp, isLocal: false },
        { msgt: MessageType.MsgPreVote, isLocal: false },
        { msgt: MessageType.MsgPreVoteResp, isLocal: false },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];

        const got = isLocalMsg(tt.msgt);
        t.equal(got, tt.isLocal, "#" + i + ": got " + got + ", want " + tt.isLocal);
    }

    t.end();
});

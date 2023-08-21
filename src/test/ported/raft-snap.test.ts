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

import { newTestMemoryStorage, newTestRaft, withPeers } from "./raft-test-util";
import {
    ConfState,
    MessageType,
    Snapshot,
    Entry,
    Message,
    SnapshotMetadata,
} from "../../ported/raftpb/raft.pb";

const testingSnap = new Snapshot({
    metadata: new SnapshotMetadata({
        index: 11, // magic number
        term: 11, // magic number
        confState: new ConfState({ voters: ["1", "2"] }),
    }),
});

tap.test("sendingSnapshotSetPendingSnapshot", (t) => {
    const storage = newTestMemoryStorage(withPeers("1"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.restore(testingSnap);

    sm.becomeCandidate();
    sm.becomeLeader();

    const prs2 = sm.prs.progress.get("2")!;
    // force set the next of node 2, so that node 2 needs a snapshot
    prs2.next = sm.raftLog.firstIndex();

    sm.Step(
        new Message({
            from: "2",
            to: "1",
            type: MessageType.MsgAppResp,
            index: prs2.next - 1,
            reject: true,
        })
    );

    t.equal(prs2.pendingSnapshot, 11, `PendingSnapshot = ${prs2.pendingSnapshot}, want 11`);

    t.end();
});

tap.test("pendingSnapshotPauseReplication", (t) => {
    const storage = newTestMemoryStorage(withPeers("1", "2"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.restore(testingSnap);

    sm.becomeCandidate();
    sm.becomeLeader();

    const prs2 = sm.prs.progress.get("2")!;
    prs2.BecomeSnapshot(11);

    sm.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "somedata" })],
        })
    );
    const msgs = sm.readMessages();
    t.equal(msgs.length, 0, `msgs.length = ${msgs.length}, want 0`);

    t.end();
});

tap.test("snapshotFailure", (t) => {
    const storage = newTestMemoryStorage(withPeers("1", "2"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.restore(testingSnap);

    sm.becomeCandidate();
    sm.becomeLeader();

    const prs2 = sm.prs.progress.get("2")!;
    prs2.next = 1;
    prs2.BecomeSnapshot(11);

    sm.Step(
        new Message({
            from: "2",
            to: "1",
            type: MessageType.MsgSnapStatus,
            reject: true,
        })
    );

    t.equal(prs2.pendingSnapshot, 0, `PendingSnapshot = ${prs2.pendingSnapshot}, want 0`);
    t.equal(prs2.next, 1, `Next = ${prs2.next}, want 1`);
    t.ok(prs2.probeSent, `ProbeSent = ${prs2.probeSent}, want true`);

    t.end();
});

tap.test("snapshotSucceed", (t) => {
    const storage = newTestMemoryStorage(withPeers("1", "2"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.restore(testingSnap);

    sm.becomeCandidate();
    sm.becomeLeader();

    const prs2 = sm.prs.progress.get("2")!;
    prs2.next = 1;
    prs2.BecomeSnapshot(11);

    sm.Step(
        new Message({
            from: "2",
            to: "1",
            type: MessageType.MsgSnapStatus,
            reject: false,
        })
    );

    t.equal(prs2.pendingSnapshot, 0, `PendingSnapshot = ${prs2.pendingSnapshot}, want 0`);
    t.equal(prs2.next, 12, `Next = ${prs2.next}, want 12`);
    t.ok(prs2.probeSent, `ProbeSent = ${prs2.probeSent}, want true`);

    t.end();
});

tap.test("snapshotAbort", (t) => {
    const storage = newTestMemoryStorage(withPeers("1", "2"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.restore(testingSnap);

    sm.becomeCandidate();
    sm.becomeLeader();

    const prs2 = sm.prs.progress.get("2")!;
    prs2.next = 1;
    prs2.BecomeSnapshot(11);

    // A successful msgAppResp that has a higher/equal index than the pending
    // snapshot should abort the pending snapshot.
    sm.Step(new Message({ from: "2", to: "1", type: MessageType.MsgAppResp, index: 11 }));

    t.equal(prs2.pendingSnapshot, 0, `PendingSnapshot = ${prs2.pendingSnapshot}, want 0`);
    // The follower entered StateReplicate and the leader send an append and
    // optimistically updated the progress (so we see 13 instead of 12). There
    // is something to append because the leader appended an empty entry to the
    // log at index 12 when it assumed leadership.
    t.equal(prs2.next, 13, `Next = ${prs2.next}, want 13`);
    const n = prs2.inflights!.count();
    t.equal(n, 1, `expected an inflight message, got ${n}`);

    t.end();
});

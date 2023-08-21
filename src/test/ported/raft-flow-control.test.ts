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
import { Entry, Message, MessageType } from "../../ported/raftpb/raft.pb";


// TestMsgAppFlowControlFull ensures:
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.
tap.test("msgAppFlowControlFull", (t) => {
    const r = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();

    const pr2 = r.prs.progress.get("2")!;
    // force the progress to be in replicate state
    pr2.BecomeReplicate();
    // fill in the inflights window
    for (let i = 0; i < r.prs.maxInflight; i++) {
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "somedata" })],
            })
        );
        const ms = r.readMessages();

        t.equal(ms.length, 1, `#${i}: msgs.length = ${ms.length}, want 1`);
    }

    // ensure 1
    t.ok(pr2.inflights?.full(), `inflights?.full = ${pr2.inflights?.full()}, want true`);

    // ensure 2
    for (let i = 0; i < 10; i++) {
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "somedata" })],
            })
        );
        const msgs = r.readMessages();
        t.equal(msgs.length, 0, `#${i}: msgs.length = ${msgs.length}, want 0`);
    }

    t.end();
});

// TestMsgAppFlowControlMoveForward ensures msgAppResp can move forward the
// sending window correctly:
// 1. valid msgAppResp.index moves the windows to pass all smaller or equal
//    index.
// 2. out-of-dated msgAppResp has no effect on the sliding window.
tap.test("msgAppFlowControlMoveForward", (t) => {
    const r = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();

    const pr2 = r.prs.progress.get("2")!;
    // force the progress to be in replicate state
    pr2.BecomeReplicate();
    // fill in the inflights window
    for (let i = 0; i < r.prs.maxInflight; i++) {
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "somedata" })],
            })
        );
        r.readMessages();
    }

    // 1 is noop, 2 is the first proposal we just sent. so we start with 2.
    for (let tt = 2; tt < r.prs.maxInflight; tt++) {
        // move forward the window
        r.Step(
            new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgAppResp,
                index: tt,
            })
        );
        r.readMessages();

        // fill in the inflights window again
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "somedata" })],
            })
        );

        const ms = r.readMessages();
        t.equal(ms.length, 1, `#${tt}: msgs.length = ${ms.length}, want 1`);

        // ensure 1
        t.ok(pr2.inflights?.full(), `inflights?.full = ${pr2.inflights?.full()}, want true`);

        // ensure 2
        for (let i = 0; i < tt; i++) {
            r.Step(
                new Message({
                    from: "2",
                    to: "1",
                    type: MessageType.MsgAppResp,
                    index: i,
                })
            );
            t.ok(
                pr2.inflights?.full(),
                `#${i}: inflights?.full = ${pr2.inflights?.full()}, want true`
            );
        }
    }

    t.end();
});

// TestMsgAppFlowControlRecvHeartbeat ensures a heartbeat response frees one
// slot if the window is full.
tap.test("msgAppFlowControlRecvHeartbeat", (t) => {
    const r = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();

    const pr2 = r.prs.progress.get("2")!;
    // force the progress to be in replicate state
    pr2.BecomeReplicate();
    // fill in the inflights window
    for (let i = 0; i < r.prs.maxInflight; i++) {
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "somedata" })],
            })
        );
        r.readMessages();
    }

    for (let tt = 1; tt < 5; tt++) {
        t.ok(
            pr2.inflights?.full(),
            `#${tt}: inflights?.full = ${pr2.inflights?.full()}, want true`
        );

        // recv tt msgHeartbeatResp and expect one free slot
        for (let i = 0; i < tt; i++) {
            r.Step(
                new Message({
                    from: "2",
                    to: "1",
                    type: MessageType.MsgHeartbeatResp,
                })
            );
            r.readMessages();
            t.notOk(
                pr2.inflights?.full(),
                `#${tt}.${i}: inflights?.full = ${pr2.inflights?.full()}, want false`
            );
        }

        // one slot
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "somedata" })],
            })
        );
        const ms = r.readMessages();
        t.equal(ms.length, 1, `#${tt}: free slot = ${ms.length}, want 1`);

        // and just one slot
        for (let i = 0; i < 10; i++) {
            r.Step(
                new Message({
                    from: "1",
                    to: "1",
                    type: MessageType.MsgProp,
                    entries: [new Entry({ data: "somedata" })],
                })
            );
            const ms1 = r.readMessages();
            t.equal(ms1.length, 0, `#${tt}.${i}: ms.length = ${ms1.length}, want 0`);
        }

        // clear all pending messages.
        r.Step(new Message({ from: "2", to: "1", type: MessageType.MsgHeartbeatResp }));
        r.readMessages();
    }

    t.end();
});

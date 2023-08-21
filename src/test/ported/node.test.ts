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

import Channel, { ReadWriteChannel } from "../../@nodeguy/channel/lib";
import { interval, Subscription } from "rxjs";
import tap from "tap";

import {
    newTestRawNode,
    newTestRaft,
    newTestConfig,
    newTestMemoryStorage,
    withPeers,
    RaftForTest,
    isolate,
    newNetwork,
    newNetworkWithConfig,
    recover,
    send
} from "./raft-test-util";
import { emptyRaftData } from "../../non-ported/raft-controller";
import { RaftData } from "../../non-ported/raft-controller";
import { getLogger } from "../../ported/logger";
import {
    errStopped,
    MsgWithResult,
    Node,
    Ready,
    restartNode,
    SoftState,
    startNode
} from "../../ported/node";
import {
    errProposalDropped,
    noLimit,
    Raft,
    RaftConfig,
    StateType
} from "../../ported/raft";
import { RawNode } from "../../ported/rawnode";
import { ReadState } from "../../ported/readonly";
import { Status } from "../../ported/status";
import { MemoryStorage } from "../../ported/storage";
import { describeEntries, isLocalMsg, NullableError } from "../../ported/util";
import {
    ConfState,
    ConfChange,
    ConfChangeType,
    Entry,
    EntryType,
    HardState,
    Message,
    MessageType,
    Snapshot,
    SnapshotMetadata,
    ConfChangeV2,
    ridnone,
} from "../../ported/raftpb/raft.pb";

const nilChannel = new Channel<any>();

function defaultShift() {
    const closed = new Channel();
    closed.close();
    return closed.shift();
}

// readyWithTimeout selects from n.ready() with a 1-second timeout. It panics on
// timeout, which is better than the indefinite wait that would occur if this
// channel were read without being wrapped in a select.
async function readyWithTimeout(n: Node): Promise<Ready> {
    // Channel with a timeout that is pushed after 1 second
    const timeout = new Channel<{}>();

    let intervalSubscription: Subscription;
    intervalSubscription = interval(1 * 1000).subscribe(() => {
        timeout.push({});
        // stop subscription to only trigger this once
        intervalSubscription.unsubscribe();
    });

    const readyChan = n.ready();
    switch (await Channel.select([readyChan.shift(), timeout.shift()])) {
        case readyChan:
            intervalSubscription.unsubscribe();
            return readyChan.value();
        default:
            throw new Error("timed out waiting for ready");
    }
}

function createNewTestNode(
    param: {
        propc?: ReadWriteChannel<MsgWithResult>;
        recvc?: ReadWriteChannel<Message>;
        confc?: ReadWriteChannel<ConfChangeV2>;
        confstatec?: ReadWriteChannel<ConfState>;
        readyc?: ReadWriteChannel<Ready>;
        advancec?: ReadWriteChannel<{}>;
        tickc?: ReadWriteChannel<{}>;
        done?: ReadWriteChannel<{}>;
        stop?: ReadWriteChannel<{}>;
        status?: ReadWriteChannel<ReadWriteChannel<Status>>;
        rn?: RawNode;
    } = {}
): Node {
    return new Node({
        propc: param.propc ?? nilChannel,
        recvc: param.recvc ?? nilChannel,
        confc: param.confc ?? nilChannel,
        confstatec: param.confstatec ?? nilChannel,
        readyc: param.readyc ?? nilChannel,
        advancec: param.advancec ?? nilChannel,
        tickc: param.tickc ?? nilChannel,
        done: param.done ?? nilChannel,
        stop: param.stop ?? nilChannel,
        status: param.status ?? nilChannel,
        rn: param.rn ?? newTestRawNode("1", 10, 1, MemoryStorage.NewMemoryStorage()),
    });
}

// TestNodeStep ensures that node.Step sends msgProp to propc chan and other
// kinds of messages to recvc chan.
tap.test("Node step", async (t) => {
    for (const msgTypeString in MessageType) {
        // filter everything that is not a msgtype string
        if (isNaN(Number(msgTypeString))) {
            continue;
        }

        const n = createNewTestNode({
            propc: new Channel<MsgWithResult>(1),
            recvc: new Channel<Message>(1),
        });
        const msgt = Number(msgTypeString);

        await n.step(new Message({ type: msgt }));
        // Proposal goes to proc chan. Others go to recvc chan.
        if (msgt === MessageType.MsgProp) {
            switch (await Channel.select([n.getPropChannel().shift(), defaultShift()])) {
                case n.getPropChannel():
                    break;
                default:
                    t.fail(`${msgt}: cannot receive ${msgTypeString} on propc chan`);
            }
        } else {
            if (isLocalMsg(msgt)) {
                switch (await Channel.select([n.getRecvChannel().shift(), defaultShift()])) {
                    case n.getRecvChannel():
                        t.fail(`${msgt}: step should ignore ${msgTypeString}`);
                        break;
                    default:
                    // do nothing
                }
            } else {
                switch (await Channel.select([n.getRecvChannel().shift(), defaultShift()])) {
                    case n.getRecvChannel():
                        // do nothing
                        break;
                    default:
                        t.fail(`${msgt}: cannot receive ${msgTypeString} on recvc chan`);
                }
            }
        }
    }

    t.end();
});

// Stop should unblock Step()
tap.test("Node, stepUnblock", async (t) => {
    // a node without buffer to block step
    const n = createNewTestNode({
        propc: new Channel<MsgWithResult>(),
        done: new Channel<{}>(),
    });

    const tests: { unblock: () => void; werr: Error }[] = [
        { unblock: n.getDoneChannel().close, werr: errStopped },
    ];

    for (let i = 0; i < tests.length; i++) {
        const tt = tests[i];
        const errc = new Channel<NullableError>(1);

        n.step(new Message({ type: MessageType.MsgProp })).then((err) => errc.push(err));

        // Channel with a timeout that is pushed after 10 seconds
        const timeout = new Channel<{}>();

        let intervalSubscription: Subscription;
        intervalSubscription = interval(1 * 1000).subscribe(async () => {
            await timeout.push({});
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });

        tt.unblock();

        switch (await Channel.select([errc.shift(), timeout.shift()])) {
            case errc:
                const err = errc.value();
                t.strictSame(err, tt.werr, `#${i}: err = ${err?.message}, want ${tt.werr.message}`);

                switch (await Channel.select([n.getDoneChannel().shift(), defaultShift()])) {
                    case n.getDoneChannel():
                        n.setDoneChannel(new Channel<{}>());
                        break;
                    default:
                    // do nothing
                }
                break;
            case timeout:
                t.fail(`#${i}: failed to unblock step`);
        }
        intervalSubscription.unsubscribe();
    }
    t.end();
});

// TestNodePropose ensures that node.Propose sends the given proposal to the
// underlying raft.
tap.test("Node propose", async (t) => {
    const msgs: Message[] = [];
    const appendStep = (appendRaft: Raft, m: Message): NullableError => {
        msgs.push(m);
        return null;
    };

    const s = newTestMemoryStorage(withPeers("1"));
    const rn = newTestRawNode("1", 10, 1, s);
    const n = Node.NewNode(rn);
    const r = rn.raft;

    n.run();
    const err = await n.campaign();
    t.equal(err, null, `Error: ${err}, want null. Err.message: ${err?.message}`);

    while (true) {
        const rd = await n.ready().shift();

        s.Append(rd.entries);
        // Change the step function to appendStep until this raft becomes leader
        if (rd.softState!.lead === r.id) {
            r.step = appendStep;
            await n.advance();
            break;
        }
        await n.advance();
    }

    await n.propose("somedata");
    await n.stop();

    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    t.equal(
        msgs[0].type,
        MessageType.MsgProp,
        `msg type = ${MessageType[msgs[0].type]}, want MsgProp`
    );
    t.strictSame(
        msgs[0].entries[0].data,
        "somedata",
        `data = ${msgs[0].entries[0].data}, want somedata`
    );

    t.end();
});

// TestNodeReadIndex ensures that node.readIndex sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out
// through ready chan.
tap.test("Node readIndex", async (t) => {
    const msgs: Message[] = [];
    const appendStep = (appendRaft: Raft, m: Message): NullableError => {
        msgs.push(m);
        return null;
    };
    const wrs: ReadState[] = [{ index: 1, requestCtx: "somedata" }];

    const s = newTestMemoryStorage(withPeers("1"));
    const rn = newTestRawNode("1", 10, 1, s);
    const n = Node.NewNode(rn);
    const r = rn.raft;
    r.readStates = wrs;

    n.run();
    await n.campaign();

    while (true) {
        const rd = await n.ready().shift();

        t.strictSame(
            rd.readStates,
            wrs,
            `ReadStates = ${JSON.stringify(rd.readStates)}, want ${JSON.stringify(wrs)}`
        );

        s.Append(rd.entries);

        if (rd.softState!.lead === r.id) {
            await n.advance();
            break;
        }
        await n.advance();
    }

    r.step = appendStep;
    const wrequestCtx = "somedata2";
    await n.readIndex(wrequestCtx);
    await n.stop();

    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    t.equal(
        msgs[0].type,
        MessageType.MsgReadIndex,
        `msg type = ${MessageType[msgs[0].type]}, want MsgReadIndex`
    );
    const firstEntryData = msgs[0].entries[0].data;
    t.strictSame(firstEntryData, wrequestCtx, `data = ${firstEntryData}, want ${wrequestCtx}`);

    t.end();
});

// TestDisableProposalForwarding ensures that proposals are not forwarded to the
// leader when DisableProposalForwarding is true.
tap.test("Node disableProposalForwarding", (t) => {
    const r1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const r2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const cfg3 = newTestConfig("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    cfg3.disableProposalForwarding = true;
    const r3 = new RaftForTest(cfg3);
    const nt = newNetwork(r1, r2, r3);

    // elect r1 as leader
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const testEntries: Entry[] = [new Entry({ data: "testdata" })];

    // send proposal to r2(follower) where DisableProposalForwarding is false
    r2.Step(
        new Message({
            from: "2",
            to: "2",
            type: MessageType.MsgProp,
            entries: testEntries,
        })
    );

    // verify r2(follower) does forward the proposal when
    // DisableProposalForwarding is false
    t.equal(r2.msgs.length, 1, `r2.msgs.length = ${r2.msgs.length}, want 1`);

    // send proposal to r3(follower) where DisableProposalForwarding is true
    r3.Step(
        new Message({
            from: "3",
            to: "3",
            type: MessageType.MsgProp,
            entries: testEntries,
        })
    );

    // verify r3(follower) does not forward the proposal when
    // DisableProposalForwarding is true
    t.equal(r3.msgs.length, 0, `r3.msgs.length = ${r2.msgs.length}, want 0`);

    t.end();
});

// TestNodeReadIndexToOldLeader ensures that raftpb.MsgReadIndex to old leader
// gets forwarded to the new leader and 'send' method does not attach its term.
tap.test("Node readIndexToOldLeader", (t) => {
    const r1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const r2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const r3 = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    const nt = newNetwork(r1, r2, r3);

    // elect r1 as leader
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const testEntries: Entry[] = [new Entry({ data: "testdata" })];

    // send readindex request to r2(follower)
    r2.Step(
        new Message({
            from: "2",
            to: "2",
            type: MessageType.MsgReadIndex,
            entries: testEntries,
        })
    );

    // verify r2(follower) forwards this message to r1(leader) with term not set
    t.equal(r2.msgs.length, 1, `msgs.length = ${r2.msgs.length}, want 1`);
    const readIndexMsg1: Message = new Message({
        from: "2",
        to: "1",
        type: MessageType.MsgReadIndex,
        entries: testEntries,
    });
    t.strictSame(
        r2.msgs[0],
        readIndexMsg1,
        `r2.msgs[0] = ${JSON.stringify(r2.msgs[0])}, want ${JSON.stringify(readIndexMsg1)}`
    );

    // send readindex request to r3(follower)
    r3.Step(
        new Message({
            from: "3",
            to: "3",
            type: MessageType.MsgReadIndex,
            entries: testEntries,
        })
    );

    // verify r3(follower) forwards this message to r1(leader) with term not set
    // as well.
    t.equal(r3.msgs.length, 1, `r3.msgs.length = ${r3.msgs.length}, want 1`);
    const readIndexMsg2: Message = new Message({
        from: "3",
        to: "1",
        type: MessageType.MsgReadIndex,
        entries: testEntries,
    });
    t.strictSame(
        r3.msgs[0],
        readIndexMsg2,
        `r3.msgs[0] = ${JSON.stringify(r3.msgs[0])}, want ${JSON.stringify(readIndexMsg2)}`
    );

    // now elect r3 as leader
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    // let r1 steps the two messages previously we got from r2, r3
    r1.Step(readIndexMsg1);
    r1.Step(readIndexMsg2);

    // verify r1(follower) forwards these messages again to r3(new leader)
    t.equal(r1.msgs.length, 2, `r1.msgs.length = ${r1.msgs.length}, want 2`);
    const readIndexMsg3 = new Message({
        from: "2",
        to: "3",
        type: MessageType.MsgReadIndex,
        entries: testEntries,
    });
    t.strictSame(
        r1.msgs[0],
        readIndexMsg3,
        `r1.msgs[0] = ${JSON.stringify(r1.msgs[0])}, want ${JSON.stringify(readIndexMsg3)}`
    );
    const readIndexMsg4 = new Message({
        from: "3",
        to: "3",
        type: MessageType.MsgReadIndex,
        entries: testEntries,
    });
    t.strictSame(
        r1.msgs[1],
        readIndexMsg4,
        `r1.msgs[1] = ${JSON.stringify(r1.msgs[1])}, want ${JSON.stringify(readIndexMsg4)}`
    );

    t.end();
});

// TestNodeProposeConfig ensures that node.proposeConfChange sends the given
// configuration proposal to the underlying raft.
tap.test("Node proposeConfig", async (t) => {
    const msgs: Message[] = [];
    const appendStep = (appendRaft: Raft, m: Message): NullableError => {
        msgs.push(m);
        return null;
    };

    const s = newTestMemoryStorage(withPeers("1"));
    const rn = newTestRawNode("1", 10, 1, s);
    const n = Node.NewNode(rn);
    const r = rn.raft;

    n.run();
    await n.campaign();

    while (true) {
        const rd = await n.ready().shift();

        s.Append(rd.entries);
        // change the step function to appendStep until this raft becomes leader
        if (rd.softState!.lead === r.id) {
            r.step = appendStep;
            await n.advance();
            break;
        }
        await n.advance();
    }

    const cc: ConfChange = new ConfChange({
        type: ConfChangeType.ConfChangeAddNode,
        nodeId: "1",
    });
    const ccData = cc;

    await n.proposeConfChange(cc);
    await n.stop();

    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    t.equal(
        msgs[0].type,
        MessageType.MsgProp,
        `msg type = ${MessageType[msgs[0].type]}, want MsgProp`
    );
    const firstMsgData = msgs[0].entries[0].data;
    t.strictSame(firstMsgData, ccData, `data = ${firstMsgData}, want ${ccData}`);

    t.end();
});

// TestNodeProposeAddDuplicateNode ensures that two proposes to add the same
// node should not affect the later propose to add new node.
tap.test("Node proposeAddDuplicateNode", async (t) => {
    const s = newTestMemoryStorage(withPeers("1"));
    const rn = newTestRawNode("1", 10, 1, s);
    const n = Node.NewNode(rn);

    n.run();
    await n.campaign();
    const rdyEntries: Entry[] = [];

    const tickerChan = new Channel<{}>();
    const tickPeriod = 100;
    const tickerSubscription = interval(tickPeriod).subscribe(async () => {
        const tickTimeout = new Channel<{}>();
        const subscription = interval(tickPeriod).subscribe(async () => {
            await tickTimeout.push({});
            // stop subscription to only trigger this once
            subscription.unsubscribe();
        });

        switch (await Channel.select([tickerChan.push({}), tickTimeout.shift()])) {
            case tickerChan:
                subscription.unsubscribe();
                break;
            case tickTimeout:
            // Omit tick after tickPeriod is over
        }
    });
    const done = new Channel<{}>();
    const stop = new Channel<{}>();
    const applyConfChan = new Channel<{}>();

    const readyChan = n.ready();
    async function asyncFunction() {
        try {
            while (true) {
                switch (
                await Channel.select([stop.shift(), tickerChan.shift(), readyChan.shift()])
                ) {
                    case stop:
                        return;
                    case tickerChan:
                        await n.tick();
                        break;
                    case readyChan:
                        const rd = readyChan.value();
                        s.Append(rd.entries);
                        let applied = false;
                        for (const e of rd.entries) {
                            rdyEntries.push(e);

                            switch (e.type) {
                                case EntryType.EntryNormal:
                                    break;
                                case EntryType.EntryConfChange:
                                    const cc: ConfChange = new ConfChange(e.data);
                                    await n.applyConfChange(cc);
                                    applied = true;
                            }

                            await n.advance();
                            if (applied) {
                                await applyConfChan.push({});
                            }
                        }
                        break;
                }
            }
        } finally {
            done.close();
        }
    }

    try {
        asyncFunction();

        const cc1: ConfChange = new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "1",
        });
        const ccData1 = cc1;
        await n.proposeConfChange(cc1);
        await applyConfChan.shift();

        // try add the same node again
        await n.proposeConfChange(cc1);
        await applyConfChan.shift();

        // the new node join should be ok
        const cc2: ConfChange = new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "2",
        });
        const ccData2 = cc2;
        await n.proposeConfChange(cc2);
        await applyConfChan.shift();

        stop.close();
        await done.shift();

        t.equal(rdyEntries.length, 4, `entries.length = ${rdyEntries.length}, want 4`);
        t.strictSame(rdyEntries[1].data, ccData1, `data = ${rdyEntries[1].data}, want ${ccData1}`);
        t.strictSame(rdyEntries[3].data, ccData2, `data = ${rdyEntries[3].data}, want ${ccData2}`);

        await n.stop();
    } finally {
        tickerSubscription.unsubscribe();
    }

    t.end();
});

// TestBlockProposal ensures that node will block proposal when it does not know
// who is the current leader; node will accept proposal when it knows who is the
// current leader.
tap.test("Node blockProposal", async (t) => {
    const rn = newTestRawNode("1", 10, 1, newTestMemoryStorage(withPeers("1")));
    const n = Node.NewNode(rn);

    n.run();

    try {
        const errc = new Channel<Error>(1);
        n.propose("somedata").then(errc.push);

        switch (await Channel.select([errc.shift(), defaultShift()])) {
            case errc:
                const err = errc.value();
                t.fail(`err = ${err?.message}, want blocking`);
                break;
            default:
                break;
        }

        await n.campaign();

        // Channel with a timeout that is pushed after 10 seconds
        const timeout = new Channel<{}>();

        let intervalSubscription: Subscription;
        intervalSubscription = interval(10 * 1000).subscribe(async () => {
            await timeout.push({});
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });

        switch (await Channel.select([errc.shift(), timeout.shift()])) {
            case errc:
                const err = errc.value();
                t.equal(err, null, `err = ${err}, want null`);
                intervalSubscription.unsubscribe();
                break;
            case timeout:
                t.fail("blocking proposal, want unblocking");
        }
    } finally {
        await n.stop();
    }

    t.end();
});

tap.test("Node proposeWaitDropped", async (t) => {
    const msgs: Message[] = [];
    const droppingMsg = "test_dropping";
    const dropStep = (dropRaft: Raft, m: Message): NullableError => {
        if (m.type === MessageType.MsgProp && JSON.stringify(m).includes(droppingMsg)) {
            getLogger().infof("successfully dropped message");
            return errProposalDropped;
        }
        msgs.push(m);
        return null;
    };

    const s = newTestMemoryStorage(withPeers("1"));
    const rn = newTestRawNode("1", 10, 1, s);
    const n = Node.NewNode(rn);
    const r = rn.raft;

    n.run();
    await n.campaign();

    while (true) {
        const rd = await n.ready().shift();

        s.Append(rd.entries);
        // change the step function to dropStep until this raft becomes leader
        if (rd.softState!.lead === r.id) {
            r.step = dropStep;
            await n.advance();
            break;
        }
        await n.advance();
    }

    // propose with cancel should be cancelled early if dropped
    const err = await new Promise<NullableError>((resolve) => {
        let intervalSubscription: Subscription;
        n.propose(droppingMsg).then((e) => {
            while (true) {
                // busy wait til intervalSubscription was assigned
                if (intervalSubscription) {
                    intervalSubscription.unsubscribe();
                    break;
                }
            }
            resolve(e);
        });
        intervalSubscription = interval(1 * 100).subscribe(async () => {
            resolve(null);
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });
    });

    t.equal(
        err,
        errProposalDropped,
        `should drop proposal, got ${err?.message}, want ${errProposalDropped?.message}`
    );

    await n.stop();
    t.equal(msgs.length, 0, `msgs.length = ${msgs.length}, want 0`);

    t.end();
});

// TestNodeTick ensures that node.tick() will increase the elapsed of the
// underlying raft state machine.
tap.test("Node tick", async (t) => {
    const s = newTestMemoryStorage(withPeers("1"));
    const rn = newTestRawNode("1", 10, 1, s);
    const n = Node.NewNode(rn);
    const r = rn.raft;

    n.run();
    const elapsed = r.electionElapsed;
    await n.tick();

    // Wait for tick NOTE: Differs from go since theres no way to map
    // len(some_channel) to TypeScript
    await new Promise((f) => setTimeout(f, 100));

    await n.stop();

    t.equal(r.electionElapsed, elapsed + 1, `elapsed = ${r.electionElapsed}, want ${elapsed + 1}`);

    t.end();
});

// TestNodeStop ensures that node.stop() blocks until the node has stopped
// processing, and that it is idempotent
tap.test("Node stop", async (t) => {
    const rn = newTestRawNode("1", 10, 1, newTestMemoryStorage(withPeers("1")));
    const n = Node.NewNode(rn);
    const donec = new Channel<{}>();

    n.run().then(() => donec.close());

    let status = await n.status();
    await n.stop();

    // Channel with a timeout that is pushed after 1 second
    const timeout = new Channel<{}>();

    let intervalSubscription: Subscription;
    intervalSubscription = interval(1 * 1000).subscribe(() => {
        timeout.push({});
        // stop subscription to only trigger this once
        intervalSubscription.unsubscribe();
    });

    switch (await Channel.select([donec.shift(), timeout.shift()])) {
        case donec:
            intervalSubscription.unsubscribe();
            break;
        case timeout:
            t.fail("timed out waiting for node to stop!");
    }

    const emptyStatus: Status = new Status();
    t.strictNotSame(status, emptyStatus, `status = ${JSON.stringify(status)}, want not empty`);

    // Further status should return be empty, the node is stopped.
    status = await n.status();
    t.strictSame(
        status,
        emptyStatus,
        `status = ${JSON.stringify(status)}, want empty: ${JSON.stringify(emptyStatus)}`
    );

    // Subsequent Stops should have no effect.
    await n.stop();

    t.end();
});

tap.test("Node readyContainUpdates", (t) => {
    const tests: { rd: Ready; wcontain: boolean }[] = [
        { rd: new Ready(), wcontain: false },
        {
            rd: new Ready({ softState: new SoftState({ lead: "1" }) }),
            wcontain: true,
        },
        {
            rd: new Ready({ hardState: new HardState({ vote: "1" }) }),
            wcontain: true,
        },
        { rd: new Ready({ entries: [new Entry()] }), wcontain: true },
        { rd: new Ready({ committedEntries: [new Entry()] }), wcontain: true },
        { rd: new Ready({ messages: [new Message()] }), wcontain: true },
        {
            rd: new Ready({
                snapshot: new Snapshot({
                    metadata: new SnapshotMetadata({ index: 1 }),
                }),
            }),
            wcontain: true,
        },
    ];

    tests.forEach((tt, i) => {
        const g = tt.rd.containsUpdates();
        t.equal(g, tt.wcontain, `#${i}: containsUpdates = ${g}, want ${tt.wcontain}`);
    });

    t.end();
});

// TestNodeStart ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
tap.test("Node start", async (t) => {
    const cc = new ConfChange({
        type: ConfChangeType.ConfChangeAddNode,
        nodeId: "1",
    });
    const ccData = cc;

    const wants: Ready[] = [
        new Ready({
            hardState: new HardState({ term: 1, commit: 1, vote: ridnone }),
            entries: [
                new Entry({
                    type: EntryType.EntryConfChange,
                    term: 1,
                    index: 1,
                    data: ccData,
                }),
            ],
            committedEntries: [
                new Entry({
                    type: EntryType.EntryConfChange,
                    term: 1,
                    index: 1,
                    data: ccData,
                }),
            ],
            mustSync: true,
        }),
        new Ready({
            hardState: new HardState({ term: 2, commit: 3, vote: "1" }),
            entries: [
                new Entry({
                    term: 2,
                    index: 3,
                    data: "foo",
                }),
            ],
            committedEntries: [
                new Entry({
                    term: 2,
                    index: 3,
                    data: "foo",
                }),
            ],
            mustSync: true,
        }),
    ];
    const storage = MemoryStorage.NewMemoryStorage();

    const c = new RaftConfig();
    c.id = "1";
    c.electionTick = 10;
    c.heartbeatTick = 1;
    c.storage = storage;
    c.maxSizePerMsg = noLimit;
    c.maxInflightMsgs = 256;

    const n = startNode(c, [{ id: "1", context: emptyRaftData }]);

    try {
        const g = await n.ready().shift();

        t.strictSame(
            g,
            wants[0],
            `#${1}, g = ${JSON.stringify(g)}, want = ${JSON.stringify(wants[0])}`
        );
        storage.Append(g.entries);
        await n.advance();

        const err = await n.campaign();
        t.equal(err, null, `Got error: ${err?.message}, want null`);

        const rd = await n.ready().shift();

        storage.Append(rd.entries);
        await n.advance();

        await n.propose("foo");
        const g2 = await n.ready().shift();

        t.strictSame(
            g2,
            wants[1],
            `#${2}, g = ${JSON.stringify(g2)}, want = ${JSON.stringify(wants[1])}`
        );
        storage.Append(g2.entries);
        await n.advance();

        // Channel with a timeout that is pushed after 1 millisecond
        const timeout = new Channel<{}>();

        let intervalSubscription: Subscription;
        intervalSubscription = interval(1).subscribe(() => {
            timeout.push({});
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });

        const readyChan = n.ready();
        switch (await Channel.select([readyChan.shift(), timeout.shift()])) {
            case readyChan:
                const unexpectedReady = readyChan.value();
                intervalSubscription.unsubscribe();
                t.fail(`unexpected Ready: ${JSON.stringify(unexpectedReady)}`);
                break;
            case timeout:
                break;
        }
    } finally {
        await n.stop();
    }
    t.end();
});

tap.test("Node restart", async (t) => {
    const entries: Entry[] = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 1, index: 2, data: "foo" }),
    ];
    const st = new HardState({ term: 1, commit: 1 });

    const want: Ready = new Ready({
        // No HardState is emitted because there was no change.
        hardState: new HardState(),
        // commit up to index commit index in st
        committedEntries: entries.slice(0, st.commit),
        // MustSync is false because no HardState or new entries are provided.
        mustSync: false,
    });

    const storage = MemoryStorage.NewMemoryStorage();
    storage.SetHardState(st);
    storage.Append(entries);
    const c: RaftConfig = new RaftConfig();
    c.id = "1";
    c.electionTick = 10;
    c.heartbeatTick = 1;
    c.storage = storage;
    c.maxSizePerMsg = noLimit;
    c.maxInflightMsgs = 256;

    const n = restartNode(c);

    try {
        const g = await n.ready().shift();

        t.strictSame(g, want, `g = ${JSON.stringify(g)}, want = ${JSON.stringify(want)}`);
        await n.advance();

        // Channel with a timeout that is pushed after 1 millisecond
        const timeout = new Channel<{}>();

        let intervalSubscription: Subscription;
        intervalSubscription = interval(1).subscribe(() => {
            timeout.push({});
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });

        const readyChan = n.ready();
        switch (await Channel.select([readyChan.shift(), timeout.shift()])) {
            case readyChan:
                const rd = readyChan.value();
                intervalSubscription.unsubscribe();
                t.fail(`unexpected Ready: ${JSON.stringify(rd)}`);
                break;
            case timeout:
                break;
        }
    } finally {
        await n.stop();
    }

    t.end();
});

tap.test("Node restartFromSnapshot", async (t) => {
    const snap: Snapshot = new Snapshot({
        metadata: new SnapshotMetadata({
            confState: new ConfState({ voters: ["1", "2"] }),
            index: 2,
            term: 1,
        }),
    });
    const entries: Entry[] = [new Entry({ term: 1, index: 3, data: "foo" })];
    const st = new HardState({ term: 1, commit: 3 });

    const want = new Ready({
        // No HardState is emitted because nothing changed relative to what is
        // already persisted.
        hardState: new HardState(),
        // commit up to index commit index in st
        committedEntries: entries,
        // MustSync is only true when there is a new HardState or new entries;
        // neither is the case here.
        mustSync: false,
    });

    const s = MemoryStorage.NewMemoryStorage();
    s.SetHardState(st);
    s.ApplySnapshot(snap);
    s.Append(entries);
    const c = new RaftConfig();
    c.id = "1";
    c.electionTick = 10;
    c.heartbeatTick = 1;
    c.storage = s;
    c.maxSizePerMsg = noLimit;
    c.maxInflightMsgs = 256;

    const n = restartNode(c);

    try {
        const g = await n.ready().shift();

        t.strictSame(g, want, `g = ${JSON.stringify(g)}, want ${JSON.stringify(want)}`);
        await n.advance();

        // Channel with a timeout that is pushed after 1 millisecond
        const timeout = new Channel<{}>();

        let intervalSubscription: Subscription;
        intervalSubscription = interval(1).subscribe(() => {
            timeout.push({});
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });

        const readyChan = n.ready();
        switch (await Channel.select([readyChan.shift(), timeout.shift()])) {
            case readyChan:
                const rd = readyChan.value();
                intervalSubscription.unsubscribe();
                t.fail(`unexpected Ready: ${JSON.stringify(rd)}`);
                break;
            case timeout:
                break;
        }
    } finally {
        await n.stop();
    }

    t.end();
});

tap.test("Node advance", async (t) => {
    const storage = MemoryStorage.NewMemoryStorage();
    const c = new RaftConfig();
    c.id = "1";
    c.electionTick = 10;
    c.heartbeatTick = 1;
    c.storage = storage;
    c.maxSizePerMsg = noLimit;
    c.maxInflightMsgs = 256;

    const n = startNode(c, [{ id: "1", context: [] }]);

    try {
        const rd = await n.ready().shift();
        storage.Append(rd.entries);
        await n.advance();

        await n.campaign();
        await n.ready().shift();

        await n.propose("foo");

        // Channel with a timeout that is pushed after 1 millisecond
        let timeout = new Channel<{}>();

        let intervalSubscription: Subscription;
        intervalSubscription = interval(1).subscribe(() => {
            timeout.push({});
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });

        const readyChan = n.ready();
        switch (await Channel.select([readyChan.shift(), timeout.shift()])) {
            case readyChan:
                intervalSubscription.unsubscribe();
                t.fail(`unexpected Ready before Advance: ${rd}`);
                break;
            case timeout:
        }
        storage.Append(rd.entries);
        await n.advance();

        timeout = new Channel<{}>();

        intervalSubscription = interval(100).subscribe(() => {
            timeout.push({});
            // stop subscription to only trigger this once
            intervalSubscription.unsubscribe();
        });
        switch (await Channel.select([readyChan.shift(), timeout.shift()])) {
            case readyChan:
                intervalSubscription.unsubscribe();
                break;
            case timeout:
                t.fail(`expect Ready after advance, but there is no Ready available`);
        }
    } finally {
        await n.stop();
    }

    t.end();
});

tap.test("Node softStateEqual", (t) => {
    const tests: { st: SoftState; we: boolean }[] = [
        { st: new SoftState(), we: true },
        { st: new SoftState({ lead: "1" }), we: false },
        {
            st: new SoftState({ raftState: StateType.StateLeader }),
            we: false,
        },
    ];

    tests.forEach((tt, i) => {
        const g = tt.st.equal(new SoftState());
        t.equal(g, tt.we, `#${i}: equal = ${g}, want ${tt.we}`);
    });

    t.end();
});

tap.test("Node isHardStateEqual", (t) => {
    const tests: { st: HardState; we: boolean }[] = [
        { st: new HardState(), we: true },
        { st: new HardState({ vote: "1" }), we: false },
        { st: new HardState({ commit: 1 }), we: false },
        { st: new HardState({ term: 1 }), we: false },
    ];

    tests.forEach((tt, i) => {
        const g = new HardState().isHardStateEqual(tt.st);
        t.equal(g, tt.we, `#${i}: equal = ${g}, want ${tt.we}`);
    });

    t.end();
});

tap.test("Node proposeAddLearnerNode", async (t) => {
    // Channel upon that a value is pushed every 100 milliseconds
    const ticker = new Channel<{}>();

    let intervalSubscription: Subscription;
    intervalSubscription = interval(100).subscribe(() => {
        ticker.push({});
    });

    try {
        const s = newTestMemoryStorage(withPeers("1"));
        const rn = newTestRawNode("1", 10, 1, s);
        const n = Node.NewNode(rn);

        n.run();
        await n.campaign();

        const stop = new Channel<{}>();
        const done = new Channel<{}>();
        const applyConfChan = new Channel<{}>();

        async function asyncFunc() {
            try {
                while (true) {
                    const readyChan = n.ready();
                    switch (
                    await Channel.select([stop.shift(), ticker.shift(), readyChan.shift()])
                    ) {
                        case stop:
                            return;
                        case ticker:
                            n.tick();
                            break;
                        case readyChan:
                            const rd = readyChan.value();
                            s.Append(rd.entries);
                            getLogger().infof("raft: %o", rd.entries);
                            for (const ent of rd.entries) {
                                if (ent.type !== EntryType.EntryConfChange) {
                                    continue;
                                }
                                const cc: ConfChange = new ConfChange(ent.data);

                                const state = await n.applyConfChange(cc);

                                t.notOk(
                                    state.learners.length === 0 ||
                                    state.learners[0] !== cc.nodeId ||
                                    cc.nodeId !== "2",
                                    `apply conf change should return new added learner: ${JSON.stringify(
                                        state
                                    )}`
                                );

                                t.equal(
                                    state.voters.length,
                                    1,
                                    `add learner should not change the nodes: ${JSON.stringify(
                                        state
                                    )}`
                                );
                                getLogger().infof("apply raft conf: %o changed to: %o", cc, state);
                                await applyConfChan.push({});
                            }
                            await n.advance();
                    }
                }
            } finally {
                done.close();
            }
        }
        asyncFunc();

        const proposeCC = new ConfChange({
            type: ConfChangeType.ConfChangeAddLearnerNode,
            nodeId: "2",
        });
        await n.proposeConfChange(proposeCC);
        await applyConfChan.shift();
        stop.close();
        await done.shift();
    } finally {
        intervalSubscription.unsubscribe();
        ticker.close();
    }

    t.end();
});

tap.test("Node appendPagination", (t) => {
    const maxSizePerMsg = 2080;
    const n = newNetworkWithConfig(
        (c: RaftConfig) => {
            c.maxSizePerMsg = maxSizePerMsg;
        },
        null,
        null,
        null
    );

    let seenFullMessage = false;
    // Inspect all messages to see that we never exceed the limit, but we do see
    // messages of larger than half the limit.
    n.msgHook = (m: Message): boolean => {
        if (m.type === MessageType.MsgApp) {
            let size = 0;
            for (const e of m.entries) {
                size += JSON.stringify(e).length;
            }
            if (size > maxSizePerMsg) {
                t.fail(`sent MsgApp that is too large: ${size}`);
            }
            if (size > maxSizePerMsg / 2) {
                seenFullMessage = true;
            }
        }
        return true;
    };

    send(n, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    // Partition the network while we make our proposals. This forces the
    // entries to be batched into larger messages.
    isolate(n, "1");
    const blob: string = new Array(1000 + 1).join("a"); // = "aaa...a"

    for (let i = 0; i < 5; i++) {
        send(
            n,
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: blob })],
            })
        );
    }

    recover(n);

    // After the partition recovers, tick the clock to wake everything back up
    // and send the messages.
    send(n, new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));
    t.ok(
        seenFullMessage,
        `Has seen message more than half the max size: ${seenFullMessage}, want true.`
    );

    t.end();
});

tap.test("Node commitPagination", async (t) => {
    const s = newTestMemoryStorage(withPeers("1"));
    const cfg = newTestConfig("1", 10, 1, s);
    cfg.maxCommittedSizePerReady = 2080;
    const rn = RawNode.NewRawNode(cfg);
    const n = Node.NewNode(rn);

    n.run();
    await n.campaign();

    let rd = await readyWithTimeout(n);
    t.equal(
        rd.committedEntries.length,
        1,
        `expected 1 (empty) entry, got ${rd.committedEntries.length}`
    );
    s.Append(rd.entries);
    await n.advance();

    const blob: string = new Array(1000 + 1).join("a"); // = "aaa...a"
    for (let i = 0; i < 3; i++) {
        const err = await n.propose(blob);
        t.equal(err, null, `got error: ${err?.message}, want null`);
    }

    // The 3 proposals will commit in two batches.
    rd = await readyWithTimeout(n);
    t.equal(
        rd.committedEntries.length,
        2,
        `expected 2 entries in first batch, got ${rd.committedEntries.length}`
    );
    s.Append(rd.entries);
    await n.advance();

    rd = await readyWithTimeout(n);
    t.equal(
        rd.committedEntries.length,
        1,
        `expected 1 entry in second batch, got ${rd.committedEntries.length}`
    );
    s.Append(rd.entries);
    await n.advance();

    t.end();
});

class IgnoreSizeHintMemStorage extends MemoryStorage {
    /**
     * Creates a new IgnoreSizeHintMemStorage that copies all fields from the
     * supplied MemoryStorage
     * @param m Object of type MemoryStorage that should get copied.
     */
    constructor(m: MemoryStorage) {
        super(m);
    }

    override Entries(lo: number, hi: number, maxSize: number): [Entry[], NullableError] {
        return super.Entries(lo, hi, noLimit);
    }
}

// TestNodeCommitPaginationAfterRestart regression tests a scenario in which the
// Storage's Entries size limitation is slightly more permissive than Raft's
// internal one. The original bug was the following:
//
// - node learns that index 11 (or 100, doesn't matter) is committed
// - nextEnts returns index 1..10 in CommittedEntries due to size limiting.
//   However, index 10 already exceeds maxBytes, due to a user-provided impl of
//   Entries.
// - Commit index gets bumped to 10
// - the node persists the HardState, but crashes before applying the entries
// - upon restart, the storage returns the same entries, but `slice` takes a
//   different code path (since it is now called with an upper bound of 10) and
//   removes the last entry.
// - Raft emits a HardState with a regressing commit index.
//
// A simpler version of this test would have the storage return a lot less
// entries than dictated by maxSize (for example, exactly one entry) after the
// restart, resulting in a larger regression. This wouldn't need to exploit
// anything about Raft-internal code paths to fail.
tap.test("Node commitPaginationAfterRestart", async (t) => {
    const s = new IgnoreSizeHintMemStorage(newTestMemoryStorage(withPeers("1")));
    const persistedHardState = new HardState({ term: 1, vote: "1", commit: 10 });

    s.hardState = persistedHardState;
    s.ents = [];
    let size = 0;
    for (let i = 0; i < 10; i++) {
        const ent = new Entry({
            term: 1,
            index: i + 1,
            type: EntryType.EntryNormal,
            data: "a",
        });

        s.ents.push(ent);
        size += JSON.stringify(ent).length;
    }

    const cfg = newTestConfig("1", 10, 1, s);
    // Set a MaxSizePerMsg that would suggest to Raft that the last committed
    // entry should not be included in the initial rd.CommittedEntries. However,
    // our storage will ignore this and *will* return it (which is how the
    // Commit index ended up being 10 initially).
    const lastEntrySize = JSON.stringify(s.ents[s.ents.length - 1]).length;
    cfg.maxSizePerMsg = size - lastEntrySize - 1;

    const rn = RawNode.NewRawNode(cfg);
    const n = Node.NewNode(rn);

    n.run();

    try {
        const rd = await readyWithTimeout(n);
        t.notOk(
            !rd.hardState.isEmptyHardState() && rd.hardState.commit < persistedHardState.commit,
            `HardState regressed: Commit ${persistedHardState.commit} -> ${rd.hardState.commit}` +
            ` Committing:\n ${describeEntries(rd.committedEntries, (data: RaftData) => {
                return String(data); // We know that data is of type string here
            })}`
        );
    } finally {
        await n.stop();
    }

    t.end();
});

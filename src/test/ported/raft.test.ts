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

import { mustTerm } from "./log.test";
import { diffu, ltoa } from "./util";
import { emptyRaftData } from "../../non-ported/raft-controller";
import { RaftData } from "../../non-ported/raft-controller";
import { newLog, RaftLog } from "../../ported/log";
import { Unstable } from "../../ported/log-unstable";
import { getLogger } from "../../ported/logger";
import { newReady, SoftState } from "../../ported/node";
import {
    ConfChange,
    ConfChangeType,
    ConfChangeV2,
    ConfState,
    Entry,
    EntryType,
    HardState,
    Message,
    MessageType,
    rid,
    ridnone,
    Snapshot,
    SnapshotMetadata
} from "../../ported/raftpb/raft.pb";
import { MemoryStorage, Storage } from "../../ported/storage";
import { ProgressStateType } from "../../ported/tracker/state";
import { NullableError, payloadSize, voteRespMsgType } from "../../ported/util";
import {
    RaftConfig,
    Raft,
    noLimit,
    errProposalDropped,
    stepFollower,
    stepCandidate,
    stepLeader,
    ReadOnlyOption,
    StateType,
} from "../../ported/raft";
import {
    newTestRaft,
    newTestMemoryStorage,
    withPeers,
    RaftForTest,
    newTestConfig,
    nopStepper,
    send,
    newNetwork,
    mustAppendEntry,
    cut,
    entsWithConfig,
    ignore,
    isolate,
    Network,
    newNetworkWithConfig,
    newTestLearnerRaft,
    nextEnts,
    preVoteConfig,
    recover,
    setRandomizedElectionTimeout,
    StateMachine,
    votedWithConfig,
    withLearners,
} from "./raft-test-util";

tap.test("raft TestProgressLeader()", (t) => {
    const r: Raft = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));

    r.becomeCandidate();
    r.becomeLeader();
    r.prs.progress.get("2")!.BecomeReplicate();

    // Send proposals to r1. The first 5 entries should be appended to the log.
    const propMsg: Message = new Message({
        from: "1",
        to: "1",
        type: MessageType.MsgProp,
        entries: [new Entry({ data: "foo" })],
    });
    for (let i = 0; i < 5; i++) {
        const pr = r.prs.progress.get(r.id)!;
        if (
            pr.state !== ProgressStateType.StateReplicate ||
            pr.match !== i + 1 ||
            pr.next !== pr.match + 1
        ) {
            t.fail(`unexpected progress ${pr}`, pr);
        }
        const err = r.Step(propMsg);
        if (err !== null) {
            t.fail(`proposal resulted in error: ${err}`);
        }
    }
    t.end();
});

// TestProgressResumeByHeartbeatResp ensures raft.heartbeat reset
// progress.paused by heartbeat response.
tap.test("raft TestProgressResumeByHeartbeatResp()", (t) => {
    const r: Raft = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();

    r.prs.progress.get("2")!.probeSent = true;

    r.Step(new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));
    if (!r.prs.progress.get("2")!.probeSent) {
        t.fail(`paused = ${r.prs.progress.get("2")!.probeSent}, want true`);
    }

    r.prs.progress.get("2")!.BecomeReplicate();
    r.Step(new Message({ from: "2", to: "1", type: MessageType.MsgHeartbeatResp }));
    if (r.prs.progress.get("2")!.probeSent) {
        t.fail(`paused = ${r.prs.progress.get("2")!.probeSent}, want false`);
    }
    t.end();
});

tap.test("raft TestProgressPaused()", (t) => {
    const r: RaftForTest = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();

    const d = "somedata";
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: d })],
        })
    );
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: d })],
        })
    );
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: d })],
        })
    );

    const msg = r.readMessages();
    if (msg.length !== 1) {
        t.fail(`len(ms) = ${msg.length}, want 1`);
    }
    t.end();
});

tap.test("raft TestProgressFlowControl()", (t) => {
    const cfg = newTestConfig("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));
    cfg.maxInflightMsgs = 3;
    // NOTE: Depends on size of JSON.stringify({ data: blob, type:
    // EntryType.EntryNormal })
    cfg.maxSizePerMsg = 2080;
    const r = new RaftForTest(cfg);
    r.becomeCandidate();
    r.becomeLeader();

    // Throw away all the messages relating to the initial election.
    r.readMessages();

    // While node 2 is in probe state, propose a bunch of entries.
    r.prs.progress.get("2")!.BecomeProbe();
    const blob: string = new Array(1000 + 1).join("a");
    for (let i = 0; i < 10; i++) {
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: blob, type: EntryType.EntryNormal })],
            })
        );
    }
    let ms = r.readMessages();

    // First append has two entries: the empty entry to confirm the election,
    // and the first proposal (only one proposal gets sent because we're in
    // probe state).
    if (ms.length !== 1 || ms[0].type !== MessageType.MsgApp) {
        t.fail(`expected 1 MsgApp, got ${ms}`);
    }

    if (ms[0].entries.length !== 2) {
        t.fail(`expected 2 entries, got ${ms[0].entries.length}`);
    }
    if (ms[0].entries[0].data !== emptyRaftData || ms[0].entries[1].data.length !== 1000) {
        t.fail(`unexpected entry sizes: ${ms[0].entries.length}`);
    }

    // When this append is acked, we change to replicate state and can send
    // multiple messages at once.
    r.Step(
        new Message({
            from: "2",
            to: "1",
            type: MessageType.MsgAppResp,
            index: ms[0].entries[1].index,
        })
    );
    ms = r.readMessages();
    if (ms.length !== 3) {
        t.fail(`expected 3 messages, got ${ms.length}`);
    }
    ms.forEach((m, i) => {
        if (m.type !== MessageType.MsgApp) {
            t.fail(`${i}: expected MsgApp, got ${m.type}`);
        }
        if (m.entries.length !== 2) {
            t.fail(`${i}: expected 2 entries, got ${m.entries.length}`);
        }
    });

    // Ack all three of those messages together and get the last two messages
    // (containing three entries).
    r.Step(
        new Message({
            from: "2",
            to: "1",
            type: MessageType.MsgAppResp,
            index: ms[2].entries[1].index,
        })
    );
    ms = r.readMessages();
    if (ms.length !== 2) {
        t.fail(`expected 2 messages, got ${ms.length}`);
    }
    ms.forEach((m, i) => {
        if (m.type !== MessageType.MsgApp) {
            t.fail(`${i}: expected MsgApp, got ${m.type}`);
        }
    });

    if (ms[0].entries.length !== 2) {
        t.fail(`${0}: expected 2 entries, got ${ms[0].entries.length}`);
    }
    if (ms[1].entries.length !== 1) {
        t.fail(`${1}: expected 1 entry, got ${ms[1].entries.length}`);
    }
    t.end();
});

tap.test("raft TestUncommittedEntryLimit()", (t) => {
    // Use a relatively large number of entries here to prevent regression of a
    // bug which computed the size before it was fixed. This test would fail
    // with the bug, either because we'd get dropped proposals earlier than we
    // expect them, or because the final tally ends up nonzero. (At the time of
    // writing, the former).
    const d = "testdata";

    const maxEntries = 1024;
    const maxEntrySize =
        maxEntries * payloadSize(new Entry({ data: d, type: EntryType.EntryNormal }));

    let n = payloadSize(new Entry());
    if (n !== 0) {
        t.fail("entry with no Data must have zero payload size");
    }

    const cfg = newTestConfig("1", 5, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    cfg.maxUncommittedEntriesSize = maxEntrySize;
    cfg.maxInflightMsgs = 2 * 1024; // avoid interference

    const r = new RaftForTest(cfg);
    r.becomeCandidate();
    r.becomeLeader();

    n = r.uncommittedSize;
    if (n !== 0) {
        t.fail(`expected zero uncommitted size, got ${n} bytes`);
    }

    // Set the two followers to the replicate state. Commit to tail of log.
    const numFollowers = 2;
    r.prs.progress.get("2")!.BecomeReplicate();
    r.prs.progress.get("3")!.BecomeReplicate();
    r.uncommittedSize = 0;

    // Send proposals to r1. The first 5 entries should be appended to the log.
    const propMsg: Message = new Message({
        from: "1",
        to: "1",
        type: MessageType.MsgProp,
        entries: [new Entry({ data: d, type: EntryType.EntryNormal })],
    });
    let propEnts: Entry[] = [];
    for (let i = 0; i < maxEntries; i++) {
        const myErr = r.Step(propMsg);
        if (myErr !== null) {
            t.fail(`proposal resulted in error: ${myErr}`);
        }
        propEnts[i] = new Entry({ data: d, type: EntryType.EntryNormal });
    }

    // Send one more proposal to r1. It should be rejected.
    let err = r.Step(propMsg);
    if (err !== errProposalDropped) {
        t.fail(`proposal not dropped: ${err}`);
    }

    // Read messages and reduce the uncommitted size as if we had committed
    // these entries.
    let ms = r.readMessages();
    let e = maxEntries * numFollowers;
    if (ms.length !== e) {
        t.fail(`expected ${e} messages, got ${ms.length}`);
    }
    r.reduceUncommittedSize(propEnts);
    if (r.uncommittedSize !== 0) {
        t.fail(`committed everything, but still tracking ${r.uncommittedSize}`);
    }

    // Send a single large proposal to r1. Should be accepted even though it
    // pushes us above the limit because we were beneath it before the proposal.
    propEnts = [];
    for (let i = 0; i < 2 * maxEntries; i++) {
        propEnts[i] = new Entry({ data: d, type: EntryType.EntryNormal });
    }

    const propMsgLarge: Message = new Message({
        from: "1",
        to: "1",
        type: MessageType.MsgProp,
        entries: propEnts,
    });
    err = r.Step(propMsgLarge);
    if (err !== null) {
        t.fail(`proposal resulted in error: ${err}`);
    }

    // Send one more proposal to r1. It should be rejected, again.
    err = r.Step(propMsg);

    if (err !== errProposalDropped) {
        t.fail(`proposal not dropped: ${err}`);
    }

    // But we can always append an entry with no Data. This is used both for the
    // leader's first empty entry and for auto-transitioning out of joint config
    // states.
    err = r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );
    if (err !== null) {
        t.fail(`${err}`);
    }

    // Read messages and reduce the uncommitted size as if we had committed
    // these entries.
    ms = r.readMessages();
    e = 2 * numFollowers;
    if (ms.length !== e) {
        t.fail(`expected ${e} messages, got ${ms.length}`);
    }
    r.reduceUncommittedSize(propEnts);
    n = r.uncommittedSize;
    if (n !== 0) {
        t.fail(`expected zero uncommitted size, got ${n}`);
    }
    t.end();
});

tap.test("raft testLeaderElection", (t) => {
    function testLeaderElection(preVote: boolean) {
        let cfg: ((arg0: RaftConfig) => void) | null = null;
        let candState = StateType.StateCandidate;
        let candTerm = 1;

        if (preVote) {
            cfg = preVoteConfig;
            // In pre-vote mode, an election that fails to complete leaves the
            // node in pre-candidate state without advancing the term.
            candState = StateType.StatePreCandidate;
            candTerm = 0;
        }

        const tests: {
            network: Network;
            state: StateType;
            expTerm: number;
        }[] = [
                {
                    network: newNetworkWithConfig(cfg, null, null, null),
                    state: StateType.StateLeader,
                    expTerm: 1,
                },
                {
                    network: newNetworkWithConfig(cfg, null, null, nopStepper),
                    state: StateType.StateLeader,
                    expTerm: 1,
                },
                {
                    network: newNetworkWithConfig(cfg, null, nopStepper, nopStepper),
                    state: candState,
                    expTerm: candTerm,
                },
                {
                    network: newNetworkWithConfig(cfg, null, nopStepper, nopStepper, null),
                    state: candState,
                    expTerm: candTerm,
                },
                {
                    network: newNetworkWithConfig(cfg, null, nopStepper, nopStepper, null, null),
                    state: StateType.StateLeader,
                    expTerm: 1,
                },

                // three logs further along than 0, but in the same term so
                // rejections are returned instead of the votes being ignored.
                {
                    network: newNetworkWithConfig(
                        cfg,
                        null,
                        entsWithConfig(cfg, 1),
                        entsWithConfig(cfg, 1),
                        entsWithConfig(cfg, 1, 1),
                        null
                    ),
                    state: StateType.StateFollower,
                    expTerm: 1,
                },
            ];

        for (let i = 0; i < tests.length; i++) {
            const tt = tests[i];

            send(tt.network, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
            const sm = tt.network.peers.get("1") as RaftForTest;

            t.equal(
                sm.state,
                tt.state,
                `#${i}: state = ${StateType[sm.state]}, want ${StateType[tt.state]}`
            );
            const g = sm.term;
            t.equal(g, tt.expTerm, `#${i}: term = ${g}, want ${tt.expTerm}`);
        }
    }

    testLeaderElection(false);
    testLeaderElection(true);
    t.end();
});

// TestLearnerElectionTimeout verifies that the leader should not start election
// even when times out.
tap.test("raft testLearnerElectionTimeout", (t) => {
    const n1 = newTestLearnerRaft("1", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));
    const n2 = newTestLearnerRaft("2", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);

    // n2 is learner. Learner should not start election even when times out.
    setRandomizedElectionTimeout(n2, n2.electionTimeout);
    for (let i = 0; i < n2.electionTimeout; i++) {
        n2.tick();
    }

    t.equal(
        n2.state,
        StateType.StateFollower,
        `peer 2 state: ${n2.state}, want ${StateType.StateFollower}`
    );

    t.end();
});

// TestLearnerPromotion verifies that the learner should not election until it
// is promoted to a normal peer.
tap.test("raft testLearnerPromotion", (t) => {
    const n1 = newTestLearnerRaft("1", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));
    const n2 = newTestLearnerRaft("2", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);

    const network: Network = newNetwork(n1, n2);

    t.notOk(n1.state === StateType.StateLeader, `peer 1 state is leader, want not`);

    // n1 should become leader
    setRandomizedElectionTimeout(n1, n1.electionTimeout);
    for (let i = 0; i < n1.electionTimeout; i++) {
        n1.tick();
    }

    t.equal(
        n1.state,
        StateType.StateLeader,
        `peer 1 state: ${n1.state}, want ${StateType.StateLeader}`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `peer 2 state: ${n2.state}, want ${StateType.StateFollower}`
    );

    send(network, new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));

    n1.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "2",
        }).asV2()
    );
    n2.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "2",
        }).asV2()
    );

    t.notOk(n2.isLearner, `peer 2 is learner, want not`);

    // n2 start election, should become leader
    setRandomizedElectionTimeout(n2, n2.electionTimeout);
    for (let i = 0; i < n2.electionTimeout; i++) {
        n2.tick();
    }

    send(network, new Message({ from: "2", to: "2", type: MessageType.MsgBeat }));

    t.equal(
        n1.state,
        StateType.StateFollower,
        `peer 1 state: ${n1.state}, want ${StateType.StateFollower}`
    );
    t.equal(
        n2.state,
        StateType.StateLeader,
        `peer 2 state: ${n2.state}, want ${StateType.StateLeader}`
    );

    t.end();
});

// TestLearnerCanVote checks that a learner can vote when it receives a valid
// Vote request. See (*raft).Step for why this is necessary and correct
// behavior.
tap.test("raft testLearnerCanVote", (t) => {
    const n2 = newTestLearnerRaft("2", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));

    n2.becomeFollower(1, ridnone);

    n2.Step(
        new Message({
            from: "1",
            to: "2",
            term: 2,
            type: MessageType.MsgVote,
            logTerm: 11,
            index: 11,
        })
    );

    t.equal(n2.msgs.length, 1, `expected exactly one message, not ${JSON.stringify(n2.msgs)}`);

    const msg = n2.msgs[0];
    t.notOk(
        msg.type !== MessageType.MsgVoteResp && !msg.reject,
        `expected learner to not reject vote`
    );

    t.end();
});

// testLeaderCycle verifies that each node in a cluster can campaign and be
// elected in turn. This ensures that elections (including pre-vote) work when
// not starting from a clean slate (as they do in TestLeaderElection)
tap.test("raft testLeaderCycle", (t) => {
    function testLeaderCycle(preVote: boolean) {
        let cfg: (arg0: RaftConfig) => void = () => {
            // Empty
        };
        if (preVote) {
            cfg = preVoteConfig;
        }

        const n = newNetworkWithConfig(cfg, null, null, null);
        for (let campaignerID = 1; campaignerID <= 3; campaignerID++) {
            send(
                n,
                new Message({
                    from: campaignerID.toString(),
                    to: campaignerID.toString(),
                    type: MessageType.MsgHup,
                })
            );

            for (const [_, peer] of n.peers) {
                const sm = peer as RaftForTest;
                t.notOk(
                    sm.id === campaignerID.toString() && sm.state !== StateType.StateLeader,
                    `preVote:${preVote}: campaigning node ${sm.id} state = ${sm.state}, want StateLeader`
                );
                t.notOk(
                    sm.id !== campaignerID.toString() && sm.state !== StateType.StateFollower,
                    `preVote=${preVote}: after campaign of node ${campaignerID},` +
                    ` node ${sm.id} had state = ${sm.state}, want StateFollower`
                );
            }
        }
    }

    testLeaderCycle(false);
    testLeaderCycle(true);

    t.end();
});

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term) log
// entries, and must overwrite higher-term log entries with lower-term ones.
tap.test("raft testLeaderElectionOverwriteNewerLogs", (t) => {
    function testLeaderElectionOverwriteNewerLogs(preVote: boolean) {
        let cfg: ((arg0: RaftConfig) => void) | null = null;
        if (preVote) {
            cfg = preVoteConfig;
        }

        // This network represents the results of the following sequence of
        // events:
        // - Node 1 won the election in term 1.
        // - Node 1 replicated a log entry to node 2 but died before sending it
        //   to other nodes.
        // - Node 3 won the second election in term 2.
        // - Node 3 wrote an entry to its logs but died without sending it to
        //   any other nodes.
        //
        // At this point, nodes 1, 2, and 3 all have uncommitted entries in
        // their logs and could win an election at term 3. The winner's log
        // entry overwrites the losers'. (TestLeaderSyncFollowerLog tests the
        // case where older log entries are overwritten, so this test focuses on
        // the case where the newer entries are lost).
        const n = newNetworkWithConfig(
            cfg,
            entsWithConfig(cfg, 1), // Node 1: Won first election
            entsWithConfig(cfg, 1), // Node 2: Got logs from node 1
            entsWithConfig(cfg, 2), // Node 3: Won second election
            votedWithConfig(cfg, "3", 2), // Node 4: Voted but didn't get logs
            votedWithConfig(cfg, "3", 2) // Node 5: Voted but didn't get logs
        );

        // Node 1 campaigns. The election fails because a quorum of nodes know
        // about the election that already happened at term 2. Node 1's term is
        // pushed ahead to 2.
        send(n, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        const sm1 = n.peers.get("1") as RaftForTest;

        t.equal(sm1.state, StateType.StateFollower, `state = ${sm1.state}, want StateFollower`);
        t.equal(sm1.term, 2, `term = ${sm1.term}, want 2`);

        // Node 1 campaigns again with a higher term. This time it succeeds.
        send(n, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

        t.equal(sm1.state, StateType.StateLeader, `state = ${sm1.state}, want StateLeader`);
        t.equal(sm1.term, 3, `term = ${sm1.term}, want 3`);

        // Now all nodes agree on a log entry with term 1 at index 1 (and term 3
        // at index 2).
        for (const i of n.peers.keys()) {
            const sm = n.peers.get(i) as RaftForTest;
            const entries = sm.raftLog.allEntries();

            t.equal(entries.length, 2, `node ${i}: entries.length === ${entries.length}, want 2`);
            t.equal(
                entries[0].term,
                1,
                `node ${i}: term at index 1 === ${entries[0].term}, want 1`
            );
            t.equal(
                entries[1].term,
                3,
                `node ${i}: term at index 2 === ${entries[1].term}, want 3`
            );
        }
    }

    testLeaderElectionOverwriteNewerLogs(false);
    testLeaderElectionOverwriteNewerLogs(true);

    t.end();
});

tap.test("raft testVoteFromAnyState", (t) => {
    function testVoteFromAnyState(vt: MessageType) {
        // iterate through all possible states in StateTypeRaft
        for (let st = 0; st < StateType.numStates; st++) {
            const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
            r.term = 1;

            switch (st) {
                case StateType.StateFollower:
                    r.becomeFollower(r.term, "3");
                    break;
                case StateType.StatePreCandidate:
                    r.becomePreCandidate();
                    break;
                case StateType.StateCandidate:
                    r.becomeCandidate();
                    break;
                case StateType.StateLeader:
                    r.becomeCandidate();
                    r.becomeLeader();
                    break;
            }

            // Note that setting our state above may have advanced r.Term past
            // its initial value.
            const origTerm = r.term;
            const newTerm = r.term + 1;

            const msg: Message = new Message({
                from: "2",
                to: "1",
                type: vt,
                term: newTerm,
                logTerm: newTerm,
                index: 42,
            });

            const err = r.Step(msg);

            t.equal(err, null, `${vt},${st}: Error is: ${err?.message}, want null`);
            t.equal(
                r.msgs.length,
                1,
                `${vt},${st}: ${r.msgs.length} messages, want 1: ${JSON.stringify(r.msgs)}`
            );

            const resp = r.msgs[0];
            t.equal(
                resp?.type,
                voteRespMsgType(vt),
                `${vt},${st}: response message type is ${resp?.type}, want ${voteRespMsgType(vt)}`
            );
            t.notOk(resp?.reject, `${vt},${st}: rejection is: ${resp?.reject}, want false`);

            // If this was a real vote, we reset our state and term.
            if (vt === MessageType.MsgVote) {
                t.equal(
                    r.state,
                    StateType.StateFollower,
                    `${vt},${st}: state ${r.state}, want ${StateType.StateFollower}`
                );
                t.equal(r.term, newTerm, `${vt},${st}:term ${r.term}, want ${newTerm}`);
                t.equal(r.vote, "2", `${vt},${st}: vote ${r.vote}, want 2`);
            } else {
                // In a prevote, nothing changes.
                t.equal(r.state, st, `${vt},${st}: state ${r.state}, want ${st}`);
                t.equal(r.term, origTerm, `${vt},${st}: term ${r.term}, want ${origTerm}`);
                // if st == StateFollower or StatePreCandidate, r hasn't voted
                // yet. In StateCandidate or StateLeader, it's voted for itself.
                t.notOk(
                    r.vote !== ridnone && r.vote !== "1",
                    `${vt},${st}: vote ${r.vote}, want ${ridnone} or 1`
                );
            }
        }
    }

    testVoteFromAnyState(MessageType.MsgVote);
    testVoteFromAnyState(MessageType.MsgPreVote);

    t.end();
});

tap.test("raft testLogReplication", (t) => {
    const tests: { network: Network; msgs: Message[]; wCommitted: number }[] = [
        {
            network: newNetwork(null, null, null),
            msgs: [
                new Message({
                    from: "1",
                    to: "1",
                    type: MessageType.MsgProp,
                    entries: [new Entry({ data: "somedata" })],
                }),
            ],
            wCommitted: 2,
        },
        {
            network: newNetwork(null, null, null),
            msgs: [
                new Message({
                    from: "1",
                    to: "1",
                    type: MessageType.MsgProp,
                    entries: [new Entry({ data: "somedata" })],
                }),
                new Message({ from: "1", to: "2", type: MessageType.MsgHup }),
                new Message({
                    from: "1",
                    to: "2",
                    type: MessageType.MsgProp,
                    entries: [new Entry({ data: "somedata" })],
                }),
            ],
            wCommitted: 4,
        },
    ];

    tests.forEach((tt, i) => {
        send(tt.network, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

        for (const message of tt.msgs) {
            send(tt.network, message);
        }

        for (const [j, x] of tt.network.peers) {
            const sm = x as RaftForTest;

            t.equal(
                sm.raftLog.committed,
                tt.wCommitted,
                `#${i}.${j}: committed = ${sm.raftLog.committed}, want ${tt.wCommitted}`
            );

            const ents: Entry[] = [];
            for (const e of nextEnts(sm, tt.network.storage.get(j)!)) {
                if (e.data !== emptyRaftData) {
                    ents.push(e);
                }
            }

            const props: Message[] = [];
            for (const m of tt.msgs) {
                if (m.type === MessageType.MsgProp) {
                    props.push(m);
                }
            }
            for (let k = 0; k < props.length; k++) {
                const m = props[k];

                t.equal(
                    ents[k].data,
                    m.entries[0].data,
                    `#${i}.${j}: data = ${ents[k].data}, want ${m.entries[0].data}`
                );
            }
        }
    });

    t.end();
});

// TestLearnerLogReplication tests that a learner can receive entries from the
// leader.
tap.test("raft testLearnerLogReplication", (t) => {
    const n1 = newTestLearnerRaft("1", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));
    const n2 = newTestLearnerRaft("2", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));

    const nt = newNetwork(n1, n2);

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);

    setRandomizedElectionTimeout(n1, n1.electionTimeout);
    for (let i = 0; i < n1.electionTimeout; i++) {
        n1.tick();
    }

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));

    // n1 is leader and n2 is learner
    t.equal(
        n1.state,
        StateType.StateLeader,
        `peer 1 state: ${n1.state}, want ${StateType.StateLeader}`
    );
    t.ok(n2.isLearner, `peer 2 state: not learner, want yes`);

    const nextCommitted = n1.raftLog.committed + 1;
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "somedata" })],
        })
    );

    t.equal(
        n1.raftLog.committed,
        nextCommitted,
        `peer 1 wants committed to ${nextCommitted}, but still ${n1.raftLog.committed}`
    );
    t.equal(
        n1.raftLog.committed,
        n2.raftLog.committed,
        `peer 2 wants committed to ${n1.raftLog.committed}, but still ${n2.raftLog.committed}`
    );

    const match = n1.prs.progress.get("2")!.match;
    t.equal(
        match,
        n2.raftLog.committed,
        `progress 2 of leader 1 wants match ${n2.raftLog.committed}, but got ${match}`
    );
    t.end();
});

tap.test("raft testSingleNodeCommit", (t) => {
    const tt = newNetwork(null);
    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    send(
        tt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );
    send(
        tt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );

    const sm = tt.peers.get("1") as RaftForTest;

    t.equal(sm.raftLog.committed, 3, `committed = ${sm.raftLog.committed}, want ${3}`);

    t.end();
});

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
tap.test("raft TestCannotCommitWithoutNewTermEntry", (t) => {
    const tt = newNetwork(null, null, null, null, null);
    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // 0 cannot reach 2,3,4
    cut(tt, "1", "3");
    cut(tt, "1", "4");
    cut(tt, "1", "5");

    send(
        tt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );
    send(
        tt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );

    let sm = tt.peers.get("1") as RaftForTest;

    t.equal(sm.raftLog.committed, 1, `committed = ${sm.raftLog.committed}, want ${1}`);

    // network recovery
    recover(tt);
    // avoid committing ChangeTerm proposal
    ignore(tt, MessageType.MsgApp);

    // elect 2 as the new leader with term 2
    send(tt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    // no log entries from previous term should be committed
    sm = tt.peers.get("2") as RaftForTest;

    t.equal(sm.raftLog.committed, 1, `committed = ${sm.raftLog.committed}, want ${1}`);

    recover(tt);
    // send heartbeat; reset wait
    send(tt, new Message({ from: "2", to: "2", type: MessageType.MsgBeat }));
    // append an entry at current term
    send(
        tt,
        new Message({
            from: "2",
            to: "2",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );
    // expect the committed to be advanced
    t.equal(sm.raftLog.committed, 5, `committed = ${sm.raftLog.committed}, want ${5}`);

    t.end();
});

// TestCommitWithoutNewTermEntry tests the entries could be committed when
// leader changes, no new proposal comes in.
tap.test("raft testCommitWithoutNewTermEntry", (t) => {
    const tt = newNetwork(null, null, null, null, null);
    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // 0 cannot reach 2,3,4
    cut(tt, "1", "3");
    cut(tt, "1", "4");
    cut(tt, "1", "5");

    send(
        tt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );
    send(
        tt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );

    const sm = tt.peers.get("1") as RaftForTest;
    t.equal(sm.raftLog.committed, 1, `committed = ${sm.raftLog.committed}, want ${1}`);

    // network recovery
    recover(tt);

    // elect 2 as the new leader with term 2 after append a ChangeTerm entry
    // from the current term, all entries should be committed
    send(tt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    t.equal(sm.raftLog.committed, 4, `committed = ${sm.raftLog.committed}, want ${4}`);

    t.end();
});

tap.test("raft testDuelingCandidates", (t) => {
    const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const b = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const c = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    const nt = newNetwork(a, b, c);
    cut(nt, "1", "3");

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    // 1 becomes leader since it receives votes from 1 and 2
    let sm = nt.peers.get("1") as RaftForTest;

    t.equal(sm.state, StateType.StateLeader, `state = ${sm.state}, want ${StateType.StateLeader}`);

    // 3 stays as candidate since it receives a vote from 3 and a rejection from
    // 2
    sm = nt.peers.get("3") as RaftForTest;

    t.equal(
        sm.state,
        StateType.StateCandidate,
        `state = ${sm.state}, want ${StateType.StateCandidate}`
    );

    recover(nt);

    // candidate 3 now increases its term and tries to vote again we expect it
    // to disrupt the leader 1 since it has a higher term 3 will be follower
    // again since both 1 and 2 rejects its vote request since 3 does not have a
    // long enough log
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    const wlog = new RaftLog({
        storage: new MemoryStorage({
            ents: [new Entry(), new Entry({ term: 1, index: 1 })],
        }),
        committed: 1,
        unstable: new Unstable({ offset: 2 }),
    });

    const tests: {
        sm: RaftForTest;
        state: StateType;
        term: number;
        raftLog: RaftLog;
    }[] = [
            { sm: a, state: StateType.StateFollower, term: 2, raftLog: wlog },
            { sm: b, state: StateType.StateFollower, term: 2, raftLog: wlog },
            {
                sm: c,
                state: StateType.StateFollower,
                term: 2,
                raftLog: newLog(MemoryStorage.NewMemoryStorage(), getLogger()),
            },
        ];

    tests.forEach((tt, i) => {
        let g = tt.sm.state;
        t.equal(g, tt.state, `#${i}: state = ${g}, want ${tt.state}`);

        g = tt.sm.term;
        t.equal(g, tt.term, `#${i}: term = ${g}, want ${tt.term}`);

        const base = ltoa(tt.raftLog);

        if (nt.peers.has((1 + i).toString())) {
            sm = nt.peers.get((1 + i).toString()) as RaftForTest;
            const l = ltoa(sm.raftLog);
            const diffBaseL = diffu(base, l);

            t.equal(diffBaseL, "", `#${i}: diff:\n${diffBaseL}`);
        } else {
            console.log(`#${i}: empty log`);
        }
    });

    t.end();
});

tap.test("raft testDuelingPreCandidates", (t) => {
    const cfgA = newTestConfig("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const cfgB = newTestConfig("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const cfgC = newTestConfig("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    cfgA.preVote = true;
    cfgB.preVote = true;
    cfgC.preVote = true;
    const a = new RaftForTest(cfgA);
    const b = new RaftForTest(cfgB);
    const c = new RaftForTest(cfgC);

    const nt = newNetwork(a, b, c);
    cut(nt, "1", "3");

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    // 1 becomes leader since it receives votes from 1 and 2
    let sm = nt.peers.get("1") as RaftForTest;

    t.equal(sm.state, StateType.StateLeader, `state = ${sm.state}, want ${StateType.StateLeader}`);

    // 3 campaigns then reverts to follower when its PreVote is rejected
    sm = nt.peers.get("3") as RaftForTest;

    t.equal(
        sm.state,
        StateType.StateFollower,
        `state = ${sm.state}, want ${StateType.StateFollower}`
    );

    recover(nt);

    // Candidate 3 now increases its term and tries to vote again. With PreVote,
    // it does not disrupt the leader.
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    const wlog = new RaftLog({
        storage: new MemoryStorage({
            ents: [new Entry(), new Entry({ term: 1, index: 1 })],
        }),
        committed: 1,
        unstable: new Unstable({ offset: 2 }),
    });

    const tests: {
        sm: RaftForTest;
        state: StateType;
        term: number;
        raftLog: RaftLog;
    }[] = [
            { sm: a, state: StateType.StateLeader, term: 1, raftLog: wlog },
            { sm: b, state: StateType.StateFollower, term: 1, raftLog: wlog },
            {
                sm: c,
                state: StateType.StateFollower,
                term: 1,
                raftLog: newLog(MemoryStorage.NewMemoryStorage(), getLogger()),
            },
        ];

    tests.forEach((tt, i) => {
        let g = tt.sm.state;
        t.equal(g, tt.state, `#${i}: state = ${g}, want ${tt.state}`);

        g = tt.sm.term;
        t.equal(g, tt.term, `#${i}: term = ${g}, want ${tt.term}`);

        const base = ltoa(tt.raftLog);
        if (nt.peers.has((i + 1).toString())) {
            sm = nt.peers.get((i + 1).toString()) as RaftForTest;
            const l = ltoa(sm.raftLog);
            const diffBaseL = diffu(base, l);

            t.equal(diffBaseL, "", `#${i}: diff:\n${diffBaseL}`);
        } else {
            console.log(`#${i}: empty log`);
        }
    });

    t.end();
});

tap.test("raft testCandidateConcede", (t) => {
    const tt = newNetwork(null, null, null);
    isolate(tt, "1");

    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    send(tt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    // heal the partition
    recover(tt);
    // send heartbeat; reset wait
    send(tt, new Message({ from: "3", to: "3", type: MessageType.MsgBeat }));

    const data = "force follower";
    // send a proposal to 3 to flush out a MsgApp to 1
    send(
        tt,
        new Message({
            from: "3",
            to: "3",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: data })],
        })
    );
    // send heartbeat; flush out commit
    send(tt, new Message({ from: "3", to: "3", type: MessageType.MsgBeat }));

    const a = tt.peers.get("1") as RaftForTest;

    let g = a.state;
    t.equal(g, StateType.StateFollower, `state = ${g}, want ${StateType.StateFollower}`);

    g = a.term;
    t.equal(g, 1, `term = ${g}, want ${1}`);

    const ltoaLog = new RaftLog({
        storage: new MemoryStorage({
            ents: [
                new Entry(),
                new Entry({ term: 1, index: 1 }),
                new Entry({ term: 1, index: 2, data: data }),
            ],
        }),
        unstable: new Unstable({ offset: 3 }),
        committed: 2,
    });

    const wantLog = ltoa(ltoaLog);

    tt.peers.forEach((p, i) => {
        if (p && p.constructor === RaftForTest) {
            const sm = p as RaftForTest;
            const l = ltoa(sm.raftLog);
            const diffWantL = diffu(wantLog, l);

            t.equal(diffWantL, "", `#${i}: diff:\n${diffWantL}`);
        } else {
            console.log(`#${i}: empty log`);
        }
    });

    t.end();
});

tap.test("raft testSingleNodeCandidate", (t) => {
    const tt = newNetwork(null);
    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const sm = tt.peers.get("1") as RaftForTest;

    t.equal(
        sm.state,
        StateType.StateLeader,
        `state = ${StateType[sm.state]}, want ${StateType[StateType.StateLeader]}`
    );

    t.end();
});

tap.test("raft testSingleNodePreCandidate", (t) => {
    const tt = newNetworkWithConfig(preVoteConfig, null);
    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const sm = tt.peers.get("1") as RaftForTest;

    t.equal(
        sm.state,
        StateType.StateLeader,
        `state = ${StateType[sm.state]}, want ${StateType[StateType.StateLeader]}`
    );

    t.end();
});

tap.test("raft testOldMessages", (t) => {
    const tt = newNetwork(null, null, null);
    // make 0 leader @ term 3
    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    send(tt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));
    send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    // pretend we're an old leader trying to make progress; this entry is
    // expected to be ignored.
    send(
        tt,
        new Message({
            from: "2",
            to: "1",
            type: MessageType.MsgApp,
            term: 2,
            entries: [new Entry({ index: 3, term: 2 })],
        })
    );
    // commit a new entry
    send(
        tt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "somedata" })],
        })
    );

    const ilog = new RaftLog({
        storage: new MemoryStorage({
            ents: [
                new Entry(),
                new Entry({ term: 1, index: 1 }),
                new Entry({ term: 2, index: 2 }),
                new Entry({ term: 3, index: 3 }),
                new Entry({ data: "somedata", term: 3, index: 4 }),
            ],
        }),
        unstable: new Unstable({ offset: 5 }),
        committed: 4,
    });

    const base = ltoa(ilog);

    for (const [i, peer] of tt.peers) {
        if (peer && peer.constructor === RaftForTest) {
            const sm = peer as RaftForTest;
            const l = ltoa(sm.raftLog);
            const g = diffu(base, l);

            t.equal(g, "", `#${i}: diff:\n${g}`);
        } else {
            console.log(`#${i}: empty log`);
        }
    }
    t.end();
});

// TestOldMessagesReply - optimization - reply with new term.

tap.test("raft TestProposal", (t) => {
    const tests: {
        network: Network;
        success: boolean;
    }[] = [
            { network: newNetwork(null, null, null), success: true },
            { network: newNetwork(null, null, nopStepper), success: true },
            { network: newNetwork(null, nopStepper, nopStepper), success: false },
            {
                network: newNetwork(null, nopStepper, nopStepper, null),
                success: false,
            },
            {
                network: newNetwork(null, nopStepper, nopStepper, null, null),
                success: true,
            },
        ];

    tests.forEach((tt, j) => {
        function localSend(m: Message) {
            try {
                send(tt.network, m);
            } catch (error) {
                // only recover if we expect it to panic (success==false)
                if (!tt.success) {
                    if (error) {
                        console.log(`#${j}: err: ${error.message}`);
                    }
                } else {
                    // If we do not expect the above send call to throw, do not
                    // catch the error
                    throw error;
                }
            }
        }

        const data = "somedata";

        // promote 1 to become leader
        localSend(new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        localSend(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: data })],
            })
        );

        let wantLog = newLog(MemoryStorage.NewMemoryStorage(), getLogger());
        if (tt.success) {
            wantLog = new RaftLog({
                storage: new MemoryStorage({
                    ents: [
                        new Entry(),
                        new Entry({ term: 1, index: 1 }),
                        new Entry({ term: 1, index: 2, data: data }),
                    ],
                }),
                unstable: new Unstable({ offset: 3 }),
                committed: 2,
            });
        }

        const base = ltoa(wantLog);
        for (const [i, peer] of tt.network.peers) {
            if (peer && peer.constructor === RaftForTest) {
                const peerAsRaft = peer as RaftForTest;
                const l = ltoa(peerAsRaft.raftLog);
                const g = diffu(base, l);

                t.equal(g, "", `#${j}: peer ${i} diff:\n${g}`);
            } else {
                console.log(`${i}: peer ${j} empty log`);
            }
        }

        const sm = tt.network.peers.get("1") as RaftForTest;
        const smTerm = sm.term;
        t.equal(smTerm, 1, `#${j}: term = ${smTerm}, want ${1}`);
    });

    t.end();
});

tap.test("raft testProposalByProxy", (t) => {
    const data = "somedata";
    const tests: Network[] = [newNetwork(null, null, null), newNetwork(null, null, nopStepper)];

    tests.forEach((tt, j) => {
        // promote 0 the leader
        send(tt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

        // propose via follower
        send(
            tt,
            new Message({
                from: "2",
                to: "2",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: data })],
            })
        );

        const wantLog = new RaftLog({
            storage: new MemoryStorage({
                ents: [
                    new Entry(),
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ data: data, term: 1, index: 2 }),
                ],
            }),
            unstable: new Unstable({ offset: 3 }),
            committed: 2,
        });

        const base = ltoa(wantLog);
        for (const [i, p] of tt.peers) {
            if (p && p.constructor === RaftForTest) {
                const pAsRaft = p as RaftForTest;
                const l = ltoa(pAsRaft.raftLog);
                const diff = diffu(base, l);

                t.equal(diff, "", `#${j}: peer ${i} diff:\n${diff}`);
            } else {
                console.log(`${i}: peer ${j} empty log`);
            }
        }

        const sm = tt.peers.get("1") as RaftForTest;
        const g = sm.term;

        t.equal(g, 1, `#${j}: term = ${g}, want ${1}`);
    });

    t.end();
});

tap.test("raft testCommit", (t) => {
    const tests: {
        matches: number[];
        logs: Entry[];
        smTerm: number;
        w: number;
    }[] = [
            // single
            {
                matches: [1],
                logs: [new Entry({ index: 1, term: 1 })],
                smTerm: 1,
                w: 1,
            },
            {
                matches: [1],
                logs: [new Entry({ index: 1, term: 1 })],
                smTerm: 2,
                w: 0,
            },
            {
                matches: [2],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
                smTerm: 2,
                w: 2,
            },
            {
                matches: [1],
                logs: [new Entry({ index: 1, term: 2 })],
                smTerm: 2,
                w: 1,
            },

            // odd
            {
                matches: [2, 1, 1],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
                smTerm: 1,
                w: 1,
            },
            {
                matches: [2, 1, 1],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 1 })],
                smTerm: 2,
                w: 0,
            },
            {
                matches: [2, 1, 2],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
                smTerm: 2,
                w: 2,
            },
            {
                matches: [2, 1, 2],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 1 })],
                smTerm: 2,
                w: 0,
            },

            // even
            {
                matches: [2, 1, 1, 1],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
                smTerm: 1,
                w: 1,
            },
            {
                matches: [2, 1, 1, 1],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 1 })],
                smTerm: 2,
                w: 0,
            },
            {
                matches: [2, 1, 1, 2],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
                smTerm: 1,
                w: 1,
            },
            {
                matches: [2, 1, 1, 2],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 1 })],
                smTerm: 2,
                w: 0,
            },
            {
                matches: [2, 1, 2, 2],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })],
                smTerm: 2,
                w: 2,
            },
            {
                matches: [2, 1, 2, 2],
                logs: [new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 1 })],
                smTerm: 2,
                w: 0,
            },
        ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1"));
        storage.Append(tt.logs);
        storage.hardState = new HardState({ term: tt.smTerm });

        const sm = newTestRaft("1", 10, 2, storage);
        for (let j = 0; j < tt.matches.length; j++) {
            const id = j + 1;
            if (id > 1) {
                sm.applyConfChange(
                    new ConfChange({
                        type: ConfChangeType.ConfChangeAddNode,
                        nodeId: id.toString(),
                    }).asV2()
                );
            }

            const pr = sm.prs.progress.get(id.toString())!;
            pr.match = tt.matches[j];
            pr.next = tt.matches[j] + 1;
        }

        sm.maybeCommit();
        const g = sm.raftLog.committed;

        t.equal(g, tt.w, `#${i}: committed = ${g}, want ${tt.w}`);
    });

    t.end();
});

tap.test("raft testPastElectionTimeout", (t) => {
    const tests: {
        elapse: number;
        wprobability: number;
        round: boolean;
    }[] = [
            { elapse: 5, wprobability: 0, round: false },
            { elapse: 10, wprobability: 0.1, round: true },
            { elapse: 13, wprobability: 0.4, round: true },
            { elapse: 15, wprobability: 0.6, round: true },
            { elapse: 18, wprobability: 0.9, round: true },
            { elapse: 20, wprobability: 1, round: false },
        ];

    tests.forEach((tt, i) => {
        const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
        sm.electionElapsed = tt.elapse;
        let c = 0;

        for (let j = 0; j < 10000; j++) {
            sm.resetRandomizedElectionTimeout();
            if (sm.pastElectionTimeout()) {
                c++;
            }
        }

        let got = c / 10000;
        if (tt.round) {
            got = Math.floor(got * 10 + 0.5) / 10;
        }

        t.equal(got, tt.wprobability, `#${i}: probability = ${got}, want ${tt.wprobability}`);
    });

    t.end();
});

// ensure that the Step function ignores the message from old term and does not
// pass it to the actual stepX function.
tap.test("raft testStepIgnoreOldTermMsg", (t) => {
    let called = false;
    function fakeStep(r: Raft, m: Message): NullableError {
        called = true;
        return null;
    }

    const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
    sm.step = fakeStep;
    sm.term = 2;
    sm.Step(new Message({ type: MessageType.MsgApp, term: sm.term - 1 }));

    t.notOk(called, `stepFunc called = ${called}, want ${false}`);

    t.end();
});

// TestHandleMsgApp ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//    matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different
//    terms), delete the existing entry and all that follow it; append any new
//    entries not already in the log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//    of last new entry).
tap.test("raft testHandleMsgApp", (t) => {
    const tests: {
        m: Message;
        wIndex: number;
        wCommit: number;
        wReject: boolean;
    }[] = [
            // Ensure 1
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 3,
                    index: 2,
                    commit: 3,
                }),
                wIndex: 2,
                wCommit: 0,
                wReject: true,
            }, // previous log mismatch
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 3,
                    index: 3,
                    commit: 3,
                }),
                wIndex: 2,
                wCommit: 0,
                wReject: true,
            }, // previous log non-exist

            // Ensure 2
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 1,
                    index: 1,
                    commit: 1,
                }),
                wIndex: 2,
                wCommit: 1,
                wReject: false,
            },
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 0,
                    index: 0,
                    commit: 1,
                    entries: [new Entry({ index: 1, term: 2 })],
                }),
                wIndex: 1,
                wCommit: 1,
                wReject: false,
            },
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 2,
                    index: 2,
                    commit: 3,
                    entries: [new Entry({ index: 3, term: 2 }), new Entry({ index: 4, term: 2 })],
                }),
                wIndex: 4,
                wCommit: 3,
                wReject: false,
            },
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 2,
                    index: 2,
                    commit: 4,
                    entries: [new Entry({ index: 3, term: 2 })],
                }),
                wIndex: 3,
                wCommit: 3,
                wReject: false,
            },
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 1,
                    index: 1,
                    commit: 4,
                    entries: [new Entry({ index: 2, term: 2 })],
                }),
                wIndex: 2,
                wCommit: 2,
                wReject: false,
            },

            // Ensure 3
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 1,
                    logTerm: 1,
                    index: 1,
                    commit: 3,
                }),
                wIndex: 2,
                wCommit: 1,
                wReject: false,
            }, // match entry 1, commit up to last new entry 1
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 1,
                    logTerm: 1,
                    index: 1,
                    commit: 3,
                    entries: [new Entry({ index: 2, term: 2 })],
                }),
                wIndex: 2,
                wCommit: 2,
                wReject: false,
            }, // match entry 1, commit up to last new entry 2
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 2,
                    index: 2,
                    commit: 3,
                }),
                wIndex: 2,
                wCommit: 2,
                wReject: false,
            }, // match entry 2, commit up to last new entry 2
            {
                m: new Message({
                    type: MessageType.MsgApp,
                    term: 2,
                    logTerm: 2,
                    index: 2,
                    commit: 4,
                }),
                wIndex: 2,
                wCommit: 2,
                wReject: false,
            }, // commit up to log.last()
        ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1"));
        storage.Append([new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 2 })]);
        const sm = newTestRaft("1", 10, 1, storage);
        sm.becomeFollower(2, ridnone);

        sm.handleAppendEntries(tt.m);

        t.equal(
            sm.raftLog.lastIndex(),
            tt.wIndex,
            `#${i}: lastIndex = ${sm.raftLog.lastIndex()}, want ${tt.wIndex}`
        );
        t.equal(
            sm.raftLog.committed,
            tt.wCommit,
            `#${i}: committed = ${sm.raftLog.committed}, want ${tt.wCommit}`
        );
        const m = sm.readMessages();
        t.equal(m.length, 1, `#${i}: msg.length = ${m.length}, want ${1}`);
        t.equal(m[0].reject, tt.wReject, `#${i}: reject = ${m[0].reject}, want ${tt.wReject}`);
    });

    t.end();
});

// TestHandleHeartbeat ensures that the follower commits to the commit in the
// message.
tap.test("raft testHandleHeartbeat", (t) => {
    const commit = 2;
    const tests: { m: Message; wCommit: number }[] = [
        {
            m: new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgHeartbeat,
                term: 2,
                commit: commit + 1,
            }),
            wCommit: commit + 1,
        },
        {
            m: new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgHeartbeat,
                term: 2,
                commit: commit - 1,
            }),
            wCommit: commit,
        }, // do not decrease commit
    ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1", "2"));
        storage.Append([
            new Entry({ index: 1, term: 1 }),
            new Entry({ index: 2, term: 2 }),
            new Entry({ index: 3, term: 3 }),
        ]);

        const sm = newTestRaft("1", 5, 1, storage);
        sm.becomeFollower(2, "2");
        sm.raftLog.commitTo(commit);
        sm.handleHeartbeat(tt.m);

        t.equal(
            sm.raftLog.committed,
            tt.wCommit,
            `#${i}: committed = ${sm.raftLog.committed}, want ${tt.wCommit}`
        );

        const m = sm.readMessages();
        t.equal(m.length, 1, `#${i}: msg.length = ${m.length}, want 1`);
        t.equal(
            m[0].type,
            MessageType.MsgHeartbeatResp,
            `#${i}: type = ${MessageType[m[0].type]}, want MsgHeartbeatResp`
        );
    });
    t.end();
});

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a
// heartbeat response.
tap.test("raft testHandleHeartbeatResp", (t) => {
    const storage = newTestMemoryStorage(withPeers("1", "2"));
    storage.Append([
        new Entry({ index: 1, term: 1 }),
        new Entry({ index: 2, term: 2 }),
        new Entry({ index: 3, term: 3 }),
    ]);
    const sm = newTestRaft("1", 5, 1, storage);
    sm.becomeCandidate();
    sm.becomeLeader();
    sm.raftLog.commitTo(sm.raftLog.lastIndex());

    // A heartbeat response from a node that is behind; re-send MsgApp
    sm.Step(new Message({ from: "2", type: MessageType.MsgHeartbeatResp }));
    let msgs = sm.readMessages();

    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    t.equal(msgs[0].type, MessageType.MsgApp, `type = ${msgs[0].type}, want MsgApp`);

    // A second heartbeat response generates another MsgApp re-send
    sm.Step(new Message({ from: "2", type: MessageType.MsgHeartbeatResp }));
    msgs = sm.readMessages();
    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    t.equal(msgs[0].type, MessageType.MsgApp, `type = ${msgs[0].type}, want MsgApp`);

    // Once we have an MsgAppResp, heartbeats no longer send MsgApp
    sm.Step(
        new Message({
            from: "2",
            type: MessageType.MsgAppResp,
            index: msgs[0].index + msgs[0].entries.length,
        })
    );
    // Consume the message sent in response to MsgAppResp
    sm.readMessages();

    sm.Step(new Message({ from: "2", type: MessageType.MsgHeartbeatResp }));
    msgs = sm.readMessages();

    t.equal(msgs.length, 0, `msgs.length = ${msgs.length}, want 0`);

    t.end();
});

// TestRaftFreesReadOnlyMem ensures raft will free read request from readOnly
// readIndexQueue and pendingReadIndex map. related issue:
// https://github.com/etcd-io/etcd/issues/7571
tap.test("raft testRaftFreesReadOnlyMem", (t) => {
    const sm = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2")));
    sm.becomeCandidate();
    sm.becomeLeader();
    sm.raftLog.commitTo(sm.raftLog.lastIndex());

    const ctx = "ctx";

    // leader starts linearizable read request. more info: raft dissertation
    // 6.4, step 2.
    sm.Step(
        new Message({
            from: "2",
            type: MessageType.MsgReadIndex,
            entries: [new Entry({ data: ctx })],
        })
    );
    const msgs = sm.readMessages();

    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    t.equal(
        msgs[0].type,
        MessageType.MsgHeartbeat,
        `type = ${MessageType[msgs[0].type]}, want MsgHeartbeat`
    );
    t.equal(msgs[0].context, ctx, `Context = ${msgs[0].context}, want ${ctx}`);
    t.equal(
        sm.readOnly.readIndexQueue.length,
        1,
        `readIndexQueue.length = ${sm.readOnly.readIndexQueue.length}, want 1`
    );
    t.ok(sm.readOnly.pendingReadIndex.has(ctx), `can't find context ${ctx} in pendingReadIndex`);

    // heartbeat responses from majority of followers (1 in this case)
    // acknowledge the authority of the leader. more info: raft dissertation
    // 6.4, step 3.
    sm.Step(
        new Message({
            from: "2",
            type: MessageType.MsgHeartbeatResp,
            context: ctx,
        })
    );
    t.equal(
        sm.readOnly.readIndexQueue.length,
        0,
        `readIndexQueue.length = ${sm.readOnly.readIndexQueue.length}, want 0`
    );
    t.equal(
        sm.readOnly.pendingReadIndex.size,
        0,
        `pendingReadIndex.size = ${sm.readOnly.pendingReadIndex.size}, want 0`
    );
    t.notOk(
        sm.readOnly.pendingReadIndex.has(ctx),
        `found context ${ctx} in pendingReadIndex, want none`
    );

    t.end();
});

// TestMsgAppRespWaitReset verifies the resume behavior of a leader MsgAppResp.
tap.test("raft testMsgAppRespWaitReset", (t) => {
    const sm = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    sm.becomeCandidate();
    sm.becomeLeader();

    // The new leader has just emitted a new Term 4 entry; consume those
    // messages from the outgoing queue.
    sm.bcastAppend();
    sm.readMessages();

    // Node 2 acks the first entry, making it committed.
    sm.Step(new Message({ from: "2", type: MessageType.MsgAppResp, index: 1 }));
    t.equal(sm.raftLog.committed, 1, `expected committed to be 1, got ${sm.raftLog.committed}`);
    // Also consume the MsgApp messages that update Commit on the followers.
    sm.readMessages();

    // A new command is now proposed on node 1.
    sm.Step(
        new Message({
            from: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );

    // The command is broadcast to all nodes not in the wait state. Node 2 left
    // the wait state due to its MsgAppResp, but node 3 is still waiting.
    let msgs = sm.readMessages();
    t.equal(msgs.length, 1, `expected 1 message, got ${msgs.length}: ${JSON.stringify(msgs)}`);
    t.equal(
        msgs[0].type,
        MessageType.MsgApp,
        `expected message with type MsgApp, got ${MessageType[msgs[0].type]}`
    );
    t.equal(msgs[0].to, "2", `expected MsgApp to node 2, got to = ${msgs[0].to}`);
    t.equal(msgs[0].entries.length, 1, `expected to send 1 entry, got: ${msgs[0].entries.length}`);
    t.equal(
        msgs[0].entries[0].index,
        2,
        `expected to send entry with index 2, but got ${msgs[0].entries[0].index}`
    );

    // Now Node 3 acks the first entry. This releases the wait and entry 2 is
    // sent.
    sm.Step(new Message({ from: "3", type: MessageType.MsgAppResp, index: 1 }));
    msgs = sm.readMessages();
    t.equal(msgs.length, 1, `expected 1 message, got ${msgs.length}`);
    t.equal(
        msgs[0].type,
        MessageType.MsgApp,
        `expected message with type MsgApp, got ${MessageType[msgs[0].type]}`
    );
    t.equal(msgs[0].to, "3", `expected message to node 3, got to = ${msgs[0].to}`);
    t.equal(msgs[0].entries.length, 1, `expected to send 1 entry, got: ${msgs[0].entries.length}`);
    t.equal(
        msgs[0].entries[0].index,
        2,
        `expected to send entry with index 2, got index = ${msgs[0].entries}`
    );

    t.end();
});

tap.test("raft testRecvMsgVote", (t) => {
    function testRecvMsgVote(msgType: MessageType) {
        const tests: {
            state: StateType;
            index: number;
            logTerm: number;
            voteFor: rid;
            wreject: boolean;
        }[] = [
                {
                    state: StateType.StateFollower,
                    index: 0,
                    logTerm: 0,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 0,
                    logTerm: 1,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 0,
                    logTerm: 2,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 0,
                    logTerm: 3,
                    voteFor: ridnone,
                    wreject: false,
                },

                {
                    state: StateType.StateFollower,
                    index: 1,
                    logTerm: 0,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 1,
                    logTerm: 1,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 1,
                    logTerm: 2,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 1,
                    logTerm: 3,
                    voteFor: ridnone,
                    wreject: false,
                },

                {
                    state: StateType.StateFollower,
                    index: 2,
                    logTerm: 0,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 2,
                    logTerm: 1,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 2,
                    logTerm: 2,
                    voteFor: ridnone,
                    wreject: false,
                },
                {
                    state: StateType.StateFollower,
                    index: 2,
                    logTerm: 3,
                    voteFor: ridnone,
                    wreject: false,
                },

                {
                    state: StateType.StateFollower,
                    index: 3,
                    logTerm: 0,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 3,
                    logTerm: 1,
                    voteFor: ridnone,
                    wreject: true,
                },
                {
                    state: StateType.StateFollower,
                    index: 3,
                    logTerm: 2,
                    voteFor: ridnone,
                    wreject: false,
                },
                {
                    state: StateType.StateFollower,
                    index: 3,
                    logTerm: 3,
                    voteFor: ridnone,
                    wreject: false,
                },

                {
                    state: StateType.StateFollower,
                    index: 3,
                    logTerm: 2,
                    voteFor: "2",
                    wreject: false,
                },
                {
                    state: StateType.StateFollower,
                    index: 3,
                    logTerm: 2,
                    voteFor: "1",
                    wreject: true,
                },

                {
                    state: StateType.StateLeader,
                    index: 3,
                    logTerm: 3,
                    voteFor: "1",
                    wreject: true,
                },
                {
                    state: StateType.StatePreCandidate,
                    index: 3,
                    logTerm: 3,
                    voteFor: "1",
                    wreject: true,
                },
                {
                    state: StateType.StateCandidate,
                    index: 3,
                    logTerm: 3,
                    voteFor: "1",
                    wreject: true,
                },
            ];

        tests.forEach((tt, i) => {
            const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
            sm.state = tt.state;

            switch (tt.state) {
                case StateType.StateFollower:
                    sm.step = stepFollower;
                    break;
                case StateType.StateCandidate: // Fallthrough
                case StateType.StatePreCandidate:
                    sm.step = stepCandidate;
                    break;
                case StateType.StateLeader:
                    sm.step = stepLeader;
                    break;
            }

            sm.vote = tt.voteFor;
            const smRaftLog = new RaftLog({
                storage: new MemoryStorage({
                    ents: [
                        new Entry(),
                        new Entry({ index: 1, term: 2 }),
                        new Entry({ index: 2, term: 2 }),
                    ],
                }),
                unstable: new Unstable({ offset: 3 }),
            });
            sm.raftLog = smRaftLog;

            // raft.Term is greater than or equal to raft.raftLog.lastTerm. In
            // this test we're only testing MsgVote responses when the
            // campaigning node has a different raft log compared to the
            // recipient node. Additionally we're verifying behavior when the
            // recipient node has already given out its vote for its current
            // term. We're not testing what the recipient node does when
            // receiving a message with a different term number, so we simply
            // initialize both term numbers to be the same.
            const term = Math.max(sm.raftLog.lastTerm(), tt.logTerm);
            sm.term = term;
            sm.Step(
                new Message({
                    type: msgType,
                    term: term,
                    from: "2",
                    index: tt.index,
                    logTerm: tt.logTerm,
                })
            );

            const msgs = sm.readMessages();
            const g = msgs.length;

            t.equal(g, 1, `#${i}: msgs.length = ${g}, want 1`);
            t.equal(
                msgs[0].type,
                voteRespMsgType(msgType),
                `#${i}, m.Type = ${MessageType[msgs[0].type]}, want ${MessageType[voteRespMsgType(msgType)]
                }`
            );
            t.equal(
                msgs[0].reject,
                tt.wreject,
                `#${i}, m.Reject = ${msgs[0].reject}, want ${tt.wreject}`
            );
        });
    }

    testRecvMsgVote(MessageType.MsgVote);
    testRecvMsgVote(MessageType.MsgPreVote);

    t.end();
});

tap.test("raft testStateTransition", (t) => {
    const tests: {
        from: StateType;
        to: StateType;
        wallow: boolean;
        wterm: number;
        wlead: rid;
    }[] = [
            {
                from: StateType.StateFollower,
                to: StateType.StateFollower,
                wallow: true,
                wterm: 1,
                wlead: ridnone,
            },
            {
                from: StateType.StateFollower,
                to: StateType.StatePreCandidate,
                wallow: true,
                wterm: 0,
                wlead: ridnone,
            },
            {
                from: StateType.StateFollower,
                to: StateType.StateCandidate,
                wallow: true,
                wterm: 1,
                wlead: ridnone,
            },
            {
                from: StateType.StateFollower,
                to: StateType.StateLeader,
                wallow: false,
                wterm: 0,
                wlead: ridnone,
            },

            {
                from: StateType.StatePreCandidate,
                to: StateType.StateFollower,
                wallow: true,
                wterm: 0,
                wlead: ridnone,
            },
            {
                from: StateType.StatePreCandidate,
                to: StateType.StatePreCandidate,
                wallow: true,
                wterm: 0,
                wlead: ridnone,
            },
            {
                from: StateType.StatePreCandidate,
                to: StateType.StateCandidate,
                wallow: true,
                wterm: 1,
                wlead: ridnone,
            },
            {
                from: StateType.StatePreCandidate,
                to: StateType.StateLeader,
                wallow: true,
                wterm: 0,
                wlead: "1",
            },

            {
                from: StateType.StateCandidate,
                to: StateType.StateFollower,
                wallow: true,
                wterm: 0,
                wlead: ridnone,
            },
            {
                from: StateType.StateCandidate,
                to: StateType.StatePreCandidate,
                wallow: true,
                wterm: 0,
                wlead: ridnone,
            },
            {
                from: StateType.StateCandidate,
                to: StateType.StateCandidate,
                wallow: true,
                wterm: 1,
                wlead: ridnone,
            },
            {
                from: StateType.StateCandidate,
                to: StateType.StateLeader,
                wallow: true,
                wterm: 0,
                wlead: "1",
            },

            {
                from: StateType.StateLeader,
                to: StateType.StateFollower,
                wallow: true,
                wterm: 1,
                wlead: ridnone,
            },
            {
                from: StateType.StateLeader,
                to: StateType.StatePreCandidate,
                wallow: false,
                wterm: 0,
                wlead: ridnone,
            },
            {
                from: StateType.StateLeader,
                to: StateType.StateCandidate,
                wallow: false,
                wterm: 1,
                wlead: ridnone,
            },
            {
                from: StateType.StateLeader,
                to: StateType.StateLeader,
                wallow: true,
                wterm: 0,
                wlead: "1",
            },
        ];

    tests.forEach((tt, i) => {
        try {
            const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
            sm.state = tt.from;

            switch (tt.to) {
                case StateType.StateFollower:
                    sm.becomeFollower(tt.wterm, tt.wlead);
                    break;
                case StateType.StatePreCandidate:
                    sm.becomePreCandidate();
                    break;
                case StateType.StateCandidate:
                    sm.becomeCandidate();
                    break;
                case StateType.StateLeader:
                    sm.becomeLeader();
                    break;
            }

            t.equal(sm.term, tt.wterm, `${i}: term = ${sm.term}, want ${tt.wterm}`);
            t.equal(sm.lead, tt.wlead, `${i}: lead = ${sm.lead}, want ${tt.wlead}`);
        } catch (error) {
            t.notOk(
                tt.wallow,
                `#${i}: allow = ${tt.wallow}, want ${false}, error: ${error.message}`
            );
        }
    });

    t.end();
});

tap.test("raft testAllServerStepdown", (t) => {
    const tests: {
        state: StateType;

        wstate: StateType;
        wterm: number;
        windex: number;
    }[] = [
            {
                state: StateType.StateFollower,
                wstate: StateType.StateFollower,
                wterm: 3,
                windex: 0,
            },
            {
                state: StateType.StatePreCandidate,
                wstate: StateType.StateFollower,
                wterm: 3,
                windex: 0,
            },
            {
                state: StateType.StateCandidate,
                wstate: StateType.StateFollower,
                wterm: 3,
                windex: 0,
            },
            {
                state: StateType.StateLeader,
                wstate: StateType.StateFollower,
                wterm: 3,
                windex: 1,
            },
        ];

    const tmsgTypes = [MessageType.MsgVote, MessageType.MsgApp];
    const tterm = 3;

    tests.forEach((tt, i) => {
        const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

        switch (tt.state) {
            case StateType.StateFollower:
                sm.becomeFollower(1, ridnone);
                break;
            case StateType.StatePreCandidate:
                sm.becomePreCandidate();
                break;
            case StateType.StateCandidate:
                sm.becomeCandidate();
                break;
            case StateType.StateLeader:
                sm.becomeCandidate();
                sm.becomeLeader();
                break;
            default:
                throw new Error("Unknown StateType");
        }

        for (let j = 0; j < tmsgTypes.length; j++) {
            const msgType = tmsgTypes[j];

            sm.Step(
                new Message({
                    from: "2",
                    type: msgType,
                    term: tterm,
                    logTerm: tterm,
                })
            );

            t.equal(sm.state, tt.wstate, `#${i}.${j} state = ${sm.state}, want ${tt.wstate}`);
            t.equal(sm.term, tt.wterm, `#${i}.${j} term = ${sm.term}, want ${tt.wterm}`);
            t.equal(
                sm.raftLog.lastIndex(),
                tt.windex,
                `#${i}.${j} lastIndex = ${sm.raftLog.lastIndex()}, want ${tt.windex}`
            );
            t.equal(
                sm.raftLog.allEntries().length,
                tt.windex,
                `#${i}.${j} ents.length = ${sm.raftLog.allEntries().length}, want ${tt.windex}`
            );

            let wlead = "2";
            if (msgType === MessageType.MsgVote) {
                wlead = ridnone;
            }
            t.equal(sm.lead, wlead, `#${i} sm.lead = ${sm.lead}, want ${wlead}`);
        }
    });

    t.end();
});

// testCandidateResetTerm tests when a candidate receives a MsgHeartbeat or
// MsgApp from leader, "Step" resets the term with leader's and reverts back to
// follower.
tap.test("raft testCandidateResetTerm", (t) => {
    function testCandidateResetTerm(mType: MessageType) {
        const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        const b = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        const c = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

        const nt: Network = newNetwork(a, b, c);

        send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);
        t.equal(
            b.state,
            StateType.StateFollower,
            `state = ${StateType[b.state]}, want StateFollower`
        );
        t.equal(
            c.state,
            StateType.StateFollower,
            `state = ${StateType[c.state]}, want StateFollower`
        );

        // isolate 3 and increase term in rest
        isolate(nt, "3");

        send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));
        send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

        t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);
        t.equal(
            b.state,
            StateType.StateFollower,
            `state = ${StateType[b.state]}, want StateFollower`
        );

        // trigger campaign in isolated c
        c.resetRandomizedElectionTimeout();
        for (let i = 0; i < c.randomizedElectionTimeout; i++) {
            c.tick();
        }

        t.equal(
            c.state,
            StateType.StateCandidate,
            `state = ${StateType[c.state]}, want StateCandidate`
        );

        recover(nt);

        // leader sends to isolated candidate and expects candidate to revert to
        // follower
        send(nt, new Message({ from: "1", to: "3", term: a.term, type: mType }));

        t.equal(
            c.state,
            StateType.StateFollower,
            `state = ${StateType[c.state]}, want StateFollower`
        );

        // follower c term is reset with leader's
        t.equal(
            a.term,
            c.term,
            `follower term expected same term as leader's: ${a.term}, got: ${c.term}`
        );
    }

    testCandidateResetTerm(MessageType.MsgHeartbeat);
    testCandidateResetTerm(MessageType.MsgApp);

    t.end();
});

tap.test("raft testLeaderStepdownWhenQuorumActive", (t) => {
    const sm = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    sm.checkQuorum = true;

    sm.becomeCandidate();
    sm.becomeLeader();

    for (let i = 0; i < sm.electionTimeout + 1; i++) {
        sm.Step(
            new Message({
                from: "2",
                type: MessageType.MsgHeartbeatResp,
                term: sm.term,
            })
        );
        sm.tick();
    }

    t.equal(sm.state, StateType.StateLeader, `state = ${StateType[sm.state]}, want StateLeader`);

    t.end();
});

tap.test("raft testLeaderStepdownWhenQuorumLost", (t) => {
    const sm = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    sm.checkQuorum = true;

    sm.becomeCandidate();
    sm.becomeLeader();

    for (let i = 0; i < sm.electionTimeout + 1; i++) {
        sm.tick();
    }

    t.equal(
        sm.state,
        StateType.StateFollower,
        `state = ${StateType[sm.state]}, want StateFollower`
    );

    t.end();
});

tap.test("raft testLeaderSupersedingWithCheckQuorum", (t) => {
    const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const b = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const c = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    a.checkQuorum = true;
    b.checkQuorum = true;
    c.checkQuorum = true;

    const nt = newNetwork(a, b, c);
    setRandomizedElectionTimeout(b, b.electionTimeout + 1);

    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);
    t.equal(c.state, StateType.StateFollower, `state = ${StateType[c.state]}, want StateFollower`);

    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    // Peer b rejected c's vote since its electionElapsed had not reached to
    // electionTimeout
    t.equal(
        c.state,
        StateType.StateCandidate,
        `state = ${StateType[c.state]}, want StateCandidate`
    );

    // Letting b's electionElapsed reach to electionTimeout
    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    t.equal(c.state, StateType.StateLeader, `state = ${StateType[c.state]}, want StateLeader`);

    t.end();
});

tap.test("raft testLeaderElectionWithCheckQuorum", (t) => {
    const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const b = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const c = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    a.checkQuorum = true;
    b.checkQuorum = true;
    c.checkQuorum = true;

    const nt = newNetwork(a, b, c);
    setRandomizedElectionTimeout(a, a.electionTimeout + 1);
    setRandomizedElectionTimeout(b, b.electionTimeout + 2);

    // Immediately after creation, votes are cast regardless of the election
    // timeout.
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);
    t.equal(c.state, StateType.StateFollower, `state = ${StateType[c.state]}, want StateFollower`);

    // need to reset randomizedElectionTimeout larger than electionTimeout
    // again, because the value might be reset to electionTimeout since the last
    // state changes
    setRandomizedElectionTimeout(a, a.electionTimeout + 1);
    setRandomizedElectionTimeout(b, b.electionTimeout + 2);
    for (let i = 0; i < a.electionTimeout; i++) {
        a.tick();
    }
    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }

    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    t.equal(a.state, StateType.StateFollower, `state = ${StateType[a.state]}, want StateFollower`);
    t.equal(c.state, StateType.StateLeader, `state = ${StateType[c.state]}, want StateLeader`);

    t.end();
});

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher
// term can disrupt the leader even if the leader still "officially" holds the
// lease, The leader is expected to step down and adopt the candidate's term
tap.test("raft testFreeStuckCandidateWithCheckQuorum", (t) => {
    const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const b = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const c = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    a.checkQuorum = true;
    b.checkQuorum = true;
    c.checkQuorum = true;

    const nt = newNetwork(a, b, c);
    setRandomizedElectionTimeout(b, b.electionTimeout + 1);

    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "1");
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    t.equal(b.state, StateType.StateFollower, `state = ${StateType[b.state]}, want StateFollower`);
    t.equal(
        c.state,
        StateType.StateCandidate,
        `state = ${StateType[c.state]}, want StateCandidate`
    );
    t.equal(c.term, b.term + 1, `term = ${c.term}, want ${b.term + 1}`);

    // Vote again for safety
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    t.equal(b.state, StateType.StateFollower, `state = ${StateType[b.state]}, want StateFollower`);
    t.equal(
        c.state,
        StateType.StateCandidate,
        `state = ${StateType[c.state]}, want StateCandidate`
    );
    t.equal(c.term, b.term + 2, `term = ${c.term}, want ${b.term + 2}`);

    recover(nt);
    send(
        nt,
        new Message({
            from: "1",
            to: "3",
            type: MessageType.MsgHeartbeat,
            term: a.term,
        })
    );

    // Disrupt the leader so that the stuck peer is freed
    t.equal(a.state, StateType.StateFollower, `state = ${StateType[a.state]}, want StateFollower`);
    t.equal(c.term, a.term, `term = ${c.term}, want ${a.term}`);

    // Vote again, should become leader this time
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));
    t.equal(
        c.state,
        StateType.StateLeader,
        `peer 3 state: ${StateType[c.state]}, want StateLeader`
    );

    t.end();
});

tap.test("raft testPromotableVoterWithCheckQuorum", (t) => {
    const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
    const b = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1")));

    a.checkQuorum = true;
    b.checkQuorum = true;

    const nt = newNetwork(a, b);
    setRandomizedElectionTimeout(b, b.electionTimeout + 1);
    // Need to remove 2 again to make it a non-promotable node since newNetwork
    // overwritten some internal states
    b.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: "2",
        }).asV2()
    );

    t.notOk(b.promotable(), `promotable = ${b.promotable}, want false`);

    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);
    t.equal(b.state, StateType.StateFollower, `state = ${StateType[b.state]}, want StateFollower`);
    t.equal(b.lead, "1", `lead = ${b.lead}, want 1`);

    t.end();
});

// TestDisruptiveFollower tests isolated follower, with slow network incoming
// from leader, election times out to become a candidate with an increased term.
// Then, the candidate's response to late leader heartbeat forces the leader to
// step down.
tap.test("raft testDisruptiveFollower", (t) => {
    const n1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n3 = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    n1.checkQuorum = true;
    n2.checkQuorum = true;
    n3.checkQuorum = true;

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);
    n3.becomeFollower(1, ridnone);

    const nt = newNetwork(n1, n2, n3);

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // check state n1.state == StateLeader n2.state == StateFollower n3.state ==
    // StateFollower
    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state: ${StateType[n1.state]}, want StateLeader`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state: ${StateType[n2.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StateFollower,
        `node 3 state: ${StateType[n3.state]}, want StateFollower`
    );

    // etcd server "advanceTicksForElection" on restart; this is to expedite
    // campaign trigger when given larger election timeouts (e.g.
    // multi-datacenter deploy) Or leader messages are being delayed while ticks
    // elapse
    setRandomizedElectionTimeout(n3, n3.electionTimeout + 2);
    for (let i = 0; i < n3.randomizedElectionTimeout - 1; i++) {
        n3.tick();
    }
    // ideally, before last election tick elapses, the follower n3 receives
    // "pb.MsgApp" or "pb.MsgHeartbeat" from leader n1, and then resets its
    // "electionElapsed" however, last tick may elapse before receiving any
    // messages from leader, thus triggering campaign
    n3.tick();

    // n1 is still leader yet while its heartbeat to candidate n3 is being
    // delayed

    // check state n1.state == StateLeader n2.state == StateFollower n3.state ==
    // StateCandidate
    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state: ${StateType[n1.state]}, want StateLeader`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state: ${StateType[n2.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StateCandidate,
        `node 3 state: ${StateType[n3.state]}, want StateCandidate`
    );
    // check term n1.Term == 2 n2.Term == 2 n3.Term == 3
    t.equal(n1.term, 2, `node 1 term: ${n1.term}, want 2`);
    t.equal(n2.term, 2, `node 2 term: ${n2.term}, want 2`);
    t.equal(n3.term, 3, `node 3 term: ${n3.term}, want 3`);

    // while outgoing vote requests are still queued in n3, leader heartbeat
    // finally arrives at candidate n3 however, due to delayed network from
    // leader, leader heartbeat was sent with lower term than candidate's
    send(
        nt,
        new Message({
            from: "1",
            to: "3",
            term: n1.term,
            type: MessageType.MsgHeartbeat,
        })
    );

    // then candidate n3 responds with "pb.MsgAppResp" of higher term and leader
    // steps down from a message with higher term this is to disrupt the current
    // leader, so that candidate with higher term can be freed with following
    // election

    // check state n1.state == StateFollower n2.state == StateFollower n3.state
    // == StateCandidate
    t.equal(
        n1.state,
        StateType.StateFollower,
        `node 1 state: ${StateType[n1.state]}, want StateFollower`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state: ${StateType[n2.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StateCandidate,
        `node 3 state: ${StateType[n3.state]}, want StateCandidate`
    );
    // check term n1.Term == 3 n2.Term == 2 n3.Term == 3
    t.equal(n1.term, 3, `node 1 term: ${n1.term}, want 3`);
    t.equal(n2.term, 2, `node 2 term: ${n2.term}, want 2`);
    t.equal(n3.term, 3, `node 3 term: ${n3.term}, want 3`);

    t.end();
});

// TestDisruptiveFollowerPreVote tests isolated follower, with slow network
// incoming from leader, election times out to become a pre-candidate with less
// log than current leader. Then pre-vote phase prevents this isolated node from
// forcing current leader to step down, thus less disruptions.
tap.test("raft testDisruptiveFollowerPreVote", (t) => {
    const n1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n3 = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    n1.checkQuorum = true;
    n2.checkQuorum = true;
    n3.checkQuorum = true;

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);
    n3.becomeFollower(1, ridnone);

    const nt = newNetwork(n1, n2, n3);

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // check state n1.state == StateLeader n2.state == StateFollower n3.state ==
    // StateFollower
    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state: ${StateType[n1.state]}, want StateLeader`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state: ${StateType[n2.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StateFollower,
        `node 3 state: ${StateType[n3.state]}, want StateFollower`
    );

    isolate(nt, "3");
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "somedata" })],
        })
    );
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "somedata" })],
        })
    );
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "somedata" })],
        })
    );
    n1.preVote = true;
    n2.preVote = true;
    n3.preVote = true;
    recover(nt);
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    // check state n1.state == StateLeader n2.state == StateFollower n3.state ==
    // StatePreCandidate
    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state: ${StateType[n1.state]}, want StateLeader`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state: ${StateType[n2.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StatePreCandidate,
        `node 3 state: ${StateType[n3.state]}, want StatePreCandidate`
    );
    // check term n1.Term == 2 n2.Term == 2 n3.Term == 2
    t.equal(n1.term, 2, `node 1 term: ${n1.term}, want 2`);
    t.equal(n2.term, 2, `node 2 term: ${n2.term}, want 2`);
    t.equal(n3.term, 2, `node 3 term: ${n3.term}, want 2`);

    // delayed leader heartbeat does not force current leader to step down
    send(
        nt,
        new Message({
            from: "1",
            to: "3",
            term: n1.term,
            type: MessageType.MsgHeartbeat,
        })
    );
    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state: ${StateType[n1.state]}, want StateLeader`
    );

    t.end();
});

tap.test("raft testReadOnlyOptionSafe", (t) => {
    const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const b = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const c = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    const nt = newNetwork(a, b, c);
    setRandomizedElectionTimeout(b, b.electionTimeout + 1);

    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);

    const tests: {
        sm: RaftForTest;
        proposals: number;
        wri: number;
        wctx: string;
    }[] = [
            { sm: a, proposals: 10, wri: 11, wctx: "ctx1" },
            { sm: b, proposals: 10, wri: 21, wctx: "ctx2" },
            { sm: c, proposals: 10, wri: 31, wctx: "ctx3" },
            { sm: a, proposals: 10, wri: 41, wctx: "ctx4" },
            { sm: b, proposals: 10, wri: 51, wctx: "ctx5" },
            { sm: c, proposals: 10, wri: 61, wctx: "ctx6" },
        ];

    tests.forEach((tt, i) => {
        for (let j = 0; j < tt.proposals; j++) {
            send(
                nt,
                new Message({
                    from: "1",
                    to: "1",
                    type: MessageType.MsgProp,
                    entries: [new Entry()],
                })
            );
        }

        send(
            nt,
            new Message({
                from: tt.sm.id,
                to: tt.sm.id,
                type: MessageType.MsgReadIndex,
                entries: [new Entry({ data: tt.wctx })],
            })
        );

        const r = tt.sm;
        t.notOk(r.readStates.length === 0, `#${i}: readStates.length = 0, want non-zero`);
        const rs = r.readStates[0];
        t.equal(rs.index, tt.wri, `#${i}: readIndex = ${rs.index}, want ${tt.wri}`);
        t.strictSame(
            rs.requestCtx,
            tt.wctx,
            `#${i}: requestCtx = ${rs.requestCtx}, want ${tt.wctx}`
        );
        r.readStates = [];
    });

    t.end();
});

tap.test("raft testReadOnlyWithLearner", (t) => {
    const a = newTestLearnerRaft("1", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));
    const b = newTestLearnerRaft("1", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));

    const nt = newNetwork(a, b);
    setRandomizedElectionTimeout(b, b.electionTimeout + 1);

    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);

    const tests: {
        sm: RaftForTest;
        proposals: number;
        wri: number;
        wctx: string;
    }[] = [
            { sm: a, proposals: 10, wri: 11, wctx: "ctx1" },
            { sm: b, proposals: 10, wri: 21, wctx: "ctx2" },
            { sm: a, proposals: 10, wri: 31, wctx: "ctx3" },
            { sm: b, proposals: 10, wri: 41, wctx: "ctx4" },
        ];

    tests.forEach((tt, i) => {
        for (let j = 0; j < tt.proposals; j++) {
            send(
                nt,
                new Message({
                    from: "1",
                    to: "1",
                    type: MessageType.MsgProp,
                    entries: [new Entry()],
                })
            );
        }

        send(
            nt,
            new Message({
                from: tt.sm.id,
                to: tt.sm.id,
                type: MessageType.MsgReadIndex,
                entries: [new Entry({ data: tt.wctx })],
            })
        );

        const r = tt.sm;
        t.notOk(r.readStates.length === 0, `#${i}: readStates.length = 0, want non-zero`);
        const rs = r.readStates[0];
        t.equal(rs.index, tt.wri, `#${i}: readIndex = ${rs.index}, want ${tt.wri}`);
        t.strictSame(
            rs.requestCtx,
            tt.wctx,
            `#${i}: requestCtx = ${rs.requestCtx}, want ${tt.wctx}`
        );
        r.readStates = [];
    });

    t.end();
});

tap.test("raft testReadOnlyOptionLease", (t) => {
    const a = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const b = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const c = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    a.readOnly.option = ReadOnlyOption.ReadOnlyLeaseBased;
    b.readOnly.option = ReadOnlyOption.ReadOnlyLeaseBased;
    c.readOnly.option = ReadOnlyOption.ReadOnlyLeaseBased;
    a.checkQuorum = true;
    b.checkQuorum = true;
    c.checkQuorum = true;

    const nt = newNetwork(a, b, c);
    setRandomizedElectionTimeout(b, b.electionTimeout + 1);

    for (let i = 0; i < b.electionTimeout; i++) {
        b.tick();
    }
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    t.equal(a.state, StateType.StateLeader, `state = ${StateType[a.state]}, want StateLeader`);

    const tests: {
        sm: RaftForTest;
        proposals: number;
        wri: number;
        wctx: string;
    }[] = [
            { sm: a, proposals: 10, wri: 11, wctx: "ctx1" },
            { sm: b, proposals: 10, wri: 21, wctx: "ctx2" },
            { sm: c, proposals: 10, wri: 31, wctx: "ctx3" },
            { sm: a, proposals: 10, wri: 41, wctx: "ctx4" },
            { sm: b, proposals: 10, wri: 51, wctx: "ctx5" },
            { sm: c, proposals: 10, wri: 61, wctx: "ctx6" },
        ];

    tests.forEach((tt, i) => {
        for (let j = 0; j < tt.proposals; j++) {
            send(
                nt,
                new Message({
                    from: "1",
                    to: "1",
                    type: MessageType.MsgProp,
                    entries: [new Entry()],
                })
            );
        }
        send(
            nt,
            new Message({
                from: tt.sm.id,
                to: tt.sm.id,
                type: MessageType.MsgReadIndex,
                entries: [new Entry({ data: tt.wctx })],
            })
        );

        const r = tt.sm;
        t.notOk(r.readStates.length === 0, `#${i}: readStates.length = 0, want non-zero`);
        const rs = r.readStates[0];
        t.equal(rs.index, tt.wri, `#${i}: readIndex = ${rs.index}, want: ${tt.wri}`);
        t.strictSame(
            rs.requestCtx,
            tt.wctx,
            `#${i}: requestCtx = ${rs.requestCtx}, want ${tt.wctx}`
        );
        r.readStates = [];
    });

    t.end();
});

// TestReadOnlyForNewLeader ensures that a leader only accepts MsgReadIndex
// message when it commits at least one log entry at it term.
tap.test("raft testReadOnlyForNewLeader", (t) => {
    const nodeConfigs: {
        id: rid;
        committed: number;
        applied: number;
        compactIndex: number;
    }[] = [
            { id: "1", committed: 1, applied: 1, compactIndex: 0 },
            { id: "2", committed: 2, applied: 2, compactIndex: 2 },
            { id: "3", committed: 2, applied: 2, compactIndex: 2 },
        ];
    const peers: StateMachine[] = [];
    for (const c of nodeConfigs) {
        const storage = newTestMemoryStorage(withPeers("1", "2", "3"));
        storage.Append([new Entry({ index: 1, term: 1 }), new Entry({ index: 2, term: 1 })]);
        storage.SetHardState(new HardState({ term: 1, commit: c.committed }));
        if (c.compactIndex !== 0) {
            storage.Compact(c.compactIndex);
        }

        const cfg = newTestConfig(c.id, 10, 1, storage);
        cfg.applied = c.applied;
        const raft = new RaftForTest(cfg);
        peers.push(raft);
    }

    const nt = newNetwork(...peers);

    // Drop MsgApp to forbid peer a to commit any log entry at its term after it
    // becomes leader.
    ignore(nt, MessageType.MsgApp);
    // Force peer a to become leader.
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const sm = nt.peers.get("1") as RaftForTest;
    t.equal(sm.state, StateType.StateLeader, `state = ${StateType[sm.state]}, want StateLeader`);

    // Ensure peer a drops read only request.
    const windex = 4;
    const wctx = "ctx";
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgReadIndex,
            entries: [new Entry({ data: wctx })],
        })
    );
    t.equal(sm.readStates.length, 0, `readStates.length = ${sm.readStates.length}, want 0`);

    recover(nt);

    // Force peer a to commit a log entry at its term
    for (let i = 0; i < sm.heartbeatTimeout; i++) {
        sm.tick();
    }
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );
    t.equal(sm.raftLog.committed, 4, `committed = ${sm.raftLog.committed}, want 4`);
    const lastLogTerm = sm.raftLog.zeroTermOnErrCompacted(...sm.raftLog.term(sm.raftLog.committed));
    t.equal(lastLogTerm, sm.term, `last log term = ${lastLogTerm}, want ${sm.term}`);

    // Ensure peer a processed postponed read only request after it committed an
    // entry at its term.
    t.equal(sm.readStates.length, 1, `readStates.length = ${sm.readStates.length}, want 1`);
    let rs = sm.readStates[0];
    t.equal(rs.index, windex, `readIndex = ${rs.index}, want ${windex}`);
    t.strictSame(rs.requestCtx, wctx, `requestCtx = ${rs.requestCtx}, want ${wctx}`);

    // Ensure peer a accepts read only request after it committed an entry at
    // its term.
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgReadIndex,
            entries: [new Entry({ data: wctx })],
        })
    );
    t.equal(sm.readStates.length, 2, `readStates.length = ${sm.readStates.length}, want 2`);
    rs = sm.readStates[1];
    t.equal(rs.index, windex, `readIndex = ${rs.index}, want ${windex}`);
    t.strictSame(rs.requestCtx, wctx, `requestCtx = ${rs.requestCtx}, want ${wctx}`);

    t.end();
});

tap.test("raft testLeaderAppResp", (t) => {
    // initial progress: match = 0; next = 3
    const tests: {
        index: number;
        reject: boolean;
        // progress
        wmatch: number;
        wnext: number;
        // message
        wmsgNum: number;
        windex: number;
        wcommitted: number;
    }[] = [
            // stale resp; no replies
            {
                index: 3,
                reject: true,
                wmatch: 0,
                wnext: 3,
                wmsgNum: 0,
                windex: 0,
                wcommitted: 0,
            },
            // denied resp; leader does not commit; decrease next and send
            // probing msg
            {
                index: 2,
                reject: true,
                wmatch: 0,
                wnext: 2,
                wmsgNum: 1,
                windex: 1,
                wcommitted: 0,
            },
            // accept resp; leader commits; broadcast with commit index
            {
                index: 2,
                reject: false,
                wmatch: 2,
                wnext: 4,
                wmsgNum: 2,
                windex: 2,
                wcommitted: 2,
            },
            // ignore heartbeat replies
            {
                index: 0,
                reject: false,
                wmatch: 0,
                wnext: 3,
                wmsgNum: 0,
                windex: 0,
                wcommitted: 0,
            },
        ];

    tests.forEach((tt, i) => {
        // sm term is 1 after it becomes the leader. thus the last log term must
        // be 1 to be committed.
        const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        sm.raftLog = new RaftLog({
            storage: new MemoryStorage({
                ents: [
                    new Entry(),
                    new Entry({ index: 1, term: 0 }),
                    new Entry({ index: 2, term: 1 }),
                ],
            }),
            unstable: new Unstable({ offset: 3 }),
        });

        sm.becomeCandidate();
        sm.becomeLeader();
        sm.readMessages();
        sm.Step(
            new Message({
                from: "2",
                type: MessageType.MsgAppResp,
                index: tt.index,
                term: sm.term,
                reject: tt.reject,
                rejectHint: tt.index,
            })
        );

        const p = sm.prs.progress.get("2")!;
        t.equal(p.match, tt.wmatch, `#${i}: match = ${p.match}, want ${tt.wmatch}`);
        t.equal(p.next, tt.wnext, `#${i}: next = ${p.next}, want ${tt.wnext}`);

        const msgs = sm.readMessages();

        t.equal(msgs.length, tt.wmsgNum, `#${i}, msgNum = ${msgs.length}, want ${tt.wmsgNum}`);
        msgs.forEach((msg, j) => {
            t.equal(msg.index, tt.windex, `#${i}.${j} index = ${msg.index}, want ${tt.windex}`);
            t.equal(
                msg.commit,
                tt.wcommitted,
                `#${i}.${j} commit = ${msg.commit}, want ${tt.wcommitted}`
            );
        });
    });

    t.end();
});

// When the leader receives a heartbeat tick, it should send a MsgHeartbeat with
// m.Index = 0, m.LogTerm=0 and empty entries.
tap.test("raft testBcastBeat", (t) => {
    const offset = 1000;
    // make a state machine with log.offset = 1000
    const s: Snapshot = new Snapshot({
        metadata: new SnapshotMetadata({
            index: offset,
            term: 1,
            confState: new ConfState({ voters: ["1", "2", "3"] }),
        }),
    });
    const storage = MemoryStorage.NewMemoryStorage();
    storage.ApplySnapshot(s);
    const sm = newTestRaft("1", 10, 1, storage);
    sm.term = 1;

    sm.becomeCandidate();
    sm.becomeLeader();
    for (let i = 0; i < 10; i++) {
        mustAppendEntry(sm, new Entry({ index: i + 1 }));
    }
    // slow follower
    sm.prs.progress.get("2")!.match = 5;
    sm.prs.progress.get("2")!.next = 6;
    // normal follower
    sm.prs.progress.get("3")!.match = sm.raftLog.lastIndex();
    sm.prs.progress.get("3")!.next = sm.raftLog.lastIndex() + 1;

    sm.Step(new Message({ type: MessageType.MsgBeat }));
    const msgs = sm.readMessages();
    t.equal(msgs.length, 2, `msgs.length = ${msgs.length}, want 2`);

    const wantCommitMap = new Map<rid, number>();
    wantCommitMap.set("2", Math.min(sm.raftLog.committed, sm.prs.progress.get("2")!.match));
    wantCommitMap.set("3", Math.min(sm.raftLog.committed, sm.prs.progress.get("3")!.match));

    msgs.forEach((m, i) => {
        t.equal(
            m.type,
            MessageType.MsgHeartbeat,
            `#${i}: type = ${MessageType[m.type]}, want = MsgHeartbeat`
        );
        t.equal(m.index, 0, `#${i}: prevIndex = ${m.index}, want 0`);
        t.equal(m.logTerm, 0, `#${i}: prevTerm = ${m.logTerm}, want 0`);

        if (wantCommitMap.get(m.to) === 0) {
            t.fail(`#${i}: unexpected to ${m.to}`);
        } else {
            t.equal(
                m.commit,
                wantCommitMap.get(m.to),
                `#${i}: commit = ${m.commit}, want ${wantCommitMap.get(m.to)}`
            );
            wantCommitMap.delete(m.to);
        }
        t.equal(m.entries.length, 0, `#${i}: entries.length = ${m.entries.length}, want 0`);
    });

    t.end();
});

// tests the output of the state machine when receiving MsgBeat
tap.test("raft TestRecvMsgBeat", (t) => {
    const tests: { state: StateType; wMsg: number }[] = [
        { state: StateType.StateLeader, wMsg: 2 },
        // candidate and follower should ignore MsgBeat
        { state: StateType.StateCandidate, wMsg: 0 },
        { state: StateType.StateFollower, wMsg: 0 },
    ];

    tests.forEach((tt, i) => {
        const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        sm.raftLog = new RaftLog({
            storage: new MemoryStorage({
                ents: [
                    new Entry(),
                    new Entry({ index: 1, term: 0 }),
                    new Entry({ index: 2, term: 1 }),
                ],
            }),
        });
        sm.term = 1;
        sm.state = tt.state;

        switch (tt.state) {
            case StateType.StateFollower:
                sm.step = stepFollower;
                break;
            case StateType.StateCandidate:
                sm.step = stepCandidate;
                break;
            case StateType.StateLeader:
                sm.step = stepLeader;
                break;
        }
        sm.Step(new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));

        const msgs = sm.readMessages();
        t.equal(msgs.length, tt.wMsg, `${i}: msgs.length = ${msgs.length}, want ${tt.wMsg}`);

        for (const m of msgs) {
            t.equal(
                m.type,
                MessageType.MsgHeartbeat,
                `${i}: msg.type = ${MessageType[m.type]}, want MsgHeartbeat`
            );
        }
    });

    t.end();
});

tap.test("raft testLeaderIncreaseNext", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 1, index: 2 }),
        new Entry({ term: 1, index: 3 }),
    ];
    const tests: {
        // progress
        state: ProgressStateType;
        next: number;

        wnext: number;
    }[] = [
            // state replicate, optimistically increase next previous entries +
            // noop entry + propose + 1
            {
                state: ProgressStateType.StateReplicate,
                next: 2,
                wnext: previousEnts.length + 1 + 1 + 1,
            },
            // state probe, not optimistically increase next
            { state: ProgressStateType.StateProbe, next: 2, wnext: 2 },
        ];

    tests.forEach((tt, i) => {
        const sm = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
        sm.raftLog.append(...previousEnts);
        sm.becomeCandidate();
        sm.becomeLeader();
        sm.prs.progress.get("2")!.state = tt.state;
        sm.prs.progress.get("2")!.next = tt.next;

        sm.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "somedata" })],
            })
        );

        const p = sm.prs.progress.get("2")!;
        t.equal(p.next, tt.wnext, `#${i}: next = ${p.next}, want ${tt.wnext}`);
    });

    t.end();
});

tap.test("raft testSendAppendForProgressProbe", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();
    r.readMessages();
    r.prs.progress.get("2")!.BecomeProbe();

    // each round is a heartbeat
    for (let i = 0; i < 3; i++) {
        if (i === 0) {
            // we expect that raft will only send out one msgAPP on the first
            // loop. After that, the follower is paused until a heartbeat
            // response is received.
            mustAppendEntry(r, new Entry({ data: "somedata" }));
            r.sendAppend("2");
            const msg = r.readMessages();

            t.equal(msg.length, 1, `msg.length = ${msg.length}, want 1`);
            t.equal(msg[0].index, 0, `index = ${msg[0].index}, want 0`);
        }

        t.ok(
            r.prs.progress.get("2")!.probeSent,
            `paused = ${r.prs.progress.get("2")!.probeSent}, want true`
        );

        for (let j = 0; j < 10; j++) {
            mustAppendEntry(r, new Entry({ data: "somedata" }));
            r.sendAppend("2");

            const l = r.readMessages().length;
            t.equal(l, 0, `msg.length = ${l}, want 0`);
        }

        // do a heartbeat
        for (let j = 0; j < r.heartbeatTimeout; j++) {
            r.Step(new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));
        }

        t.ok(
            r.prs.progress.get("2")!.probeSent,
            `paused = ${r.prs.progress.get("2")!.probeSent}, want true`
        );

        // consume the heartbeat
        const msgs = r.readMessages();
        t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
        t.equal(
            msgs[0].type,
            MessageType.MsgHeartbeat,
            `type = ${MessageType[msgs[0].type]}, want MsgHeartbeat`
        );
    }

    // a heartbeat response will allow another message to be sent
    r.Step(new Message({ from: "2", to: "1", type: MessageType.MsgHeartbeatResp }));
    const msgToResp = r.readMessages();

    t.equal(msgToResp.length, 1, `msgToResp.length = ${msgToResp.length}, want 1`);
    t.equal(msgToResp[0].index, 0, `index = ${msgToResp[0].index}, want 0`);
    t.ok(
        r.prs.progress.get("2")!.probeSent,
        `paused = ${r.prs.progress.get("2")!.probeSent}, want true`
    );

    t.end();
});

tap.test("raft testSendAppendForProgressReplicate", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();
    r.readMessages();
    r.prs.progress.get("2")!.BecomeReplicate();

    for (let i = 0; i < 10; i++) {
        mustAppendEntry(r, new Entry({ data: "somedata" }));
        r.sendAppend("2");

        const msgs = r.readMessages();
        t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    }

    t.end();
});

tap.test("raft testSendAppendForProgressSnapshot", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();
    r.readMessages();
    r.prs.progress.get("2")!.BecomeSnapshot(10);

    for (let i = 0; i < 10; i++) {
        mustAppendEntry(r, new Entry({ data: "somedata" }));
        r.sendAppend("2");
        const msgs = r.readMessages();

        t.equal(msgs.length, 0, `msgs.length = ${msgs.length}, want 0`);
    }

    t.end();
});

tap.test("raft testRecvMsgUnreachable", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 1, index: 2 }),
        new Entry({ term: 1, index: 3 }),
    ];
    const s = newTestMemoryStorage(withPeers("1", "2"));
    s.Append(previousEnts);
    const r = newTestRaft("1", 10, 1, s);
    r.becomeCandidate();
    r.becomeLeader();
    r.readMessages();
    // set node 2 to state replicate
    r.prs.progress.get("2")!.match = 3;
    r.prs.progress.get("2")!.BecomeReplicate();
    r.prs.progress.get("2")!.OptimisticUpdate(5);

    r.Step(new Message({ from: "2", to: "1", type: MessageType.MsgUnreachable }));

    t.equal(
        r.prs.progress.get("2")!.state,
        ProgressStateType.StateProbe,
        `state = ${ProgressStateType[r.prs.progress.get("2")!.state]}, want StateProbe`
    );
    const wnext = r.prs.progress.get("2")!.match + 1;
    t.equal(
        r.prs.progress.get("2")!.next,
        wnext,
        `next = ${r.prs.progress.get("2")!.next}, want ${wnext}`
    );

    t.end();
});

tap.test("raft testRestore", (t) => {
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({ voters: ["1", "2", "3"] }),
        }),
    });

    const storage = newTestMemoryStorage(withPeers("1", "2"));
    const sm = newTestRaft("1", 10, 1, storage);
    let ok = sm.restore(s);

    t.ok(ok, `restore = ${ok}, want true`);
    const lastIndex = sm.raftLog.lastIndex();
    t.equal(lastIndex, s.metadata.index, `log.lastindex = ${lastIndex}, want ${s.metadata.index}`);
    const term = mustTerm(...sm.raftLog.term(s.metadata.index));
    t.equal(term, s.metadata.term, `log.lastTerm = ${term}, want ${s.metadata.term}`);
    const sg = sm.prs.voterNodes();
    t.strictSame(
        sg,
        s.metadata.confState.voters,
        `sm.voters = ${sg}, want ${s.metadata.confState.voters}`
    );

    ok = sm.restore(s);
    t.notOk(ok, `restore = ${ok}, want false`);
    // It should not campaign before actually applying data.
    for (let i = 0; i < sm.randomizedElectionTimeout; i++) {
        sm.tick();
    }
    t.equal(
        sm.state,
        StateType.StateFollower,
        `state = ${StateType[sm.state]}, want StateFollower`
    );

    t.end();
});

// TestRestoreWithLearner restores a snapshot which contains learners.
tap.test("raft testRestoreWithLearner", (t) => {
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({ voters: ["1", "2"], learners: ["3"] }),
        }),
    });

    const storage = newTestMemoryStorage(withPeers("1", "2"), withLearners("3"));
    const sm = newTestLearnerRaft("3", 8, 2, storage);

    let ok = sm.restore(s);
    t.ok(ok, `restore: ok = ${ok}, want true`);

    const lastIndex = sm.raftLog.lastIndex();
    t.equal(
        lastIndex,
        s.metadata.index,
        `log.lastIndex = ${sm.raftLog.lastIndex()}, want ${s.metadata.index}`
    );
    const term = mustTerm(...sm.raftLog.term(s.metadata.index));
    t.equal(term, s.metadata.term, `log.lastTerm = ${term}, want ${s.metadata.term}`);

    const sg = sm.prs.voterNodes();
    t.equal(
        sg.length,
        s.metadata.confState.voters.length,
        `sm.Voters = ${sg}, length not equal with ${s.metadata.confState.voters}`
    );

    const learners = sm.prs.learnerNodes();
    t.equal(
        learners.length,
        s.metadata.confState.learners.length,
        `sm.LearnerNodes = ${learners}, length not equal with ${s.metadata.confState.learners}`
    );

    for (const voter of s.metadata.confState.voters) {
        t.notOk(
            sm.prs.progress.get(voter)!.isLearner,
            `sm.node ${voter} isLearner = ${sm.prs.progress.get(voter)!.isLearner}, want false`
        );
    }
    for (const learner of s.metadata.confState.learners) {
        t.ok(
            sm.prs.progress.get(learner)!.isLearner,
            `sm.node ${learner} isLearner = ${sm.prs.progress.get(learner)!.isLearner}, want true`
        );
    }

    ok = sm.restore(s);
    t.notOk(ok, `restore: ok = ${ok}, want false`);

    t.end();
});

// Tests if outgoing voter can receive and apply snapshot correctly.
tap.test("raft testRestoreWithVotersOutgoing", (t) => {
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({
                voters: ["2", "3", "4"],
                votersOutgoing: ["1", "2", "3"],
            }),
        }),
    });

    const storage = newTestMemoryStorage(withPeers("1", "2"));
    const sm = newTestRaft("1", 10, 1, storage);

    let ok = sm.restore(s);
    t.ok(ok, `restore: ok = ${ok}, want true`);

    const lastIndex = sm.raftLog.lastIndex();
    t.equal(lastIndex, s.metadata.index, `log.lastIndex = ${lastIndex}, want ${s.metadata.index}`);

    const term = mustTerm(...sm.raftLog.term(s.metadata.index));
    t.equal(term, s.metadata.term, `log.lastTerm = term, want ${s.metadata.term}`);

    const sg = sm.prs.voterNodes();
    const wsg = ["1", "2", "3", "4"];
    t.strictSame(sg, wsg, `sm.voters = ${sg}, want ${wsg}`);

    ok = sm.restore(s);
    t.notOk(ok, `restore: ok = ${ok}, want false`);

    // It should not campaign before actually applying data.
    for (let i = 0; i < sm.randomizedElectionTimeout; i++) {
        sm.tick();
    }
    t.equal(
        sm.state,
        StateType.StateFollower,
        `state = ${StateType[sm.state]}, want StateFollower`
    );

    t.end();
});

// TestRestoreVoterToLearner verifies that a normal peer can be downgraded to a
// learner through a snapshot. At the time of writing, we don't allow
// configuration changes to do this directly, but note that the snapshot may
// compress multiple changes to the configuration into one: the voter could have
// been removed, then readded as a learner and the snapshot reflects both
// changes. In that case, a voter receives a snapshot telling it that it is now
// a learner. In fact, the node has to accept that snapshot, or it is
// permanently cut off from the Raft log.
tap.test("raft testRestoreVoterToLearner", (t) => {
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({ voters: ["1", "2"], learners: ["3"] }),
        }),
    });

    const storage = newTestMemoryStorage(withPeers("1", "2", "3"));
    const sm = newTestRaft("3", 10, 1, storage);

    t.notOk(sm.isLearner, `${sm.id} isLearner = ${sm.isLearner}, want false`);
    const ok = sm.restore(s);
    t.ok(ok, `restore: ok = ${ok}, want true`);

    t.end();
});

// TestRestoreLearnerPromotion checks that a learner can become a follower after
// restoring snapshot.
tap.test("raft testRestoreLearnerPromotion", (t) => {
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({ voters: ["1", "2", "3"] }),
        }),
    });

    const storage = newTestMemoryStorage(withPeers("1", "2"), withLearners("3"));
    const sm = newTestLearnerRaft("3", 10, 1, storage);

    t.ok(sm.isLearner, `${sm.id} isLearner = ${sm.isLearner}, want true`);

    const ok = sm.restore(s);
    t.ok(ok, `restore: ok = ${ok}, want true`);

    t.notOk(sm.isLearner, `${sm.id} isLearner = ${sm.isLearner}, want false`);

    t.end();
});

// TestLearnerReceiveSnapshot tests that a learner can receive a snapshot from
// leader
tap.test("raft testLearnerReceiveSnapshot", (t) => {
    // restore the state machine from a snapshot so it has a compacted log and a
    // snapshot
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({ voters: ["1"], learners: ["2"] }),
        }),
    });

    const store = newTestMemoryStorage(withPeers("1"), withLearners("2"));
    const n1 = newTestLearnerRaft("1", 10, 1, store);
    const n2 = newTestLearnerRaft("2", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));

    n1.restore(s);
    const ready = newReady(n1, new SoftState(), new HardState());
    store.ApplySnapshot(ready.snapshot);
    n1.advance(ready);

    // Force set n1 applied index.
    n1.raftLog.appliedTo(n1.raftLog.committed);

    const nt = newNetwork(n1, n2);

    setRandomizedElectionTimeout(n1, n1.electionTimeout);
    for (let i = 0; i < n1.electionTimeout; i++) {
        n1.tick();
    }

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));

    t.equal(
        n2.raftLog.committed,
        n1.raftLog.committed,
        `peer 2 must commit to ${n1.raftLog.committed}, but is at ${n2.raftLog.committed}`
    );

    t.end();
});

tap.test("raft testRestoreIgnoreSnapshot", (t) => {
    const previousEnts: Entry[] = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 1, index: 2 }),
        new Entry({ term: 1, index: 3 }),
    ];
    const commit = 1;
    const storage = newTestMemoryStorage(withPeers("1", "2"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.raftLog.append(...previousEnts);
    sm.raftLog.commitTo(commit);

    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: commit,
            term: 1,
            confState: new ConfState({ voters: ["1", "2"] }),
        }),
    });

    // ignore snapshot.
    let ok = sm.restore(s);
    t.notOk(ok, `restore: ok = ${ok}, want false`);
    t.equal(sm.raftLog.committed, commit, `commit = ${sm.raftLog.committed}, want ${commit}`);

    // ignore snapshot and fast forward commit
    s.metadata.index = commit + 1;
    ok = sm.restore(s);
    t.notOk(ok, `restore: ok = ${ok}, want false`);
    t.equal(
        sm.raftLog.committed,
        commit + 1,
        `commit = ${sm.raftLog.committed}, want ${commit + 1}`
    );

    t.end();
});

tap.test("raft testProvideSnap", (t) => {
    // restore the state machine from a snapshot so it has a compacted log and a
    // snapshot
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({ voters: ["1", "2"] }),
        }),
    });

    const storage = newTestMemoryStorage(withPeers("1"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.restore(s);

    sm.becomeCandidate();
    sm.becomeLeader();

    // force set the next node 2, so that node 2 needs a snapshot
    sm.prs.progress.get("2")!.next = sm.raftLog.firstIndex();
    sm.Step(
        new Message({
            from: "2",
            to: "1",
            type: MessageType.MsgAppResp,
            index: sm.prs.progress.get("2")!.next - 1,
            reject: true,
        })
    );

    const msgs = sm.readMessages();

    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want 1`);
    const m = msgs[0];
    t.equal(m.type, MessageType.MsgSnap, `m.type = ${MessageType[m.type]}, want MsgSnap`);

    t.end();
});

tap.test("raft testIgnoreProvidingSnap", (t) => {
    // restore the state machine from a snapshot so it has a compacted log and a
    // snapshot
    const s = new Snapshot({
        metadata: new SnapshotMetadata({
            index: 11, // magic number
            term: 11, // magic number
            confState: new ConfState({ voters: ["1", "2"] }),
        }),
    });
    const storage = newTestMemoryStorage(withPeers("1"));
    const sm = newTestRaft("1", 10, 1, storage);
    sm.restore(s);

    sm.becomeCandidate();
    sm.becomeLeader();

    // force set the next of node 2, so that node 2 needs a snapshot change node
    // 2 to be inactive, expect node 1 ignore sending snapshot to 2
    sm.prs.progress.get("2")!.next = sm.raftLog.firstIndex() - 1;
    sm.prs.progress.get("2")!.recentActive = false;

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

// Commented out right now, since the comment from etcd/raft suggests this one
// is not complete tap.test("raft testRestoreFromSnapMsg", (t) => { const s =
// new Snapshot({ metadata: new SnapshotMetadata({ index: 11, // magic number
// term: 11, // magic number confState: new ConfState({ voters: ["1", "2"] }
//         }
//     };

//     const m: Message = { type: MessageType.MsgSnap, from: "1", term: 2,
//     snapshot: s };

//     const sm = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1",
//     "2"))); sm.Step(m);

//     t.equal(sm.lead, "1", `sm.lead = ${sm.lead}, want 1`);

//     // TO DO(bdarnell): what should this test?

//     t.end();
// });

tap.test("raft testSlowNodeRestore", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");
    for (let j = 0; j <= 100; j++) {
        send(
            nt,
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry()],
            })
        );
    }
    const lead = nt.peers.get("1") as RaftForTest;
    nextEnts(lead, nt.storage.get("1")!);
    nt.storage
        .get("1")!
        .CreateSnapshot(lead.raftLog.applied, new ConfState({ voters: lead.prs.voterNodes() }), []);
    nt.storage.get("1")!.Compact(lead.raftLog.applied);

    recover(nt);
    // send heartbeats so that the leader can learn everyone is active. node 3
    // will only be considered as active when node 1 receives a reply from it.
    let loopCounter = 0;
    while (true && loopCounter < 1000) {
        send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgBeat }));
        if (lead.prs.progress.get("3")!.recentActive) {
            break;
        }
        loopCounter++;
    }
    t.ok(
        loopCounter < 1000,
        `loopCounter should be < 1000, is ${loopCounter}. >= 1000 hints at infinite loop`
    );

    // trigger a snapshot.
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );

    const follower = nt.peers.get("3") as RaftForTest;

    // trigger a commit
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );
    t.equal(
        follower.raftLog.committed,
        lead.raftLog.committed,
        `follower committed = ${follower.raftLog.committed}, want ${lead.raftLog.committed}`
    );

    t.end();
});

// TestStepConfig tests that when raft step msgProp in EntryConfChange type, it
// appends the entry to log and sets pendingConf to be true.
tap.test("raft testStepConfig", (t) => {
    // a raft that cannot make progress
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();
    const index = r.raftLog.lastIndex();
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ type: EntryType.EntryConfChange })],
        })
    );

    const g = r.raftLog.lastIndex();
    t.equal(g, index + 1, `index = ${g}, want ${index + 1}`);
    t.equal(
        r.pendingConfIndex,
        index + 1,
        `pendingConfIndex = ${r.pendingConfIndex}, want ${index + 1}`
    );

    t.end();
});

// TestStepIgnoreConfig tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set the
// proposal to noop and keep its original state.
tap.test("raft testStepIgnoreConfig", (t) => {
    // a raft that cannot make progress
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.becomeCandidate();
    r.becomeLeader();
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ type: EntryType.EntryConfChange })],
        })
    );
    const index = r.raftLog.lastIndex();
    const pendingConfIndex = r.pendingConfIndex;
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ type: EntryType.EntryConfChange })],
        })
    );

    const wents: Entry[] = [new Entry({ type: EntryType.EntryNormal, term: 1, index: 3 })];
    const [ents, err] = r.raftLog.entries(index + 1, noLimit);

    t.equal(err, null, `error = ${err?.message}, want null`);
    t.strictSame(ents, wents, `ents = ${JSON.stringify(ents)}, want ${JSON.stringify(wents)}`);
    t.equal(
        r.pendingConfIndex,
        pendingConfIndex,
        `pendingConfIndex = ${r.pendingConfIndex}, want ${pendingConfIndex}`
    );

    t.end();
});

// TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
// based on uncommitted entries.
tap.test("raft testNewLeaderPendingConfig", (t) => {
    const tests: {
        addEntry: boolean;
        wpendingIndex: number;
    }[] = [
            { addEntry: false, wpendingIndex: 0 },
            { addEntry: true, wpendingIndex: 1 },
        ];

    tests.forEach((tt, i) => {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
        if (tt.addEntry) {
            mustAppendEntry(r, new Entry({ type: EntryType.EntryNormal }));
        }

        r.becomeCandidate();
        r.becomeLeader();

        t.equal(
            r.pendingConfIndex,
            tt.wpendingIndex,
            `#${i}: pendingConfIndex = ${r.pendingConfIndex}, want ${tt.wpendingIndex}`
        );
    });

    t.end();
});

// TestAddNode tests that addNode could update nodes correctly.
tap.test("raft testAddNode", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "2",
        }).asV2()
    );

    const nodes = r.prs.voterNodes();
    const wnodes = ["1", "2"];
    t.strictSame(nodes, wnodes, `nodes = ${nodes}, want ${wnodes}`);

    t.end();
});

// TestAddLearner tests that addLearner could update nodes correctly.
tap.test("raft testAddLearner", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
    // Add new learner peer.
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddLearnerNode,
            nodeId: "2",
        }).asV2()
    );

    t.notOk(r.isLearner, `r.islearner = ${r.isLearner}, want false`);

    const nodes = r.prs.learnerNodes();
    const wnodes = ["2"];
    t.strictSame(nodes, wnodes, `nodes = ${nodes}, want ${wnodes}`);
    t.ok(
        r.prs.progress.get("2")!.isLearner,
        `2 isLearner = ${r.prs.progress.get("2")!.isLearner}, want false`
    );

    // Promote peer to voter.
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "2",
        }).asV2()
    );
    t.notOk(
        r.prs.progress.get("2")!.isLearner,
        `2 isLearner = ${r.prs.progress.get("2")!.isLearner}, want false`
    );

    // Demote r.
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddLearnerNode,
            nodeId: "1",
        }).asV2()
    );
    t.ok(
        r.prs.progress.get("1")!.isLearner,
        `1 isLearner = ${r.prs.progress.get("1")!.isLearner}, want true`
    );
    t.ok(r.isLearner, `r.isLearner = ${r.isLearner}, want true`);

    // Promote r again.
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "1",
        }).asV2()
    );
    t.notOk(
        r.prs.progress.get("1")!.isLearner,
        `1 isLearner = ${r.prs.progress.get("1")!.isLearner}, want false`
    );
    t.notOk(r.isLearner, `r.isLearner = ${r.isLearner}, want false`);

    t.end();
});

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
tap.test("raft testAddNodeCheckQuorum", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
    r.checkQuorum = true;

    r.becomeCandidate();
    r.becomeLeader();

    for (let i = 0; i < r.electionTimeout - 1; i++) {
        r.tick();
    }
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: "2",
        }).asV2()
    );

    // This tick will reach electionTimeout, which triggers a quorum check.
    r.tick();

    // Node 1 should still be the leader after a single tick.
    t.equal(r.state, StateType.StateLeader, `state = ${StateType[r.state]}, want StateLeader`);

    // After another electionTimeout ticks without hearing from node 2, node 1
    // should step down.
    for (let i = 0; i < r.electionTimeout; i++) {
        r.tick();
    }

    t.equal(r.state, StateType.StateFollower, `state = ${StateType[r.state]}, want StateFollower`);

    t.end();
});

// TestRemoveNode tests that removeNode could update nodes and and removed list
// correctly.
tap.test("raft testRemoveNode", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2")));
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: "2",
        }).asV2()
    );
    const w = ["1"];
    const g = r.prs.voterNodes();
    t.strictSame(g, w, `nodes = ${g}, want ${w}`);

    // Removing the remaining voter should generate an error
    const [_, err] = r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: "1",
        }).asV2()
    );

    if (err === null) {
        // Above method call should generate an error.
        t.fail("No error generated upon recovery, node was not removed correctly");
    } else {
        // Expected case, all is well.
        t.pass(`Removing the last remaining voter lead to an error as expected`);
    }

    t.end();
});

// TestRemoveLearner tests that removeNode could update nodes and and removed
// list correctly.
tap.test("raft testRemoveLearner", (t) => {
    const r = newTestLearnerRaft("1", 10, 1, newTestMemoryStorage(withPeers("1"), withLearners("2")));
    r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: "2",
        }).asV2()
    );
    let w = ["1"];
    let g = r.prs.voterNodes();
    t.strictSame(g, w, `nodes = ${g}, want ${w}`);

    w = [];
    g = r.prs.learnerNodes();
    t.strictSame(g, w, `nodes = ${g}, want ${w}`);

    // Removing the remaining voter will generate an error
    const [_, err] = r.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: "1",
        }).asV2()
    );

    if (err === null) {
        // Above method call should generate an error.
        t.fail("No error generated upon recovery, node was not removed correctly");
    } else {
        // Expected case, all is well.
        t.pass(`Removing the last remaining voter lead to an error as expected`);
    }

    t.end();
});

tap.test("raft testPromotable", (t) => {
    const id = "1";
    const tests: { peers: rid[]; wp: boolean }[] = [
        { peers: ["1"], wp: true },
        { peers: ["1", "2", "3"], wp: true },
        { peers: [], wp: false },
        { peers: ["2", "3"], wp: false },
    ];

    tests.forEach((tt, i) => {
        const r = newTestRaft(id, 5, 1, newTestMemoryStorage(withPeers(...tt.peers)));
        const g = r.promotable();

        t.equal(g, tt.wp, `#${i}: promotable = ${g}, want ${tt.wp}`);
    });

    t.end();
});

tap.test("raft testRaftNodes", (t) => {
    const tests: { ids: rid[]; wids: rid[] }[] = [
        { ids: ["1", "2", "3"], wids: ["1", "2", "3"] },
        { ids: ["3", "2", "1"], wids: ["1", "2", "3"] },
    ];

    tests.forEach((tt, i) => {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers(...tt.ids)));

        const g = r.prs.voterNodes();
        t.strictSame(g, tt.wids, `#${i}: nodes = ${g}, want ${tt.wids}`);
    });

    t.end();
});

tap.test("raft testCampaignWhileLeader", (t) => {
    function testCampaignWhileLeader(preVote: boolean) {
        const cfg = newTestConfig("1", 5, 1, newTestMemoryStorage(withPeers("1")));
        cfg.preVote = preVote;

        const r = new RaftForTest(cfg);

        t.equal(
            r.state,
            StateType.StateFollower,
            `new node state = ${StateType[r.state]}, want StateFollower`
        );
        // We don't call campaign() directly because it comes after the check
        // for our current state.
        r.Step(new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        t.equal(
            r.state,
            StateType.StateLeader,
            `node state = ${StateType[r.state]}, want StateLeader`
        );

        const term = r.term;
        r.Step(new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        t.equal(
            r.state,
            StateType.StateLeader,
            `node state = ${StateType[r.state]}, want StateLeader`
        );
        t.equal(r.term, term, `expected to remain in term ${term}, got ${r.term}`);
    }

    testCampaignWhileLeader(false);
    testCampaignWhileLeader(true);

    t.end();
});

// TestCommitAfterRemoveNode verifies that pending commands can become committed
// when a config change reduces the quorum requirements
tap.test("raft testCommitAfterRemoveNode", (t) => {
    // Create a cluster with two nodes.
    const s = newTestMemoryStorage(withPeers("1", "2"));
    const r = newTestRaft("1", 5, 1, s);
    r.becomeCandidate();
    r.becomeLeader();
    // Begin to remove the second node.
    const cc = new ConfChange({
        type: ConfChangeType.ConfChangeRemoveNode,
        nodeId: "2",
    });
    const ccData = cc;

    r.Step(
        new Message({
            type: MessageType.MsgProp,
            entries: [new Entry({ type: EntryType.EntryConfChange, data: ccData })],
        })
    );
    // Stabilize the log and make sure nothing is committed yet.
    let ents = nextEnts(r, s);
    t.equal(ents.length, 0, `wanted 0 committed entries, got ${ents.length}`);
    const ccIndex = r.raftLog.lastIndex();

    // While the config change is pending, make another proposal.
    r.Step(
        new Message({
            type: MessageType.MsgProp,
            entries: [new Entry({ type: EntryType.EntryNormal, data: "hello" })],
        })
    );
    // Node 2 acknowledges the config change, committing it.
    r.Step(new Message({ type: MessageType.MsgAppResp, from: "2", index: ccIndex }));
    ents = nextEnts(r, s);
    t.equal(ents.length, 2, `got ${ents.length} entries, want 2`);
    t.equal(
        ents[0].type,
        EntryType.EntryNormal,
        `got ${EntryType[ents[0].type]}, want EntryNormal`
    );
    t.equal(ents[0].data, emptyRaftData, `want ents[0].data to be empty, got ${ents[0].data}`);
    t.equal(
        ents[1].type,
        EntryType.EntryConfChange,
        `got ${EntryType[ents[1].type]}, want EntryConfChange`
    );

    // Apply the config change. This reduces quorum requirements so the pending
    // command can now commit.
    r.applyConfChange(cc.asV2());
    ents = nextEnts(r, s);
    t.equal(ents.length, 1, `got ${ents.length} entries, want 1`);
    t.equal(
        ents[0].type,
        EntryType.EntryNormal,
        `got ${EntryType[ents[0].type]}, want EntryNormal`
    );
    t.equal(ents[0].data, "hello", `got data = "${ents[0].data}", want "hello"`);

    t.end();
});

// TestLeaderTransferToUpToDateNode verifies transferring should succeed if the
// transferee has the most up-to-date log entries when transfer starts.
tap.test("raft testLeaderTransferToUpToDateNode", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const lead = nt.peers.get("1") as RaftForTest;

    t.equal(lead.lead, "1", `after election leader is ${lead.lead}, want 1`);

    // Transfer leadership to 2.
    send(nt, new Message({ from: "2", to: "1", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateFollower, "2");

    // After some log replication, transfer leadership back to 1.
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );
    send(nt, new Message({ from: "1", to: "2", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should
// succeed if the transferee has the most up-to-date log entries when transfer
// starts. Not like TestLeaderTransferToUpToDateNode, where the leader transfer
// message is sent to the leader, in this test case every leader transfer
// message is sent to the follower.
tap.test("raft testLeaderTransferToUpToDateNodeFromFollower", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const lead = nt.peers.get("1") as RaftForTest;

    t.equal(lead.lead, "1", `after election leader is ${lead.lead}, want 1`);

    // Transfer leadership to 2.
    send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateFollower, "2");

    // After sending some log replication, transfer leadership back to 1.
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
tap.test("raft testLeaderTransferWithCheckQuorum", (t) => {
    const nt = newNetwork(null, null, null);
    for (let i = 0; i < 4; i++) {
        const r = nt.peers.get("1") as RaftForTest;
        r.checkQuorum = true;
        setRandomizedElectionTimeout(r, r.electionTimeout + i);
    }

    // Letting peer 2 electionElapsed reach to timeout so that it can vote for
    // peer 1
    const f = nt.peers.get("2") as RaftForTest;
    for (let i = 0; i < f.electionTimeout; i++) {
        f.tick();
    }

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const lead = nt.peers.get("1") as RaftForTest;
    t.equal(lead.lead, "1", `after election leader is ${lead.lead}, want 1`);

    // Transfer leadership to 2.
    send(nt, new Message({ from: "2", to: "1", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateFollower, "2");

    // After some log replication, transfer leadership back to 1.
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );

    send(nt, new Message({ from: "1", to: "2", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

tap.test("raft testLeaderTransferToSlowFollower", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );

    recover(nt);
    const lead = nt.peers.get("1")! as RaftForTest;
    const match = lead.prs.progress.get("3")!.match;
    t.equal(match, 1, `node 1 has match ${match} for node 3, want 1`);

    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateFollower, "3");

    t.end();
});

tap.test("raft testLeaderTransferAfterSnapshot", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgProp, entries: [new Entry()] }));
    const lead = nt.peers.get("1") as RaftForTest;
    nextEnts(lead, nt.storage.get("1")!);
    nt.storage
        .get("1")!
        .CreateSnapshot(
            lead.raftLog.applied,
            new ConfState({ voters: lead.prs.voterNodes() }),
            null
        );

    nt.storage.get("1")!.Compact(lead.raftLog.applied);
    recover(nt);
    const match = lead.prs.progress.get("3")!.match;
    t.equal(match, 1, `node 1 has match ${match} for node 3, want 1`);

    let filtered: Message = new Message();
    // Snapshot needs to be applied before sending MsgApResp
    nt.msgHook = (m: Message): boolean => {
        if (m.type !== MessageType.MsgAppResp || m.from !== "3" || m.reject) {
            return true;
        }
        filtered = m;
        return false;
    };

    // Transfer leadership to 3 when node 3 lacks the snapshot.
    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.state,
        StateType.StateLeader,
        `node 1 has state: ${StateType[lead.state]}, should still be StateLeader`
    );
    t.notSame(
        filtered,
        new Message(),
        `Follower should report snapshot progress automatically. Got ${JSON.stringify(
            filtered
        )}, want ${JSON.stringify(new Message())}`
    );

    // Apply snapshot and resume progress
    const follower = nt.peers.get("3") as RaftForTest;
    const ready = newReady(follower, new SoftState(), new HardState());
    nt.storage.get("3")!.ApplySnapshot(ready.snapshot);
    follower.advance(ready);
    nt.msgHook = null;
    send(nt, filtered);

    checkLeaderTransferState(t, lead, StateType.StateFollower, "3");

    t.end();
});

tap.test("raft testLeaderTransferToSelf", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const lead = nt.peers.get("1") as RaftForTest;

    // Transfer leadership to self, there will be noop.
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgTransferLeader }));
    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

tap.test("raft testLeaderTransferToNonExistingNode", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    const lead = nt.peers.get("1") as RaftForTest;
    // Transfer leadership to non-existing node, there will be noop.
    send(nt, new Message({ from: "4", to: "1", type: MessageType.MsgTransferLeader }));
    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

tap.test("raft testLeaderTransferTimeout", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");

    const lead = nt.peers.get("1") as RaftForTest;

    // Transfer leadership to isolated node, wait for timeout.
    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));

    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    for (let i = 0; i < lead.heartbeatTimeout; i++) {
        lead.tick();
    }

    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    for (let i = 0; i < lead.electionTimeout - lead.heartbeatTimeout; i++) {
        lead.tick();
    }

    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

tap.test("raft testLeaderTransferIgnoreProposal", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");

    const lead = nt.peers.get("1") as RaftForTest;

    // Transfer leadership to isolated node to let transfer pending, then send
    // proposal.
    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );
    const err = lead.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry()],
        })
    );

    t.strictSame(
        err,
        errProposalDropped,
        `got ${err?.message}, should return drop proposal error: ${errProposalDropped.message}`
    );
    const match = lead.prs.progress.get("1")!.match;
    t.equal(match, 1, `node 1 has match ${match}, want 1`);

    t.end();
});

tap.test("raft testLeaderTransferReceiveHigherTermVote", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");

    const lead = nt.peers.get("1") as RaftForTest;

    // Transfer leadership to isolated node to let transfer pending
    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    send(
        nt,
        new Message({
            from: "2",
            to: "2",
            type: MessageType.MsgHup,
            index: 1,
            term: 2,
        })
    );

    checkLeaderTransferState(t, lead, StateType.StateFollower, "2");

    t.end();
});

tap.test("raft testLeaderTransferRemoveNode", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    ignore(nt, MessageType.MsgTimeoutNow);

    const lead = nt.peers.get("1") as RaftForTest;

    // The leadTransferee is removed when leadership transferring.
    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    lead.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: "3",
        }).asV2()
    );

    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

tap.test("raft testLeaderTransferDemoteNode", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    ignore(nt, MessageType.MsgTimeoutNow);

    const lead = nt.peers.get("1") as RaftForTest;

    // The leadTransferee is demoted when leadership transferring
    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    lead.applyConfChange(
        new ConfChangeV2({
            changes: [
                {
                    type: ConfChangeType.ConfChangeRemoveNode,
                    nodeId: "3",
                },
                {
                    type: ConfChangeType.ConfChangeAddLearnerNode,
                    nodeId: "3",
                },
            ],
        })
    );

    // Make the Raft group commit the LeaveJoint entry.
    lead.applyConfChange(new ConfChangeV2());
    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

// TestLeaderTransferBack verifies leadership can transfer back to self when
// last transfer is pending.
tap.test("raft testLeaderTransferBack", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");

    const lead = nt.peers.get("1") as RaftForTest;

    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    // Transfer leadership back to self.
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to
// another node when last transfer is pending.
tap.test("raft testLeaderTransferSecondTransferToAnotherNode", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");

    const lead = nt.peers.get("1") as RaftForTest;

    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    // Transfer leadership to another node.
    send(nt, new Message({ from: "2", to: "1", type: MessageType.MsgTransferLeader }));

    checkLeaderTransferState(t, lead, StateType.StateFollower, "2");

    t.end();
});

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader
// request to the same node should not extend the timeout while the first one is
// pending.
tap.test("raft TestLeaderTransferSecondTransferToSameNode", (t) => {
    const nt = newNetwork(null, null, null);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    isolate(nt, "3");

    const lead = nt.peers.get("1") as RaftForTest;

    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));
    t.equal(
        lead.leadTransferee,
        "3",
        `wait transferring, leadTransferee = ${lead.leadTransferee}, want 3`
    );

    for (let i = 0; i < lead.heartbeatTimeout; i++) {
        lead.tick();
    }

    // Second transfer leadership request to the same node.
    send(nt, new Message({ from: "3", to: "1", type: MessageType.MsgTransferLeader }));

    for (let i = 0; i < lead.electionTimeout - lead.heartbeatTimeout; i++) {
        lead.tick();
    }

    checkLeaderTransferState(t, lead, StateType.StateLeader, "1");

    t.end();
});

function checkLeaderTransferState(t: any, r: RaftForTest, state: StateType, lead: rid) {
    t.equal(
        r.state,
        state,
        `after transferring, node has state ${StateType[r.state]}, want state ${StateType[state]}`
    );
    t.equal(r.lead, lead, `After transferring, lead is ${r.lead}, want ${lead}`);
    t.equal(
        r.leadTransferee,
        ridnone,
        `after transferring, node has leadTransferee ${r.leadTransferee}, want leadTransferee ${ridnone}`
    );
}

// TestTransferNonMember verifies that when a MsgTimeoutNow arrives at a node
// that has been removed from the group, nothing happens. (previously, if the
// node also got votes, it would panic as it transitioned to StateLeader)
tap.test("raft testTransferNonMember", (t) => {
    const r = newTestRaft("1", 5, 1, newTestMemoryStorage(withPeers("2", "3", "4")));
    r.Step(new Message({ from: "2", to: "1", type: MessageType.MsgTimeoutNow }));

    r.Step(new Message({ from: "2", to: "1", type: MessageType.MsgVoteResp }));
    r.Step(new Message({ from: "3", to: "1", type: MessageType.MsgVoteResp }));

    t.equal(r.state, StateType.StateFollower, `state is ${StateType[r.state]}, want StateFollower`);

    t.end();
});

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away. Previously the
// cluster would come to a standstill when run with PreVote enabled.
tap.test("raft testNodeWithSmallerTermCanCompleteElection", (t) => {
    const n1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n3 = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);
    n3.becomeFollower(1, ridnone);

    n1.preVote = true;
    n2.preVote = true;
    n3.preVote = true;

    // Cause a network partition to isolate node 3
    const nt = newNetwork(n1, n2, n3);
    cut(nt, "1", "3");
    cut(nt, "2", "3");

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    let sm = nt.peers.get("1") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateLeader,
        `peer 1 state: ${StateType[sm.state]}, want StateLeader`
    );

    sm = nt.peers.get("2") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateFollower,
        `peer 2 state: ${StateType[sm.state]}, want StateFollower`
    );

    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));
    sm = nt.peers.get("3") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StatePreCandidate,
        `peer 3 state: ${StateType[sm.state]}, want StatePreCandidate`
    );

    send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    // check whether the term values are expected a.Term == 3 b.Term == 3 c.Term
    // == 1
    sm = nt.peers.get("1") as RaftForTest;
    t.equal(sm.term, 3, `peer 1 term: ${sm.term}, want 3`);

    sm = nt.peers.get("2") as RaftForTest;
    t.equal(sm.term, 3, `peer 2 term: ${sm.term}, want 3`);

    sm = nt.peers.get("3") as RaftForTest;
    t.equal(sm.term, 1, `peer 3 term: ${sm.term}, want 1`);

    // check state a == follower b == leader c == pre-candidate
    sm = nt.peers.get("1") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateFollower,
        `peer 1 state ${StateType[sm.state]}, want StateFollower`
    );

    sm = nt.peers.get("2") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateLeader,
        `peer 2 state ${StateType[sm.state]}, want StateLeader`
    );

    sm = nt.peers.get("3") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StatePreCandidate,
        `peer 3 state ${StateType[sm.state]}, want StatePreCandidate`
    );

    sm.logger.infof("going to bring back peer 3 and kill peer 2");

    // recover the network then immediately isolate b which is currently the
    // leader, this is to emulate the crash of b.
    recover(nt);
    cut(nt, "2", "1");
    cut(nt, "2", "3");

    // call for election
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // do we have a leader?
    const sma = nt.peers.get("1") as RaftForTest;
    const smb = nt.peers.get("3") as RaftForTest;
    const atLeastOneLeader =
        sma.state === StateType.StateLeader || smb.state === StateType.StateLeader;
    t.ok(atLeastOneLeader, `At least one leader, got ${atLeastOneLeader}, want true`);

    t.end();
});

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
tap.test("raft testPreVoteWithSplitVote", (t) => {
    const n1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n3 = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);
    n3.becomeFollower(1, ridnone);

    n1.preVote = true;
    n2.preVote = true;
    n3.preVote = true;

    const nt = newNetwork(n1, n2, n3);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // simulate leader down. follower start split vote.
    isolate(nt, "1");
    send(
        nt,
        new Message({ from: "2", to: "2", type: MessageType.MsgHup }),
        new Message({ from: "3", to: "3", type: MessageType.MsgHup })
    );

    // check whether the term values are expected n2.Term == 3 n3.Term == 3
    let sm = nt.peers.get("2") as RaftForTest;
    t.equal(sm.term, 3, `peer 2 term: ${sm.term}, want 3`);

    sm = nt.peers.get("3") as RaftForTest;
    t.equal(sm.term, 3, `peer 3 term: ${sm.term}, want 3`);

    // check state n2 == candidate n3 == candidate
    sm = nt.peers.get("2") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateCandidate,
        `peer 2 state: ${StateType[sm.state]}, want StateCandidate`
    );

    sm = nt.peers.get("3") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateCandidate,
        `peer 3 state: ${StateType[sm.state]}, want StateCandidate`
    );

    // node 2 election timeout first
    send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    // check whether the term values are expected n2.Term == 4 n3.Term == 4
    sm = nt.peers.get("2") as RaftForTest;
    t.equal(sm.term, 4, `peer 2 term: ${sm.term}, want 4`);

    sm = nt.peers.get("3") as RaftForTest;
    t.equal(sm.term, 4, `peer 3 term: ${sm.term}, want 4`);

    // check state n2 == leader n3 == follower
    sm = nt.peers.get("2") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateLeader,
        `peer 2 state: ${StateType[sm.state]}, want StateLeader`
    );

    sm = nt.peers.get("3") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateFollower,
        `peer 3 state: ${StateType[sm.state]}, want StateFollower`
    );

    t.end();
});

// TestPreVoteWithCheckQuorum ensures that after a node become pre-candidate, it
// will checkQuorum correctly.
tap.test("raft testPreVoteWithCheckQuorum", (t) => {
    const n1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n3 = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);
    n3.becomeFollower(1, ridnone);

    n1.preVote = true;
    n2.preVote = true;
    n3.preVote = true;

    n1.checkQuorum = true;
    n2.checkQuorum = true;
    n3.checkQuorum = true;

    const nt = newNetwork(n1, n2, n3);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // isolate node 1. node 2 and node 3 have leader info
    isolate(nt, "1");

    // check state
    let sm = nt.peers.get("1") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateLeader,
        `peer 1 state: ${StateType[sm.state]}, want StateLeader`
    );

    sm = nt.peers.get("2") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateFollower,
        `peer 2 state: ${StateType[sm.state]}, want StateFollower`
    );

    sm = nt.peers.get("3") as RaftForTest;
    t.equal(
        sm.state,
        StateType.StateFollower,
        `peer 3 state: ${StateType[sm.state]}, want StateFollower`
    );

    // node 2 will ignore node 3's preVote
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));
    send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    // Do we have a leader?
    const haveNoLeader = n2.state !== StateType.StateLeader && n3.state !== StateType.StateFollower;
    t.notOk(haveNoLeader, `Got haveNoLeader = ${haveNoLeader}, want false`);

    t.end();
});

// TestLearnerCampaign verifies that a learner won't campaign even if it
// receives a MsgHup or MsgTimeoutNow.
tap.test("raft testLearnerCampaign", (t) => {
    const n1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1")));
    n1.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddLearnerNode,
            nodeId: "2",
        }).asV2()
    );
    const n2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1")));
    n2.applyConfChange(
        new ConfChange({
            type: ConfChangeType.ConfChangeAddLearnerNode,
            nodeId: "2",
        }).asV2()
    );

    const nt = newNetwork(n1, n2);
    send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    t.ok(n2.isLearner, `n2.isLearner = ${n2.isLearner}, want true`);
    t.equal(
        n2.state,
        StateType.StateFollower,
        `n2.state = ${StateType[n2.state]}, want StateFollower`
    );

    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
    t.equal(n1.state, StateType.StateLeader, `n1.state = ${StateType[n1.state]}, want StateLeader`);
    t.equal(n1.lead, "1", `n1.lead = ${n1.lead}, want 1`);

    // NB: TransferLeader already checks that the recipient is not a learner,
    // but the check could have happened by the time the recipient becomes a
    // learner, in which case it will receive MsgTimeoutNow as in this test case
    // and we verify that it's ignored.
    send(nt, new Message({ from: "1", to: "2", type: MessageType.MsgTimeoutNow }));

    t.equal(
        n2.state,
        StateType.StateFollower,
        `n2.state = ${StateType[n2.state]}, want StateFollower`
    );

    t.end();
});

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2,
// n3]. n1 is leader with term 2 n2 is follower with term 2 n3 is partitioned,
// with term 4 and less log, state is candidate
function newPreVoteMigrationCluster(t: any) {
    const n1 = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n2 = newTestRaft("2", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    const n3 = newTestRaft("3", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

    n1.becomeFollower(1, ridnone);
    n2.becomeFollower(1, ridnone);
    n3.becomeFollower(1, ridnone);

    n1.preVote = true;
    n2.preVote = true;
    // We intentionally do not enable PreVote for n3, this is done so in order
    // to simulate a rolling restart process where it's possible to have a mixed
    // version cluster with replicas with PreVote enabled, and replicas without.

    const nt = newNetwork(n1, n2, n3);
    send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));

    // Cause a network partition to isolate n3.
    isolate(nt, "3");
    send(
        nt,
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    // check state n1.state == StateLeader n2.state == StateFollower n3.state ==
    // StateCandidate
    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state = ${StateType[n1.state]}, want StateLeader`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state = ${StateType[n2.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StateCandidate,
        `node 3 state = ${StateType[n3.state]}, want StateCandidate`
    );

    // check term n1.Term == 2 n2.Term == 2 n3.Term == 4
    t.equal(n1.term, 2, `node 1 term = ${n1.term}, want 2`);
    t.equal(n2.term, 2, `node 2 term = ${n2.term}, want 2`);
    t.equal(n3.term, 4, `node 3 term = ${n3.term}, want 4`);

    // Enable prevote on n3, then recover the network
    n3.preVote = true;
    recover(nt);

    return nt;
}

tap.test("raft testPreVoteMigrationCanCompleteElection", (t) => {
    const nt = newPreVoteMigrationCluster(t);

    // n1 is leader with term 2 n2 is follower with term 2 n3 is pre-candidate
    // with term 4, and less log
    const n2 = nt.peers.get("2") as RaftForTest;
    const n3 = nt.peers.get("3") as RaftForTest;

    // simulate leader down
    isolate(nt, "1");

    // Call for elections from both n2 and n3.
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));
    send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    // check state n2.state == Follower n3.state == PreCandidate
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state = ${StateType[n2.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StatePreCandidate,
        `node 3 state = ${StateType[n3.state]}, want StatePreCandidate`
    );

    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));
    send(nt, new Message({ from: "2", to: "2", type: MessageType.MsgHup }));

    // Do we have a leader?
    const noLeader = n2.state !== StateType.StateLeader && n3.state !== StateType.StateFollower;
    t.notOk(noLeader, `noLeader = ${noLeader}, want false`);

    t.end();
});

tap.test("raft testPreVoteMigrationWithFreeStuckPreCandidate", (t) => {
    const nt = newPreVoteMigrationCluster(t);

    // n1 is leader with term 2 n2 is follower with term 2 n3 is pre-candidate
    // with term 4, and less log
    const n1 = nt.peers.get("1") as RaftForTest;
    const n2 = nt.peers.get("2") as RaftForTest;
    const n3 = nt.peers.get("3") as RaftForTest;

    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state = ${StateType[n1.state]}, want StateLeader`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state = ${StateType[n1.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StatePreCandidate,
        `node 3 state = ${StateType[n1.state]}, want StatePreCandidate`
    );

    // Pre-vote again for safety
    send(nt, new Message({ from: "3", to: "3", type: MessageType.MsgHup }));

    t.equal(
        n1.state,
        StateType.StateLeader,
        `node 1 state = ${StateType[n1.state]}, want StateLeader`
    );
    t.equal(
        n2.state,
        StateType.StateFollower,
        `node 2 state = ${StateType[n1.state]}, want StateFollower`
    );
    t.equal(
        n3.state,
        StateType.StatePreCandidate,
        `node 3 state = ${StateType[n1.state]}, want StatePreCandidate`
    );

    send(
        nt,
        new Message({
            from: "1",
            to: "3",
            type: MessageType.MsgHeartbeat,
            term: n1.term,
        })
    );

    // Disrupt the leader so that the stuck peer is freed
    t.equal(
        n1.state,
        StateType.StateFollower,
        `node 1 state = ${StateType[n1.state]}, want StateFollower`
    );
    t.equal(n3.term, n1.term, `node 3 term = ${n3.term}, want ${n1.term}`);

    t.end();
});

tap.test("raft testConfChangeCheckBeforeCampaign", (t) => {
    function testConfChangeCheckBeforeCampaign(v2: boolean) {
        const nt = newNetwork(null, null, null);
        const n1 = nt.peers.get("1") as RaftForTest;
        const n2 = nt.peers.get("2") as RaftForTest;

        send(nt, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        t.equal(
            n1.state,
            StateType.StateLeader,
            `node 1 state = ${StateType[n1.state]}, want StateLeader`
        );

        // Begin to remove the third node.
        const cc = new ConfChange({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: "2",
        });
        let ccData: RaftData;
        let ty: EntryType;
        if (v2) {
            const ccv2 = cc.asV2();
            ccData = ccv2;
            ty = EntryType.EntryConfChangeV2;
        } else {
            ccData = cc;
            ty = EntryType.EntryConfChange;
        }

        send(
            nt,
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ type: ty, data: ccData })],
            })
        );

        // Trigger campaign in node 2
        for (let i = 0; i < n2.randomizedElectionTimeout; i++) {
            n2.tick();
        }

        // It's still follower because committed conf change is not applied.
        t.equal(
            n2.state,
            StateType.StateFollower,
            `node 2 state = ${StateType[n2.state]}, want StateFollower`
        );

        // Transfer leadership to peer 2.
        send(nt, new Message({ from: "2", to: "1", type: MessageType.MsgTransferLeader }));
        t.equal(
            n1.state,
            StateType.StateLeader,
            `node 1 state = ${StateType[n1.state]}, want StateLeader`
        );
        t.equal(
            n2.state,
            StateType.StateFollower,
            `node 2 state = ${StateType[n2.state]}, want StateFollower`
        );
        // Abort transfer leader
        for (let i = 0; i < n1.electionTimeout; i++) {
            n1.tick();
        }

        // Advance apply
        nextEnts(n2, nt.storage.get("2")!);

        // Transfer leadership to peer 2 again
        send(nt, new Message({ from: "2", to: "1", type: MessageType.MsgTransferLeader }));
        t.equal(
            n1.state,
            StateType.StateFollower,
            `node 1 state = ${StateType[n1.state]}, want StateFollower`
        );
        t.equal(
            n2.state,
            StateType.StateLeader,
            `node 2 state = ${StateType[n2.state]}, want StateLeader`
        );

        nextEnts(n1, nt.storage.get("1")!);
        // Trigger campaign in node 2
        for (let i = 0; i < n1.randomizedElectionTimeout; i++) {
            n1.tick();
        }
        t.equal(
            n1.state,
            StateType.StateCandidate,
            `node 1 state = ${StateType[n1.state]}, want StateCandidate`
        );
    }

    // Tests if unapplied ConfChange is checked before campaign.
    testConfChangeCheckBeforeCampaign(false);
    // Tests if unapplied ConfChangeV2 is checked before campaign.
    testConfChangeCheckBeforeCampaign(true);

    t.end();
});

tap.test("raft testFastLogRejection", (t) => {
    const tests: {
        leaderLog: Entry[]; // Logs on the leader
        followerLog: Entry[]; // Logs on the follower
        rejectHintTerm: number; // Expected term included in rejected MsgAppResp.
        rejectHintIndex: number; // Expected index included in rejected MsgAppResp.
        nextAppendTerm: number; // Expected term when leader appends after rejected.
        nextAppendIndex: number; // Expected index when leader appends after rejected.
    }[] = [
            // This case tests that leader can find the conflict index quickly.
            // Firstly leader appends (type=MsgApp,index=7,logTerm=4,
            // entries=...); After rejected leader appends
            // (type=MsgApp,index=3,logTerm=2).
            {
                leaderLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                    new Entry({ term: 4, index: 5 }),
                    new Entry({ term: 4, index: 6 }),
                    new Entry({ term: 4, index: 7 }),
                ],
                followerLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 3, index: 4 }),
                    new Entry({ term: 3, index: 5 }),
                    new Entry({ term: 3, index: 6 }),
                    new Entry({ term: 3, index: 7 }),
                    new Entry({ term: 3, index: 8 }),
                    new Entry({ term: 3, index: 9 }),
                    new Entry({ term: 3, index: 10 }),
                    new Entry({ term: 3, index: 11 }),
                ],
                rejectHintTerm: 3,
                rejectHintIndex: 7,
                nextAppendTerm: 2,
                nextAppendIndex: 3,
            },
            // This case tests that leader can find the conflict index quickly.
            // Firstly leader appends (type=MsgApp,index=8,logTerm=5,
            // entries=...); After rejected leader appends
            // (type=MsgApp,index=4,logTerm=3).
            {
                leaderLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 3, index: 4 }),
                    new Entry({ term: 4, index: 5 }),
                    new Entry({ term: 4, index: 6 }),
                    new Entry({ term: 4, index: 7 }),
                    new Entry({ term: 5, index: 8 }),
                ],
                followerLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 3, index: 4 }),
                    new Entry({ term: 3, index: 5 }),
                    new Entry({ term: 3, index: 6 }),
                    new Entry({ term: 3, index: 7 }),
                    new Entry({ term: 3, index: 8 }),
                    new Entry({ term: 3, index: 9 }),
                    new Entry({ term: 3, index: 10 }),
                    new Entry({ term: 3, index: 11 }),
                ],
                rejectHintTerm: 3,
                rejectHintIndex: 8,
                nextAppendTerm: 3,
                nextAppendIndex: 4,
            },
            // This case tests that follower can find the conflict index
            // quickly. Firstly leader appends (type=MsgApp,index=4,logTerm=1,
            // entries=...); After rejected leader appends
            // (type=MsgApp,index=1,logTerm=1).
            {
                leaderLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 1, index: 2 }),
                    new Entry({ term: 1, index: 3 }),
                    new Entry({ term: 1, index: 4 }),
                ],
                followerLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                ],
                rejectHintTerm: 1,
                rejectHintIndex: 1,
                nextAppendTerm: 1,
                nextAppendIndex: 1,
            },
            // This case is similar to the previous case. However, this time,
            // the leader has a longer uncommitted log tail than the follower.
            // Firstly leader appends (type=MsgApp,index=6,logTerm=1,
            // entries=...); After rejected leader appends
            // (type=MsgApp,index=1,logTerm=1).
            {
                leaderLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 1, index: 2 }),
                    new Entry({ term: 1, index: 3 }),
                    new Entry({ term: 1, index: 4 }),
                    new Entry({ term: 1, index: 5 }),
                    new Entry({ term: 1, index: 6 }),
                ],
                followerLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                ],
                rejectHintTerm: 1,
                rejectHintIndex: 1,
                nextAppendTerm: 1,
                nextAppendIndex: 1,
            },
            // This case is similar to the previous case. However, this time,
            // the follower has a longer uncommitted log tail than the leader.
            // Firstly leader appends (type=MsgApp,index=4,logTerm=1,
            // entries=...); After rejected leader appends
            // (type=MsgApp,index=1,logTerm=1).
            {
                leaderLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 1, index: 2 }),
                    new Entry({ term: 1, index: 3 }),
                    new Entry({ term: 1, index: 4 }),
                ],
                followerLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                    new Entry({ term: 4, index: 5 }),
                    new Entry({ term: 4, index: 6 }),
                ],
                rejectHintTerm: 1,
                rejectHintIndex: 1,
                nextAppendTerm: 1,
                nextAppendIndex: 1,
            },
            // A normal case that there are no log conflicts. Firstly leader
            // appends (type=MsgApp,index=5,logTerm=5, entries=...); After
            // rejected leader appends (type=MsgApp,index=4,logTerm=4).
            {
                leaderLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 1, index: 2 }),
                    new Entry({ term: 1, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                    new Entry({ term: 5, index: 5 }),
                ],
                followerLog: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 1, index: 2 }),
                    new Entry({ term: 1, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                ],
                rejectHintTerm: 4,
                rejectHintIndex: 4,
                nextAppendTerm: 4,
                nextAppendIndex: 4,
            },
            // Test case from example comment in stepLeader (on leader).
            {
                leaderLog: [
                    new Entry({ term: 2, index: 1 }),
                    new Entry({ term: 5, index: 2 }),
                    new Entry({ term: 5, index: 3 }),
                    new Entry({ term: 5, index: 4 }),
                    new Entry({ term: 5, index: 5 }),
                    new Entry({ term: 5, index: 6 }),
                    new Entry({ term: 5, index: 7 }),
                    new Entry({ term: 5, index: 8 }),
                    new Entry({ term: 5, index: 9 }),
                ],
                followerLog: [
                    new Entry({ term: 2, index: 1 }),
                    new Entry({ term: 4, index: 2 }),
                    new Entry({ term: 4, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                    new Entry({ term: 4, index: 5 }),
                    new Entry({ term: 4, index: 6 }),
                ],
                rejectHintTerm: 4,
                rejectHintIndex: 6,
                nextAppendTerm: 2,
                nextAppendIndex: 1,
            },
            // Test case from example comment in handleAppendEntries (on
            // follower).
            {
                leaderLog: [
                    new Entry({ term: 2, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 2, index: 3 }),
                    new Entry({ term: 2, index: 4 }),
                    new Entry({ term: 2, index: 5 }),
                ],
                followerLog: [
                    new Entry({ term: 2, index: 1 }),
                    new Entry({ term: 4, index: 2 }),
                    new Entry({ term: 4, index: 3 }),
                    new Entry({ term: 4, index: 4 }),
                    new Entry({ term: 4, index: 5 }),
                    new Entry({ term: 4, index: 6 }),
                    new Entry({ term: 4, index: 7 }),
                    new Entry({ term: 4, index: 8 }),
                ],
                rejectHintTerm: 2,
                rejectHintIndex: 1,
                nextAppendTerm: 2,
                nextAppendIndex: 1,
            },
        ];

    tests.forEach((tt, i) => {
        const s1 = MemoryStorage.NewMemoryStorage();
        s1.snapshot.metadata.confState = new ConfState({ voters: ["1", "2", "3"] });
        s1.Append(tt.leaderLog);
        const s2 = MemoryStorage.NewMemoryStorage();
        s2.snapshot.metadata.confState = new ConfState({ voters: ["1", "2", "3"] });
        s2.Append(tt.followerLog);

        const n1 = newTestRaft("1", 10, 1, s1);
        const n2 = newTestRaft("2", 10, 1, s2);

        n1.becomeCandidate();
        n1.becomeLeader();

        n2.Step(new Message({ from: "1", to: "1", type: MessageType.MsgHeartbeat }));

        let msgs = n2.readMessages();
        t.equal(msgs.length, 1, `#${i}: peer 2 msgs.length = ${msgs.length}, want 1`);
        t.equal(
            msgs[0].type,
            MessageType.MsgHeartbeatResp,
            `#${i}: peer 2 msgs[0].type = ${MessageType[msgs[0].type]}, want MsgHeartbeatResp`
        );
        let stepResult = n1.Step(msgs[0]);
        t.equal(
            stepResult,
            null,
            `#${i}: peer 1 step heartbeat response error: ${stepResult?.message}, want null`
        );

        msgs = n1.readMessages();
        t.equal(msgs.length, 1, `#${i}: peer 1 msgs.length = ${msgs.length}, want 1`);
        t.equal(
            msgs[0].type,
            MessageType.MsgApp,
            `peer 1 msgs[0].type = ${MessageType[msgs[0].type]}, want MsgApp`
        );
        stepResult = n2.Step(msgs[0]);
        t.equal(
            stepResult,
            null,
            `#${i}: peer 2 step append error: ${stepResult?.message}, want null`
        );

        msgs = n2.readMessages();
        t.equal(msgs.length, 1, `#${i}: peer 2 msgs.length = ${msgs.length}, want 1`);
        t.equal(
            msgs[0].type,
            MessageType.MsgAppResp,
            `peer 2 msgs[0].type = ${MessageType[msgs[0].type]}, want MsgAppResp`
        );

        t.ok(msgs[0].reject, `#${i}: peer 2 reject = ${msgs[0].reject}, want true`);
        t.equal(
            msgs[0].logTerm,
            tt.rejectHintTerm,
            `#${i}: expected hint log term = ${tt.rejectHintTerm}, got ${msgs[0].logTerm}`
        );
        t.equal(
            msgs[0].rejectHint,
            tt.rejectHintIndex,
            `#${i}: expected hint index: ${tt.rejectHintIndex}, got ${msgs[0].rejectHint}`
        );

        stepResult = n1.Step(msgs[0]);
        t.equal(
            stepResult,
            null,
            `#${i}: peer 1 step append error: ${stepResult?.message}, want null`
        );

        msgs = n1.readMessages();
        t.equal(
            msgs[0].logTerm,
            tt.nextAppendTerm,
            `#${i}: expected log term = ${tt.nextAppendTerm}, got ${msgs[0].logTerm}`
        );
        t.equal(
            msgs[0].index,
            tt.nextAppendIndex,
            `#${i}: expected index = ${tt.nextAppendIndex}, got ${msgs[0].index}`
        );
    });

    t.end();
});

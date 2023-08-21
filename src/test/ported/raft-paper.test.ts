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
import {
    idsBySize,
    mustAppendEntry,
    newNetwork,
    newTestMemoryStorage,
    newTestRaft,
    nopStepper,
    RaftForTest,
    send,
    withPeers
} from "./raft-test-util";
import { diffu, ltoa } from "./util";
import { emptyRaftData } from "../../non-ported/raft-controller";
import { defaultLogger, discardLogger, setLogger } from "../../ported/logger";
import { Raft, StateType } from "../../ported/raft";
import {
    Entry,
    HardState,
    Message,
    MessageType,
    rid,
    ridnone
} from "../../ported/raftpb/raft.pb";
import { MemoryStorage } from "../../ported/storage";
import { NullableError } from "../../ported/util";


// testUpdateTermFromMessage tests that if one server’s current term is smaller
// than the other’s, then it updates its current term to the larger value. If a
// candidate or leader discovers that its term is out of date, it immediately
// reverts to follower state. Reference: section 5.1
tap.test("Raft paper testUpdateFromMessage", (t) => {
    function testForStateType(state: StateType) {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

        switch (state) {
            case StateType.StateFollower:
                r.becomeFollower(1, "2");
                break;
            case StateType.StateCandidate:
                r.becomeCandidate();
                break;
            case StateType.StateLeader:
                r.becomeCandidate();
                r.becomeLeader();
        }

        r.Step(new Message({ type: MessageType.MsgApp, term: 2 }));

        t.equal(r.term, 2, `${StateType[state]}: want term = 2, got ${r.term}`);
        t.equal(
            r.state,
            StateType.StateFollower,
            `${StateType[state]}: want state = StateFollower, got ${StateType[r.state]}`
        );
    }

    testForStateType(StateType.StateFollower);
    testForStateType(StateType.StateCandidate);
    testForStateType(StateType.StateLeader);

    t.end();
});

// TestRejectStaleTermMessage tests that if a server receives a request with a
// stale term number, it rejects the request. Our implementation ignores the
// request instead. Reference: section 5.1
tap.test("Raft paper rejectStaleTermMessage", (t) => {
    let called = false;
    const fakeStep = (_: Raft, m: Message): NullableError => {
        called = true;
        return null;
    };

    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    r.step = fakeStep;
    r.loadState(new HardState({ term: 2 }));

    r.Step(new Message({ type: MessageType.MsgApp, term: r.term - 1 }));

    t.notOk(called, `stepFunc called = ${called}, want false`);

    t.end();
});

// TestStartAsFollower tests that when servers start up, they begin as
// followers. Reference: section 5.2
tap.test("Raft paper startAsFollower", (t) => {
    const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
    t.equal(r.state, StateType.StateFollower, `state = ${StateType[r.state]}, want StateFollower`);
    t.end();
});

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick, it
// will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries as
// heartbeat to all followers. Reference: section 5.2
tap.test("Raft paper leaderBcastBeat", (t) => {
    // heartbeat interval
    const hi = 1;
    const r = newTestRaft("1", 10, hi, newTestMemoryStorage(withPeers("1", "2", "3")));
    r.becomeCandidate();
    r.becomeLeader();

    for (let i = 0; i < 10; i++) {
        mustAppendEntry(r, new Entry({ index: i + 1 }));
    }

    for (let i = 0; i < hi; i++) {
        r.tick();
    }

    const msgs = r.readMessages();
    msgs.sort(msgSortFunction);

    const wantMsgs = [
        getMessage(
            new Message({
                from: "1",
                to: "2",
                term: 1,
                type: MessageType.MsgHeartbeat,
            })
        ),
        getMessage(
            new Message({
                from: "1",
                to: "3",
                term: 1,
                type: MessageType.MsgHeartbeat,
            })
        ),
    ];

    t.strictSame(msgs, wantMsgs, `msgs = ${JSON.stringify(msgs)}, want ${JSON.stringify(wantMsgs)}`);

    t.end();
});

// testNonLeaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then votes
// for itself and issues RequestVote RPCs in parallel to each of the other
// servers in the cluster. Reference: section 5.2 Also if a candidate fails to
// obtain a majority, it will time out and start a new election by incrementing
// its term and initiating another round of RequestVote RPCs. Reference: section
// 5.2
tap.test("Raft paper nonLeaderStartElection", (t) => {
    function testNonLeaderStartElection(state: StateType) {
        // election timeout
        const et = 10;
        const r = newTestRaft("1", et, 1, newTestMemoryStorage(withPeers("1", "2", "3")));

        switch (state) {
            case StateType.StateFollower:
                r.becomeFollower(1, "2");
                break;
            case StateType.StateCandidate:
                r.becomeCandidate();
                break;
        }

        for (let i = 1; i < 2 * et; i++) {
            r.tick();
        }

        t.equal(r.term, 2, `term = ${r.term}, want 2`);
        t.equal(
            r.state,
            StateType.StateCandidate,
            `state = ${StateType[r.state]}, want StateCandidate`
        );
        const rVote = r.prs.votes.get(r.id);
        t.ok(rVote, `vote for self = ${rVote}, want true`);

        const msgs = r.readMessages();
        msgs.sort(msgSortFunction);
        const wantMsgs: Message[] = [
            new Message({ from: "1", to: "2", term: 2, type: MessageType.MsgVote }),
            new Message({ from: "1", to: "3", term: 2, type: MessageType.MsgVote }),
        ];
        t.strictSame(
            msgs,
            wantMsgs,
            `msgs = ${JSON.stringify(msgs)}, want ${JSON.stringify(wantMsgs)}`
        );
    }

    testNonLeaderStartElection(StateType.StateFollower);
    testNonLeaderStartElection(StateType.StateCandidate);

    t.end();
});

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in leader
// election during one round of RequestVote RPC: a) it wins the election b) it
// loses the election c) it is unclear about the result Reference: section 5.2
tap.test("Raft paper leaderElectionInOneRoundRPC", (t) => {
    const tests: {
        size: number;
        votes: Map<rid, boolean>;
        state: StateType;
    }[] = [
            // win the election when receiving votes from a majority of the
            // servers
            {
                size: 1,
                votes: new Map<rid, boolean>(),
                state: StateType.StateLeader,
            },
            {
                size: 3,
                votes: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                ]),
                state: StateType.StateLeader,
            },
            {
                size: 3,
                votes: new Map<rid, boolean>([["2", true]]),
                state: StateType.StateLeader,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                    ["4", true],
                    ["5", true],
                ]),
                state: StateType.StateLeader,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                    ["4", true],
                ]),
                state: StateType.StateLeader,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                ]),
                state: StateType.StateLeader,
            },

            // return to follower state if it receives vote denial from a
            // majority
            {
                size: 3,
                votes: new Map<rid, boolean>([
                    ["2", false],
                    ["3", false],
                ]),
                state: StateType.StateFollower,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>([
                    ["2", false],
                    ["3", false],
                    ["4", false],
                    ["5", false],
                ]),
                state: StateType.StateFollower,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>([
                    ["2", true],
                    ["3", false],
                    ["4", false],
                    ["5", false],
                ]),
                state: StateType.StateFollower,
            },

            // stay in candidate if it does not obtain the majority
            {
                size: 3,
                votes: new Map<rid, boolean>(),
                state: StateType.StateCandidate,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>([["2", true]]),
                state: StateType.StateCandidate,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>([
                    ["2", false],
                    ["3", false],
                ]),
                state: StateType.StateCandidate,
            },
            {
                size: 5,
                votes: new Map<rid, boolean>(),
                state: StateType.StateCandidate,
            },
        ];

    tests.forEach((tt, i) => {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers(...idsBySize(tt.size))));

        r.Step(new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        for (const [id, vote] of tt.votes) {
            r.Step(
                new Message({
                    from: id,
                    to: "1",
                    term: r.term,
                    type: MessageType.MsgVoteResp,
                    reject: !vote,
                })
            );
        }

        t.equal(
            r.state,
            tt.state,
            `#${i}: state = ${StateType[r.state]}, want ${StateType[tt.state]}`
        );
        t.equal(r.term, 1, `#${i}: term = ${r.term}, want 1`);
    });

    t.end();
});

// TestFollowerVote tests that each follower will vote for at most one candidate
// in a given term, on a first-come-first-served basis. Reference: section 5.2
tap.test("Raft paper followerVote", (t) => {
    const tests: { vote: rid; nvote: rid; wreject: boolean }[] = [
        { vote: ridnone, nvote: "1", wreject: false },
        { vote: ridnone, nvote: "2", wreject: false },
        { vote: "1", nvote: "1", wreject: false },
        { vote: "2", nvote: "2", wreject: false },
        { vote: "1", nvote: "2", wreject: true },
        { vote: "2", nvote: "1", wreject: true },
    ];

    tests.forEach((tt, i) => {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        r.loadState(new HardState({ term: 1, vote: tt.vote }));

        r.Step(
            getMessage(
                new Message({
                    from: tt.nvote,
                    to: "1",
                    term: 1,
                    type: MessageType.MsgVote,
                })
            )
        );

        const msgs = r.readMessages();
        const wantMsgs: Message[] = [
            new Message({
                from: "1",
                to: tt.nvote,
                term: 1,
                type: MessageType.MsgVoteResp,
                reject: tt.wreject,
            }),
        ];

        t.strictSame(
            msgs,
            wantMsgs,
            `#${i}: msgs = ${JSON.stringify(msgs)}, want ${JSON.stringify(wantMsgs)}`
        );
    });

    t.end();
});

// TestCandidateFallback tests that while waiting for votes, if a candidate
// receives an AppendEntries RPC from another server claiming to be leader whose
// term is at least as large as the candidate's current term, it recognizes the
// leader as legitimate and returns to follower state. Reference: section 5.2
tap.test("Raft paper candidateFallback", (t) => {
    const tests: Message[] = [
        new Message({ from: "2", to: "1", term: 1, type: MessageType.MsgApp }),
        new Message({ from: "2", to: "1", term: 2, type: MessageType.MsgApp }),
    ];

    tests.forEach((tt, i) => {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        r.Step(new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        t.equal(
            r.state,
            StateType.StateCandidate,
            `#${i}: unexpected state = ${StateType[r.state]}, want StateCandidate`
        );

        r.Step(tt);

        t.equal(
            r.state,
            StateType.StateFollower,
            `#${i}: state = ${StateType[r.state]}, want StateFollower`
        );
        t.equal(r.term, tt.term, `#${i}: term = ${r.term}, want ${tt.term}`);
    });

    t.end();
});

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized. Reference: section 5.2
tap.test("Raft paper nonleaderElectionTimeoutRandomized", (t) => {
    function nonleaderElectionTimeoutRandomized(state: StateType) {
        const et = 10;
        const r = newTestRaft("1", et, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        const timeouts = new Map<number, boolean>();

        for (let round = 0; round < 50 * et; round++) {
            switch (state) {
                case StateType.StateFollower:
                    r.becomeFollower(r.term + 1, "2");
                    break;
                case StateType.StateCandidate:
                    r.becomeCandidate();
                    break;
            }

            let time = 0;
            while (r.readMessages().length === 0) {
                r.tick();
                time++;
            }
            timeouts.set(time, true);
        }

        for (let d = et + 1; d < 2 * et; d++) {
            const timeoutOccurred = timeouts.get(d);
            t.ok(timeoutOccurred, `timeout in ${d} ticks should happen`);
        }
    }

    setLogger(discardLogger);
    nonleaderElectionTimeoutRandomized(StateType.StateFollower);
    nonleaderElectionTimeoutRandomized(StateType.StateCandidate);
    setLogger(defaultLogger);

    t.end();
});

// testNonLeadersElectionTimeoutNonConflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election. Reference: section 5.2
tap.test("Raft paper nonLeadersElectionTimeoutNonConflict", (t) => {
    function nonLeadersElectionTimeoutNonConflict(state: StateType) {
        const et = 10;
        const size = 5;
        const rs: RaftForTest[] = [];
        const ids = idsBySize(size);
        for (let i = 0; i < size; i++) {
            rs.push(newTestRaft(ids[i], et, 1, newTestMemoryStorage(withPeers(...ids))));
        }
        let conflicts = 0;

        const numRounds = 1000;
        for (let round = 0; round < numRounds; round++) {
            for (const r of rs) {
                switch (state) {
                    case StateType.StateFollower:
                        r.becomeFollower(r.term + 1, ridnone);
                        break;
                    case StateType.StateCandidate:
                        r.becomeCandidate();
                        break;
                }
            }

            let timeoutNum = 0;
            while (timeoutNum === 0) {
                for (const r of rs) {
                    r.tick();
                    if (r.readMessages().length > 0) {
                        timeoutNum++;
                    }
                }
            }
            // several rafts time out at the same tick
            if (timeoutNum > 1) {
                conflicts++;
            }
        }

        const conflictProb = conflicts / numRounds;
        t.ok(conflictProb <= 0.3, `probability of conflicts = ${conflictProb}, want <= 0.3`);
    }

    setLogger(discardLogger);
    nonLeadersElectionTimeoutNonConflict(StateType.StateFollower);
    nonLeadersElectionTimeoutNonConflict(StateType.StateCandidate);
    setLogger(defaultLogger);

    t.end();
});

// TestLeaderStartReplication tests that when receiving client proposals, the
// leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate the
// entry. Also, when sending an AppendEntries RPC, the leader includes the index
// and term of the entry in its log that immediately precedes the new entries.
// Also, it writes the new entry into stable storage. Reference: section 5.3
tap.test("Raft paper leaderStartReplication", (t) => {
    const s = newTestMemoryStorage(withPeers("1", "2", "3"));
    const r = newTestRaft("1", 10, 1, s);
    r.becomeCandidate();
    r.becomeLeader();
    commitNoopEntry(r, s);
    const li = r.raftLog.lastIndex();

    const ents: Entry[] = [new Entry({ data: "some data" })];
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: ents,
        })
    );

    t.equal(r.raftLog.lastIndex(), li + 1, `lastIndex = ${r.raftLog.lastIndex()}, want ${li + 1}`);
    t.equal(r.raftLog.committed, li, `committed = ${r.raftLog.committed}, want ${li}`);

    const msgs = r.readMessages();
    msgs.sort(msgSortFunction);
    const wents: Entry[] = [new Entry({ index: li + 1, term: 1, data: "some data" })];
    const wantMsgs: Message[] = [
        new Message({
            from: "1",
            to: "2",
            term: 1,
            type: MessageType.MsgApp,
            index: li,
            logTerm: 1,
            entries: wents,
            commit: li,
        }),
        new Message({
            from: "1",
            to: "3",
            term: 1,
            type: MessageType.MsgApp,
            index: li,
            logTerm: 1,
            entries: wents,
            commit: li,
        }),
    ];

    t.strictSame(msgs, wantMsgs, `msgs = ${JSON.stringify(msgs)}, want ${JSON.stringify(wantMsgs)}`);
    const unstableEntries = r.raftLog.unstableEntries();
    t.strictSame(
        unstableEntries,
        wents,
        `ents = ${JSON.stringify(unstableEntries)}, want ${JSON.stringify(wents)}`
    );

    t.end();
});

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine. Also, the leader keeps track of the highest index it knows to be
// committed, and it includes that index in future AppendEntries RPCs so that
// the other servers eventually find out. Reference: section 5.3
tap.test("Raft paper leaderCommitEntry", (t) => {
    const s = newTestMemoryStorage(withPeers("1", "2", "3"));
    const r = newTestRaft("1", 10, 1, s);
    r.becomeCandidate();
    r.becomeLeader();
    commitNoopEntry(r, s);
    const li = r.raftLog.lastIndex();
    r.Step(
        new Message({
            from: "1",
            to: "1",
            type: MessageType.MsgProp,
            entries: [new Entry({ data: "some data" })],
        })
    );

    for (const m of r.readMessages()) {
        r.Step(acceptAndReply(m));
    }

    const committed = r.raftLog.committed;
    t.equal(committed, li + 1, `committed = ${committed}, want ${li + 1}`);

    const wents: Entry[] = [new Entry({ index: li + 1, term: 1, data: "some data" })];
    const nextEnts = r.raftLog.nextEnts();
    t.strictSame(
        nextEnts,
        wents,
        `nextEnts = ${JSON.stringify(nextEnts)}, want ${JSON.stringify(wents)}`
    );

    const msgs = r.readMessages();
    msgs.sort(msgSortFunction);

    msgs.forEach((m, i) => {
        const w = (i + 2).toString();
        t.equal(m.to, w, `to = ${m.to}, want ${w}`);
        t.equal(m.type, MessageType.MsgApp, `type = ${MessageType[m.type]}, want MsgApp`);
        t.equal(m.commit, li + 1, `commit = ${m.commit}, want ${li + 1}`);
    });

    t.end();
});

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
tap.test("Raft paper leaderAcknowledgeCommit", (t) => {
    const tests: {
        size: number;
        acceptors: Map<rid, boolean>;
        wack: boolean;
    }[] = [
            { size: 1, acceptors: new Map<rid, boolean>(), wack: true },
            { size: 3, acceptors: new Map<rid, boolean>(), wack: false },
            {
                size: 3,
                acceptors: new Map<rid, boolean>([["2", true]]),
                wack: true,
            },
            {
                size: 3,
                acceptors: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                ]),
                wack: true,
            },
            { size: 5, acceptors: new Map<rid, boolean>(), wack: false },
            {
                size: 5,
                acceptors: new Map<rid, boolean>([["2", true]]),
                wack: false,
            },
            {
                size: 5,
                acceptors: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                ]),
                wack: true,
            },
            {
                size: 5,
                acceptors: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                    ["4", true],
                ]),
                wack: true,
            },
            {
                size: 5,
                acceptors: new Map<rid, boolean>([
                    ["2", true],
                    ["3", true],
                    ["4", true],
                    ["5", true],
                ]),
                wack: true,
            },
        ];

    tests.forEach((tt, i) => {
        const s = newTestMemoryStorage(withPeers(...idsBySize(tt.size)));
        const r = newTestRaft("1", 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        commitNoopEntry(r, s);
        const li = r.raftLog.lastIndex();
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "some data" })],
            })
        );

        for (const m of r.readMessages()) {
            if (tt.acceptors.get(m.to)) {
                r.Step(acceptAndReply(m));
            }
        }

        const committed = r.raftLog.committed > li;
        t.equal(committed, tt.wack, `#${i}: ack commit = ${committed}, want ${tt.wack}`);
    });

    t.end();
});

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including entries
// created by previous leaders. Also, it applies the entry to its local state
// machine (in log order). Reference: section 5.3
tap.test("Raft paper leaderCommitPrecedingEntries", (t) => {
    const tests: Entry[][] = [
        [],
        [new Entry({ term: 2, index: 1 })],
        [new Entry({ term: 1, index: 1 }), new Entry({ term: 2, index: 2 })],
        [new Entry({ term: 1, index: 1 })],
    ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1", "2", "3"));
        storage.Append(tt);
        const r = newTestRaft("1", 10, 1, storage);
        r.loadState(new HardState({ term: 2 }));
        r.becomeCandidate();
        r.becomeLeader();
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry({ data: "some data" })],
            })
        );

        for (const m of r.readMessages()) {
            r.Step(acceptAndReply(m));
        }

        const li = tt.length;
        const wents = tt;
        wents.push(
            new Entry({ term: 3, index: li + 1, data: emptyRaftData }),
            new Entry({ term: 3, index: li + 2, data: "some data" })
        );

        const nextEnts = r.raftLog.nextEnts();
        t.strictSame(
            nextEnts,
            wents,
            `#${i}: ents = ${JSON.stringify(nextEnts)}, want ${JSON.stringify(wents)}`
        );
    });

    t.end();
});

// TestFollowerCommitEntry tests that once a follower learns that a log entry is
// committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
tap.test("Raft paper followerCommitEntry", (t) => {
    const tests: { ents: Entry[]; commit: number }[] = [
        {
            ents: [new Entry({ term: 1, index: 1, data: "some data" })],
            commit: 1,
        },
        {
            ents: [
                new Entry({ term: 1, index: 1, data: "some data" }),
                new Entry({ term: 1, index: 2, data: "some data 2" }),
            ],
            commit: 2,
        },
        {
            ents: [
                new Entry({ term: 1, index: 1, data: "some data2" }),
                new Entry({ term: 1, index: 2, data: "some data" }),
            ],
            commit: 2,
        },
        {
            ents: [
                new Entry({ term: 1, index: 1, data: "some data" }),
                new Entry({ term: 1, index: 2, data: "some data2" }),
            ],
            commit: 1,
        },
    ];

    tests.forEach((tt, i) => {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        r.becomeFollower(1, "2");
        r.Step(
            new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgApp,
                term: 1,
                entries: tt.ents,
                commit: tt.commit,
            })
        );

        const committed = r.raftLog.committed;
        t.equal(committed, tt.commit, `#${i}: committed = ${committed}, want ${tt.commit}`);
        const wents = tt.ents.slice(0, tt.commit);
        const nextEnts = r.raftLog.nextEnts();
        t.strictSame(
            nextEnts,
            wents,
            `#${i}: nextEnts = ${JSON.stringify(nextEnts)}, want ${JSON.stringify(wents)}`
        );
    });

    t.end();
});

// TestFollowerCheckMsgApp tests that if the follower does not find an entry in
// its log with the same index and term as the one in AppendEntries RPC, then it
// refuses the new entries. Otherwise it replies that it accepts the append
// entries. Reference: section 5.3
tap.test("Raft paper followerCheckMsgApp", (t) => {
    const ents: Entry[] = [new Entry({ term: 1, index: 1 }), new Entry({ term: 2, index: 2 })];
    const tests: {
        term: number;
        index: number;
        windex: number;
        wreject: boolean;
        wrejectHint: number;
        wlogterm: number;
    }[] = [
            // match with committed entries
            {
                term: 0,
                index: 0,
                windex: 1,
                wreject: false,
                wrejectHint: 0,
                wlogterm: 0,
            },
            {
                term: ents[0].term,
                index: ents[0].index,
                windex: 1,
                wreject: false,
                wrejectHint: 0,
                wlogterm: 0,
            },
            // match with uncommitted entries
            {
                term: ents[1].term,
                index: ents[1].index,
                windex: 2,
                wreject: false,
                wrejectHint: 0,
                wlogterm: 0,
            },

            // unmatch with existing entry
            {
                term: ents[0].term,
                index: ents[1].index,
                windex: ents[1].index,
                wreject: true,
                wrejectHint: 1,
                wlogterm: 1,
            },
            // nonexisting entry
            {
                term: ents[1].term,
                index: ents[1].index + 1,
                windex: ents[1].index + 1,
                wreject: true,
                wrejectHint: 2,
                wlogterm: 2,
            },
        ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1", "2", "3"));
        storage.Append(ents);
        const r = newTestRaft("1", 10, 1, storage);
        r.loadState(new HardState({ commit: 1 }));
        r.becomeFollower(2, "2");
        r.Step(
            new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgApp,
                term: 2,
                logTerm: tt.term,
                index: tt.index,
            })
        );

        const msgs = r.readMessages();
        const wantMsgs: Message[] = [
            new Message({
                from: "1",
                to: "2",
                type: MessageType.MsgAppResp,
                term: 2,
                index: tt.windex,
                reject: tt.wreject,
                rejectHint: tt.wrejectHint,
                logTerm: tt.wlogterm,
            }),
        ];

        t.strictSame(
            msgs,
            wantMsgs,
            `#${i}: msgs = ${JSON.stringify(msgs)}, want ${JSON.stringify(wantMsgs)}`
        );
    });

    t.end();
});

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid, the
// follower will delete the existing conflict entry and all that follow it, and
// append any new entries not already in the log. Also, it writes the new entry
// into stable storage. Reference: section 5.3
tap.test("Raft paper followerAppendEntries", (t) => {
    const tests: {
        index: number;
        term: number;
        ents: Entry[];
        wents: Entry[];
        wunstable: Entry[];
    }[] = [
            {
                index: 2,
                term: 2,
                ents: [new Entry({ term: 3, index: 3 })],
                wents: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 2, index: 2 }),
                    new Entry({ term: 3, index: 3 }),
                ],
                wunstable: [new Entry({ term: 3, index: 3 })],
            },
            {
                index: 1,
                term: 1,
                ents: [new Entry({ term: 3, index: 2 }), new Entry({ term: 4, index: 3 })],
                wents: [
                    new Entry({ term: 1, index: 1 }),
                    new Entry({ term: 3, index: 2 }),
                    new Entry({ term: 4, index: 3 }),
                ],
                wunstable: [new Entry({ term: 3, index: 2 }), new Entry({ term: 4, index: 3 })],
            },
            {
                index: 0,
                term: 0,
                ents: [new Entry({ term: 1, index: 1 })],
                wents: [new Entry({ term: 1, index: 1 }), new Entry({ term: 2, index: 2 })],
                wunstable: [],
            },
            {
                index: 0,
                term: 0,
                ents: [new Entry({ term: 3, index: 1 })],
                wents: [new Entry({ term: 3, index: 1 })],
                wunstable: [new Entry({ term: 3, index: 1 })],
            },
        ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1", "2", "3"));
        storage.Append([new Entry({ term: 1, index: 1 }), new Entry({ term: 2, index: 2 })]);
        const r = newTestRaft("1", 10, 1, storage);
        r.becomeFollower(2, "2");

        r.Step(
            new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgApp,
                term: 2,
                logTerm: tt.term,
                index: tt.index,
                entries: tt.ents,
            })
        );

        const allEntries = r.raftLog.allEntries();
        t.strictSame(
            allEntries,
            tt.wents,
            `#${i}: ents = ${JSON.stringify(allEntries)}, want ${JSON.stringify(tt.wents)}`
        );

        const unstableEntries = r.raftLog.unstableEntries();
        t.strictSame(
            unstableEntries,
            tt.wunstable,
            `#${i}: unstableEnts = ${JSON.stringify(unstableEntries)}, want ${JSON.stringify(
                tt.wunstable
            )}`
        );
    });

    t.end();
});

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own. Reference: section 5.3, figure 7
tap.test("Raft paper leaderSyncFollowerLog", (t) => {
    const ents: Entry[] = [
        new Entry({ term: 0, index: 0 }),
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 1, index: 2 }),
        new Entry({ term: 1, index: 3 }),
        new Entry({ term: 4, index: 4 }),
        new Entry({ term: 4, index: 5 }),
        new Entry({ term: 5, index: 6 }),
        new Entry({ term: 5, index: 7 }),
        new Entry({ term: 6, index: 8 }),
        new Entry({ term: 6, index: 9 }),
        new Entry({ term: 6, index: 10 }),
    ];
    const term = 8;
    const tests: Entry[][] = [
        [
            new Entry({ term: 0, index: 0 }),
            new Entry({ term: 1, index: 1 }),
            new Entry({ term: 1, index: 2 }),
            new Entry({ term: 1, index: 3 }),
            new Entry({ term: 4, index: 4 }),
            new Entry({ term: 4, index: 5 }),
            new Entry({ term: 5, index: 6 }),
            new Entry({ term: 5, index: 7 }),
            new Entry({ term: 6, index: 8 }),
            new Entry({ term: 6, index: 9 }),
        ],
        [
            new Entry({ term: 0, index: 0 }),
            new Entry({ term: 1, index: 1 }),
            new Entry({ term: 1, index: 2 }),
            new Entry({ term: 1, index: 3 }),
            new Entry({ term: 4, index: 4 }),
        ],
        [
            new Entry({ term: 0, index: 0 }),
            new Entry({ term: 1, index: 1 }),
            new Entry({ term: 1, index: 2 }),
            new Entry({ term: 1, index: 3 }),
            new Entry({ term: 4, index: 4 }),
            new Entry({ term: 4, index: 5 }),
            new Entry({ term: 5, index: 6 }),
            new Entry({ term: 5, index: 7 }),
            new Entry({ term: 6, index: 8 }),
            new Entry({ term: 6, index: 9 }),
            new Entry({ term: 6, index: 10 }),
            new Entry({ term: 6, index: 11 }),
        ],
        [
            new Entry({ term: 0, index: 0 }),
            new Entry({ term: 1, index: 1 }),
            new Entry({ term: 1, index: 2 }),
            new Entry({ term: 1, index: 3 }),
            new Entry({ term: 4, index: 4 }),
            new Entry({ term: 4, index: 5 }),
            new Entry({ term: 5, index: 6 }),
            new Entry({ term: 5, index: 7 }),
            new Entry({ term: 6, index: 8 }),
            new Entry({ term: 6, index: 9 }),
            new Entry({ term: 6, index: 10 }),
            new Entry({ term: 7, index: 11 }),
            new Entry({ term: 7, index: 12 }),
        ],
        [
            new Entry({ term: 0, index: 0 }),
            new Entry({ term: 1, index: 1 }),
            new Entry({ term: 1, index: 2 }),
            new Entry({ term: 1, index: 3 }),
            new Entry({ term: 4, index: 4 }),
            new Entry({ term: 4, index: 5 }),
            new Entry({ term: 4, index: 6 }),
            new Entry({ term: 4, index: 7 }),
        ],
        [
            new Entry({ term: 0, index: 0 }),
            new Entry({ term: 1, index: 1 }),
            new Entry({ term: 1, index: 2 }),
            new Entry({ term: 1, index: 3 }),
            new Entry({ term: 2, index: 4 }),
            new Entry({ term: 2, index: 5 }),
            new Entry({ term: 2, index: 6 }),
            new Entry({ term: 3, index: 7 }),
            new Entry({ term: 3, index: 8 }),
            new Entry({ term: 3, index: 9 }),
            new Entry({ term: 3, index: 10 }),
            new Entry({ term: 3, index: 11 }),
        ],
    ];

    tests.forEach((tt, i) => {
        const leadStorage = newTestMemoryStorage(withPeers("1", "2", "3"));
        leadStorage.Append(ents);
        const lead = newTestRaft("1", 10, 1, leadStorage);
        lead.loadState(new HardState({ term: term, commit: lead.raftLog.lastIndex() }));

        const followerStorage = newTestMemoryStorage(withPeers("1", "2", "3"));
        followerStorage.Append(tt);
        const follower = newTestRaft("2", 10, 1, followerStorage);
        follower.loadState(new HardState({ term: term - 1 }));
        // It is necessary to have a three-node cluster. The second may have
        // more up-to-date log than the first one, so the first node needs the
        // vote from the third node to become the leader.
        const n = newNetwork(lead, follower, nopStepper);
        send(n, new Message({ from: "1", to: "1", type: MessageType.MsgHup }));
        // The election occurs in the term after the one we loaded with
        // lead.loadState above.
        send(
            n,
            new Message({
                from: "3",
                to: "1",
                type: MessageType.MsgVoteResp,
                term: term + 1,
            })
        );
        send(
            n,
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry()],
            })
        );

        const diff = diffu(ltoa(lead.raftLog), ltoa(follower.raftLog));
        t.strictSame(diff, "", `#${i}: log diff: ${diff}`);
    });

    t.end();
});

// TestVoteRequest tests that the vote request includes information about the
// candidate’s log and are sent to all of the other nodes. Reference: section
// 5.4.1
tap.test("Raft paper voteRequest", (t) => {
    const tests: { ents: Entry[]; wterm: number }[] = [
        { ents: [new Entry({ term: 1, index: 1 })], wterm: 2 },
        {
            ents: [new Entry({ term: 1, index: 1 }), new Entry({ term: 2, index: 2 })],
            wterm: 3,
        },
    ];

    tests.forEach((tt, j) => {
        const r = newTestRaft("1", 10, 1, newTestMemoryStorage(withPeers("1", "2", "3")));
        r.Step(
            new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgApp,
                term: tt.wterm - 1,
                logTerm: 0,
                index: 0,
                entries: tt.ents,
            })
        );
        r.readMessages();

        for (let i = 1; i < r.electionTimeout * 2; i++) {
            r.tickElection();
        }

        const msgs = r.readMessages();
        msgs.sort(msgSortFunction);

        t.equal(msgs.length, 2, `#${j}: msgs.length = ${msgs.length}, want 2`);

        msgs.forEach((m, i) => {
            t.equal(
                m.type,
                MessageType.MsgVote,
                `#${i}: msgType = ${MessageType[m.type]}, want MsgVote`
            );
            t.equal(m.to, (i + 2).toString(), `#${i}: to = ${m.to}, want 2`);
            t.equal(m.term, tt.wterm, `#${i}: term = ${m.term}, want ${tt.wterm}`);

            const windex = tt.ents[tt.ents.length - 1].index;
            const wlogterm = tt.ents[tt.ents.length - 1].term;

            t.equal(m.index, windex, `#${i}: index = ${m.index}, want ${windex}`);
            t.equal(m.logTerm, wlogterm, `#${i}: logterm = ${m.logTerm}, want ${wlogterm}`);
        });
    });

    t.end();
});

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate. Reference: section 5.4.1
tap.test("Raft paper voter", (t) => {
    const tests: {
        ents: Entry[];
        logterm: number;
        index: number;
        wreject: boolean;
    }[] = [
            // same logterm
            {
                ents: [new Entry({ term: 1, index: 1 })],
                logterm: 1,
                index: 1,
                wreject: false,
            },
            {
                ents: [new Entry({ term: 1, index: 1 })],
                logterm: 1,
                index: 2,
                wreject: false,
            },
            {
                ents: [new Entry({ term: 1, index: 1 }), new Entry({ term: 1, index: 2 })],
                logterm: 1,
                index: 1,
                wreject: true,
            },
            // candidate higher logterm
            {
                ents: [new Entry({ term: 1, index: 1 })],
                logterm: 2,
                index: 1,
                wreject: false,
            },
            {
                ents: [new Entry({ term: 1, index: 1 })],
                logterm: 2,
                index: 2,
                wreject: false,
            },
            {
                ents: [new Entry({ term: 1, index: 1 }), new Entry({ term: 1, index: 2 })],
                logterm: 2,
                index: 1,
                wreject: false,
            },
            // voter higher logterm
            {
                ents: [new Entry({ term: 2, index: 1 })],
                logterm: 1,
                index: 1,
                wreject: true,
            },
            {
                ents: [new Entry({ term: 2, index: 1 })],
                logterm: 1,
                index: 2,
                wreject: true,
            },
            {
                ents: [new Entry({ term: 2, index: 1 }), new Entry({ term: 1, index: 2 })],
                logterm: 1,
                index: 1,
                wreject: true,
            },
        ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1", "2"));
        storage.Append(tt.ents);
        const r = newTestRaft("1", 10, 1, storage);

        r.Step(
            new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgVote,
                term: 3,
                logTerm: tt.logterm,
                index: tt.index,
            })
        );

        const msgs = r.readMessages();
        t.equal(msgs.length, 1, `#${i}: msgs.length 0 ${msgs.length}, want 1`);
        const m = msgs[0];
        t.equal(
            m.type,
            MessageType.MsgVoteResp,
            `#${i}: msgType = ${MessageType[m.type]}, want MsgVoteResp`
        );
        t.equal(m.reject, tt.wreject, `#${i}: reject = ${m.reject}, want ${tt.wreject}`);
    });

    t.end();
});

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the
// leader’s current term are committed by counting replicas. Reference: section
// 5.4.2
tap.test("Raft paper leaderOnlyCommitsLogFromCurrentTerm", (t) => {
    const ents: Entry[] = [new Entry({ term: 1, index: 1 }), new Entry({ term: 2, index: 2 })];
    const tests: { index: number; wcommit: number }[] = [
        // do not commit log entries in previous terms
        { index: 1, wcommit: 0 },
        { index: 2, wcommit: 0 },
        // commit log in current erm
        { index: 3, wcommit: 3 },
    ];

    tests.forEach((tt, i) => {
        const storage = newTestMemoryStorage(withPeers("1", "2"));
        storage.Append(ents);
        const r = newTestRaft("1", 10, 1, storage);
        r.loadState(new HardState({ term: 2 }));
        // become leader at term 3
        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();
        // propose a entry to current term
        r.Step(
            new Message({
                from: "1",
                to: "1",
                type: MessageType.MsgProp,
                entries: [new Entry()],
            })
        );

        r.Step(
            new Message({
                from: "2",
                to: "1",
                type: MessageType.MsgAppResp,
                term: r.term,
                index: tt.index,
            })
        );

        t.equal(
            r.raftLog.committed,
            tt.wcommit,
            `#${i}: commit = ${r.raftLog.committed}, want ${tt.wcommit}`
        );
    });

    t.end();
});

/**
 * Sort for the following with the given priority: from, to, term, type
 */
function msgSortFunction(a: Message, b: Message): number {
    return a.from !== b.from
        ? ('' + a.from).localeCompare(b.from)
        : a.to !== b.to
            ? ('' + a.to).localeCompare(b.to)
            : a.term !== b.term
                ? a.term - b.term
                : a.type !== b.type
                    ? a.type - b.type
                    : 0;
}

// returns a message where all missing properties are set to their default values
function getMessage(m: Message): Message {
    return new Message(m);
}

function commitNoopEntry(r: RaftForTest, s: MemoryStorage) {
    if (r.state !== StateType.StateLeader) {
        throw new Error("It should only be used when it is leader");
    }
    r.bcastAppend();
    // simulate the response of MsgApp
    const msgs = r.readMessages();
    for (const m of msgs) {
        if (
            m.type !== MessageType.MsgApp ||
            m.entries.length !== 1 ||
            m.entries[0].data !== emptyRaftData
        ) {
            throw new Error("not a message to append noop entry");
        }
        r.Step(acceptAndReply(m));
    }
    // ignore further messages to refresh followers' commit index
    r.readMessages();
    s.Append(r.raftLog.unstableEntries());
    r.raftLog.appliedTo(r.raftLog.committed);
    r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm());
}

function acceptAndReply(m: Message): Message {
    if (m.type !== MessageType.MsgApp) {
        throw new Error("type should be MsgApp");
    }
    return new Message({
        from: m.to,
        to: m.from,
        term: m.term,
        type: MessageType.MsgAppResp,
        index: m.index + m.entries.length,
    });
}

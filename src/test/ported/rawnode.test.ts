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

import { newTestConfig, newTestMemoryStorage, newTestRawNode, withPeers } from "./raft-test-util";
import { emptyRaftData, RaftData } from "../../non-ported/raft-controller";
import { getLogger } from "../../ported/logger";
import { Ready, SoftState } from "../../ported/node";
import { JointConfig } from "../../ported/quorum/joint";
import { MajorityConfig } from "../../ported/quorum/majority";
import { noLimit, Raft, StateType } from "../../ported/raft";
import { errStepLocalMsg, RawNode } from "../../ported/rawnode";
import { ReadState } from "../../ported/readonly";
import { MemoryStorage, Storage } from "../../ported/storage";
import { Config } from "../../ported/tracker/tracker";
import { isLocalMsg, NullableError } from "../../ported/util";
import {
    ConfChange,
    ConfChangeSingle,
    ConfChangeV2,
    ConfState,
    ConfChangeType,
    Entry,
    HardState,
    MessageType,
    Snapshot,
    ConfChangeTransition,
    ConfChangeI,
    EntryType,
    Message,
    SnapshotMetadata,
} from "../../ported/raftpb/raft.pb";

// TestRawNodeStep ensures that RawNode.Step ignore local message.
tap.test("rawnode rawNodeStep()", (t) => {
    for (const msgTypeString in MessageType) {
        // filter everything that is not a msgtype string
        if (isNaN(Number(msgTypeString))) {
            continue;
        }
        const type: MessageType = Number(msgTypeString);

        const s = MemoryStorage.NewMemoryStorage();
        s.SetHardState(new HardState({ term: 1, commit: 1 }));
        s.Append([new Entry({ term: 1, index: 1 })]);
        const snapShot: Snapshot = new Snapshot({
            metadata: {
                confState: new ConfState({
                    voters: ["1"],
                }),
                index: 1,
                term: 1,
            },
        });
        const err = s.ApplySnapshot(snapShot);
        if (err !== null) {
            t.fail(err.message);
        }

        // Append an empty entry to make sure the non-local messages (like vote
        // requests) are ignored and don't trigger assertions.
        const rawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, s));
        const stepError = rawNode.step(
            new Message({
                type: type,
                // This test only ensures that local messages are ignored. The
                // rest can be left up to default values in go. Here, it is only
                // strictly necessary to set term:0, s.t. the raft object
                // ignores the message. But all other values can be set to their
                // default values, too. Omitted for brevity.
                term: 0,
            })
        );

        // LocalMsg should be ignored.
        if (isLocalMsg(type)) {
            if (stepError !== errStepLocalMsg) {
                t.error(`${msgTypeString}: step should ignore ${stepError!.message}`);
            }
        }
    }
    t.end();
});

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because
// there is no goroutine in RawNode.

// TestRawNodeProposeAndConfChange tests the configuration change mechanism.
// Each test case sends a configuration change which is either simple or joint,
// verifies that it applies and that the resulting ConfState matches
// expectations, and for joint configurations makes sure that they are exited
// successfully.
tap.test("rawnode rawNodeProposeAndConfChange()", (t) => {
    const testCases: {
        cc: ConfChangeI;
        exp: ConfState;
        exp2: ConfState | null;
    }[] = [
            // V1 config change.
            {
                cc: new ConfChange({
                    type: ConfChangeType.ConfChangeAddNode,
                    nodeId: "2",
                }),
                exp: new ConfState({ voters: ["1", "2"] }),
                exp2: null,
            },
            // Proposing the same as a V2 change works just the same, without
            // entering a joint config.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            type: ConfChangeType.ConfChangeAddNode,
                            nodeId: "2",
                        }),
                    ],
                }),
                exp: new ConfState({ voters: ["1", "2"] }),
                exp2: null,
            },
            // Ditto if we add it as a learner instead.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                            nodeId: "2",
                        }),
                    ],
                }),
                exp: new ConfState({ voters: ["1"], learners: ["2"] }),
                exp2: null,
            },
            // We can ask explicitly for joint consensus if we want it.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                            nodeId: "2",
                        }),
                    ],
                    transition: ConfChangeTransition.ConfChangeTransitionJointExplicit,
                }),
                exp: new ConfState({
                    voters: ["1"],
                    votersOutgoing: ["1"],
                    learners: ["2"],
                }),
                exp2: new ConfState({ voters: ["1"], learners: ["2"] }),
            },
            // Ditto, but with implicit transition (the harness checks this).
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                            nodeId: "2",
                        }),
                    ],
                    transition: ConfChangeTransition.ConfChangeTransitionJointImplicit,
                }),
                exp: new ConfState({
                    voters: ["1"],
                    votersOutgoing: ["1"],
                    learners: ["2"],
                    autoLeave: true,
                }),
                exp2: new ConfState({ voters: ["1"], learners: ["2"] }),
            },
            // Add a new node and demote n1. This exercises the interesting case
            // in which we really need joint config changes and also need
            // learnersNext.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            nodeId: "2",
                            type: ConfChangeType.ConfChangeAddNode,
                        }),
                        new ConfChangeSingle({
                            nodeId: "1",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                        new ConfChangeSingle({
                            nodeId: "3",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                    ],
                }),
                exp: new ConfState({
                    voters: ["2"],
                    votersOutgoing: ["1"],
                    learners: ["3"],
                    learnersNext: ["1"],
                    autoLeave: true,
                }),
                exp2: new ConfState({ voters: ["2"], learners: ["1", "3"] }),
            },
            // Ditto explicit.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            nodeId: "2",
                            type: ConfChangeType.ConfChangeAddNode,
                        }),
                        new ConfChangeSingle({
                            nodeId: "1",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                        new ConfChangeSingle({
                            nodeId: "3",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                    ],
                    transition: ConfChangeTransition.ConfChangeTransitionJointExplicit,
                }),
                exp: new ConfState({
                    voters: ["2"],
                    votersOutgoing: ["1"],
                    learners: ["3"],
                    learnersNext: ["1"],
                }),
                exp2: new ConfState({ voters: ["2"], learners: ["1", "3"] }),
            },
            // Ditto implicit.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            nodeId: "2",
                            type: ConfChangeType.ConfChangeAddNode,
                        }),
                        new ConfChangeSingle({
                            nodeId: "1",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                        new ConfChangeSingle({
                            nodeId: "3",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                    ],
                    transition: ConfChangeTransition.ConfChangeTransitionJointImplicit,
                }),
                exp: new ConfState({
                    voters: ["2"],
                    votersOutgoing: ["1"],
                    learners: ["3"],
                    learnersNext: ["1"],
                    autoLeave: true,
                }),
                exp2: new ConfState({ voters: ["2"], learners: ["1", "3"] }),
            },
            // V1 config change.
            {
                cc: new ConfChange({
                    type: ConfChangeType.ConfChangeAddNode,
                    nodeId: "2",
                }),
                exp: getConfState(new ConfState({ voters: ["1", "2"] })),
                exp2: null,
            },
            // Proposing the same as a V2 change works just the same, without
            // entering a joint config.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            type: ConfChangeType.ConfChangeAddNode,
                            nodeId: "2",
                        }),
                    ],
                }),
                exp: getConfState(new ConfState({ voters: ["1", "2"] })),
                exp2: null,
            },
            // Ditto if we add it as a learner instead.
            {
                cc: new ConfChangeV2({
                    changes: [{ type: ConfChangeType.ConfChangeAddLearnerNode, nodeId: "2" }],
                }),
                exp: getConfState(new ConfState({ voters: ["1"], learners: ["2"] })),
                exp2: null,
            },
            // We can ask explicitly for joint consensus if we want it.
            {
                cc: new ConfChangeV2({
                    transition: ConfChangeTransition.ConfChangeTransitionJointExplicit,
                    changes: [{ type: ConfChangeType.ConfChangeAddLearnerNode, nodeId: "2" }],
                }),
                exp: getConfState(
                    new ConfState({
                        voters: ["1"],
                        votersOutgoing: ["1"],
                        learners: ["2"],
                    })
                ),
                exp2: getConfState(new ConfState({ voters: ["1"], learners: ["2"] })),
            },
            // Ditto, but with implicit transition (the harness checks this).
            {
                cc: new ConfChangeV2({
                    transition: ConfChangeTransition.ConfChangeTransitionJointImplicit,
                    changes: [{ type: ConfChangeType.ConfChangeAddLearnerNode, nodeId: "2" }],
                }),
                exp: getConfState(
                    new ConfState({
                        voters: ["1"],
                        votersOutgoing: ["1"],
                        learners: ["2"],
                        autoLeave: true,
                    })
                ),
                exp2: getConfState(new ConfState({ voters: ["1"], learners: ["2"] })),
            },
            // Add a new node and demote n1. This exercises the interesting case
            // in which we really need joint config changes and also need
            // LearnersNext.
            {
                cc: new ConfChangeV2({
                    changes: [
                        new ConfChangeSingle({
                            nodeId: "2",
                            type: ConfChangeType.ConfChangeAddNode,
                        }),
                        {
                            nodeId: "1",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        },
                        new ConfChangeSingle({
                            nodeId: "3",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                    ],
                }),
                exp: new ConfState({
                    voters: ["2"],
                    votersOutgoing: ["1"],
                    learners: ["3"],
                    learnersNext: ["1"],
                    autoLeave: true,
                }),
                exp2: getConfState(new ConfState({ voters: ["2"], learners: ["1", "3"] })),
            },
            // Ditto explicit.
            {
                cc: new ConfChangeV2({
                    transition: ConfChangeTransition.ConfChangeTransitionJointExplicit,
                    changes: [
                        new ConfChangeSingle({
                            nodeId: "2",
                            type: ConfChangeType.ConfChangeAddNode,
                        }),
                        {
                            nodeId: "1",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        },
                        new ConfChangeSingle({
                            nodeId: "3",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                    ],
                }),
                exp: new ConfState({
                    voters: ["2"],
                    votersOutgoing: ["1"],
                    learners: ["3"],
                    learnersNext: ["1"],
                    autoLeave: false,
                }),
                exp2: getConfState(new ConfState({ voters: ["2"], learners: ["1", "3"] })),
            },
            // Ditto implicit.
            {
                cc: new ConfChangeV2({
                    transition: ConfChangeTransition.ConfChangeTransitionJointImplicit,
                    changes: [
                        new ConfChangeSingle({
                            nodeId: "2",
                            type: ConfChangeType.ConfChangeAddNode,
                        }),
                        {
                            nodeId: "1",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        },
                        new ConfChangeSingle({
                            nodeId: "3",
                            type: ConfChangeType.ConfChangeAddLearnerNode,
                        }),
                    ],
                }),
                exp: new ConfState({
                    voters: ["2"],
                    votersOutgoing: ["1"],
                    learners: ["3"],
                    learnersNext: ["1"],
                    autoLeave: true,
                }),
                exp2: getConfState(new ConfState({ voters: ["2"], learners: ["1", "3"] })),
            },
        ];

    testCases.forEach((tc, i) => {
        const s = newTestMemoryStorage(withPeers("1"));
        const rawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, s));

        rawNode.campaign();
        let proposed = false;
        let ccData: RaftData = emptyRaftData;

        // Propose the ConfChange, wait until it applies, save the resulting
        // ConfState.
        let cs: ConfState | null = null;
        while (!cs) {
            const rd = rawNode.ready();
            s.Append(rd.entries);
            for (const entry of rd.committedEntries ? rd.committedEntries : []) {
                let confChange: ConfChangeI | null = null;
                if (entry.type === EntryType.EntryConfChange) {
                    confChange = new ConfChange(entry.data);
                } else if (entry.type === EntryType.EntryConfChangeV2) {
                    confChange = new ConfChangeV2(entry.data);
                }
                if (confChange !== null) {
                    cs = rawNode.applyConfChange(confChange);
                }
            }
            rawNode.advance(rd);

            // Once we are the leader, propose a command and a ConfChange.
            if (!proposed && rd.softState?.lead === rawNode.raft.id) {
                const err = rawNode.propose("somedata");
                if (err !== null) {
                    t.fail(`${i}: Error proposing some data: ${err.message}`);
                }

                const [ch, chOK] = tc.cc.asV1();
                if (chOK) {
                    ccData = ch;
                    rawNode.proposeConfChange(ch);
                } else {
                    const ccv2 = tc.cc.asV2();
                    ccData = ccv2;
                    rawNode.proposeConfChange(ccv2);
                }
                proposed = true;
            }
        }

        // Check that the last index is exactly the conf change we put in, down
        // to the bits. Note that this comes from the Storage, which will not
        // reflect any unstable entries that we'll only be presented with in the
        // next Ready.
        const [lastIndex, lastIndexError] = s.LastIndex();
        t.equal(lastIndexError, null, `#${i}: error: ${lastIndexError?.message}`);

        const [entries, entriesErr] = s.Entries(lastIndex - 1, lastIndex + 1, noLimit);
        t.equal(entriesErr, null, `#${i}: error: ${entriesErr?.message}`);

        t.equal(entries.length, 2, `#${i}: entries.length = ${entries.length}, want 2`);

        t.strictSame(
            entries[0].data,
            "somedata",
            `${i}: entries: ${entries[0].data}, should be "somedata"`
        );

        let typ = EntryType.EntryConfChange;
        const [_, asV1ok] = tc.cc.asV1();
        if (!asV1ok) {
            typ = EntryType.EntryConfChangeV2;
        }
        t.equal(entries["1"].type, typ, `${i}: type = ${entries["1"].type}, want ${typ}`);

        t.strictSame(entries["1"].data, ccData, `data = ${entries["1"].data}, want ${ccData}`);

        const exp = tc.exp;
        t.strictSame(cs, exp, `${i}: Found ${JSON.stringify(cs)}, want: ${JSON.stringify(exp)}`);

        let maybePlusOne = 0;
        const [autoLeave, ok] = tc.cc.asV2().enterJoint();
        if (ok && autoLeave) {
            // If this is an auto-leaving joint conf change, it will have
            // appended the entry that auto-leaves, so add one to the last index
            // that forms the basis of our expectations on pendingConfIndex.
            // (Recall that lastIndex was taken from stable storage, but this
            // auto-leaving entry isn't on stable storage yet).
            maybePlusOne = 1;
        }
        const expIndex = lastIndex + maybePlusOne;
        const actIndex = rawNode.raft.pendingConfIndex;
        t.equal(
            actIndex,
            expIndex,
            `${i}: pendingConfIndex: expected ${expIndex}, got ${actIndex}`
        );

        // Move the RawNode along. If the ConfChange was simple, nothing else
        // should happen. Otherwise, we're in a joint state, which is either
        // left automatically or not. If not, we add the proposal that leaves it
        // manually.
        let ready = rawNode.ready();
        let context: RaftData = emptyRaftData;
        if (!tc.exp.autoLeave) {
            t.ok(
                ready.entries.length === 0,
                `${i}: expected no more entries, but got: ${ready.entries?.length
                } further entries. ${JSON.stringify(ready.entries)}`
            );

            if (tc.exp2 === null) {
                return;
            }
            context = "manual";
            getLogger().infof("leaving joint state manually");
            const errConfChange = rawNode.proposeConfChange(new ConfChangeV2({ context: context }));
            t.equal(errConfChange, null, `${i}: error: ${errConfChange?.message}`);
            ready = rawNode.ready();
        }

        // Check that the right ConfChange comes out.
        t.equal(
            ready.entries.length,
            1,
            `${i}: expected exactly one more entry, got: ${ready.entries.length}; ${JSON.stringify(
                ready
            )}`
        );
        t.equal(
            ready.entries[0]?.type,
            EntryType.EntryConfChangeV2,
            `${i}: expected exactly one more entry, got: ${ready.entries.length}; ${JSON.stringify(
                ready
            )}`
        );

        // if this condition is false, the 2 assertions above already complain
        // about that, but let the other test cases run regardless
        if (ready.entries.length >= 1) {
            const confChange: ConfChangeV2 = new ConfChangeV2(ready.entries[0].data ?? {});

            t.strictSame(
                confChange,
                new ConfChangeV2({ context: context }),
                `${i}: Confchange expected: ${JSON.stringify(
                    new ConfChangeV2({ context: context })
                )}, got: ${JSON.stringify(confChange)}`
            );
            // Lie and pretend the ConfChange applied. It won't do so because
            // now we require the joint quorum and we're only running one node.
            const confState = rawNode.applyConfChange(confChange);
            t.strictSame(confState, tc.exp2, `exp: ${tc.exp2}, got: ${confState}`);
        }
    });

    t.end();
});

// TestRawNodeJointAutoLeave tests the configuration change auto leave even
// leader lost leadership.
tap.test("rawnode rawNodeJointAutoLeave()", (t) => {
    const testConfchange = new ConfChangeV2({
        transition: ConfChangeTransition.ConfChangeTransitionJointImplicit,
        changes: [
            new ConfChangeSingle({
                type: ConfChangeType.ConfChangeAddLearnerNode,
                nodeId: "2",
            }),
        ],
    });
    const expConfState: ConfState = new ConfState({
        voters: ["1"],
        votersOutgoing: ["1"],
        learners: ["2"],
        learnersNext: [],
        autoLeave: true,
    });
    const exp2ConfState: ConfState = new ConfState({
        voters: ["1"],
        votersOutgoing: [],
        learners: ["2"],
        learnersNext: [],
        autoLeave: false,
    });

    const storage = newTestMemoryStorage(withPeers("1"));
    const rawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, storage));

    rawNode.campaign();
    let proposed = false;

    let lastIndex = 0;
    let ccData: RaftData = emptyRaftData;

    // Propose the ConfChange, wait until it applies, save the resulting
    // ConfState.
    let confState: ConfState | null = null;
    while (confState === null) {
        const rd = rawNode.ready();
        storage.Append(rd.entries);
        for (const ent of rd.committedEntries ? rd.committedEntries : []) {
            let confChange: ConfChangeI | null = null;
            if (ent.type === EntryType.EntryConfChangeV2) {
                confChange = new ConfChangeV2(ent.data);
            }
            if (confChange !== null) {
                // Force it step down.
                rawNode.step(
                    new Message({
                        type: MessageType.MsgHeartbeatResp,
                        from: "1",
                        term: rawNode.raft.term + 1,
                    })
                );
                confState = rawNode.applyConfChange(confChange);
            }
        }
        rawNode.advance(rd);
        // Once we are the leader, propose a command and a ConfChange.
        if (!proposed && rd.softState && rd.softState.lead === rawNode.raft.id) {
            const proposeError = rawNode.propose("somedata");
            t.equal(proposeError, null, `Error proposing somedata`);
            ccData = testConfchange;
            rawNode.proposeConfChange(testConfchange);
            proposed = true;
        }
    }

    // Check that the last index is exactly the conf change we put in, down to
    // the bits. Note that this comes from the Storage, which will not reflect
    // any unstable entries that we'll only be presented with in the next Ready.
    let lastIndexErr: NullableError;
    [lastIndex, lastIndexErr] = storage.LastIndex();
    t.equal(lastIndexErr, null, "error in storage.lastIndex: " + lastIndexErr?.message);

    const [entries, err] = storage.Entries(lastIndex - 1, lastIndex + 1, noLimit);
    t.equal(err, null, "error in storage.Entries: " + err?.message);
    t.equal(entries.length, 2, "entries.length = " + entries.length + ", want 2");
    t.strictSame(
        entries[0].data,
        "somedata",
        "entries[0].data = " + entries[0].data + ", want somedata"
    );
    t.equal(
        entries["1"].type,
        EntryType.EntryConfChangeV2,
        "type = " + entries["1"].type + ", want " + EntryType.EntryConfChangeV2
    );
    t.strictSame(entries["1"].data, ccData, "data = " + entries["1"].data + ", want " + ccData);
    t.strictSame(
        confState,
        expConfState,
        "exp:\n" + JSON.stringify(expConfState) + "\nact:\n" + JSON.stringify(confState)
    );
    t.equal(
        rawNode.raft.pendingConfIndex,
        0,
        "pendingConfIndex: expected " + 0 + ", got " + rawNode.raft.pendingConfIndex
    );

    // Move the RawNode along. It should not leave joint because it's follower.
    let ready = rawNode.readyWithoutAccept();
    // Check that the right ConfChange comes out.
    t.equal(ready.entries.length, 0, "expected zero entries, got " + ready.entries.length);

    // Make it leader again. It should leave joint automatically after moving
    // apply index.
    rawNode.campaign();
    ready = rawNode.ready();
    storage.Append(ready.entries);
    rawNode.advance(ready);
    ready = rawNode.ready();
    storage.Append(ready.entries);

    // Check that the right ConfChange comes out.
    t.equal(
        ready.entries.length,
        1,
        "expected exactly one more entry, got " + JSON.stringify(ready)
    );
    t.equal(
        ready.entries[0].type,
        EntryType.EntryConfChangeV2,
        "expected exactly one more entry of type EntryConfChangeV2, got " + JSON.stringify(ready)
    );
    const confChangeV2: ConfChangeV2 = new ConfChangeV2(ready.entries[0].data ?? {});

    const expectedCCV2: ConfChangeV2 = new ConfChangeV2();
    t.strictSame(
        confChangeV2,
        expectedCCV2,
        "expected zero ConfChangeV2, got " + JSON.stringify(confChangeV2)
    );
    // Lie and pretend the ConfChange applied. It won't do so because now we
    // require the joint quorum and we're only running one node.
    confState = rawNode.applyConfChange(confChangeV2);
    t.strictSame(
        confState,
        exp2ConfState,
        "exp:\n" + JSON.stringify(exp2ConfState) + "\nact:\n" + JSON.stringify(confState)
    );

    t.end();
});

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same
// node should not affect the later propose to add new node.
tap.test("rawnode rawNodeProposeAddDuplicateNode()", (t) => {
    const storage = newTestMemoryStorage(withPeers("1"));
    const rawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, storage));

    let ready = rawNode.ready();
    storage.Append(ready.entries);
    rawNode.advance(ready);

    rawNode.campaign();
    while (true) {
        ready = rawNode.ready();
        storage.Append(ready.entries);
        if (ready.softState !== null && ready.softState.lead === rawNode.raft.id) {
            rawNode.advance(ready);
            break;
        }
        rawNode.advance(ready);
    }

    function proposeConfChangeAndApply(confChange: ConfChange) {
        rawNode.proposeConfChange(confChange);
        ready = rawNode.ready();
        storage.Append(ready.entries);
        for (const entry of ready.committedEntries ? ready.committedEntries : []) {
            if (entry.type === EntryType.EntryConfChange) {
                const loopConfChange = new ConfChange(entry.data);
                rawNode.applyConfChange(loopConfChange);
            }
        }
        rawNode.advance(ready);
    }

    const confChange1 = new ConfChange({
        type: ConfChangeType.ConfChangeAddNode,
        nodeId: "1",
    });
    const ccData1 = confChange1;
    proposeConfChangeAndApply(confChange1);

    // try to add the same node again
    proposeConfChangeAndApply(confChange1);

    // the new node join should be ok
    const confChange2 = new ConfChange({
        type: ConfChangeType.ConfChangeAddNode,
        nodeId: "2",
    });
    const ccData2 = confChange2;
    proposeConfChangeAndApply(confChange2);

    const [lastIndex, err] = storage.LastIndex();
    t.equal(err, null, "error: " + err?.message);

    // the last three entries should be ConfChange confChange1, confChange1,
    // confChange2
    const [entries, entriesErr] = storage.Entries(lastIndex - 2, lastIndex + 1, noLimit);
    t.equal(entriesErr, null, "error: " + err?.message);
    t.equal(entries.length, 3, "entries.length = " + entries.length + ", want " + 3);

    t.strictSame(entries[0].data, ccData1, `found: ${entries[0].data}, expected: ${ccData1}`);
    t.strictSame(entries["2"].data, ccData2, `found: ${entries["2"].data}, expected: ${ccData2}`);

    t.end();
});

// TestRawNodeReadIndex ensures that Rawnode.ReadIndex sends the MsgReadIndex
// message to the underlying raft. It also ensures that ReadState can be read
// out.
tap.test("rawnode rawNodeReadIndex()", (t) => {
    const msgs: Message[] = [];
    function appendStep(r: Raft, m: Message): NullableError {
        msgs.push(m);
        return null;
    }
    const wrs: ReadState[] = [new ReadState({ index: 1, requestCtx: "somedata" })];

    const storage = newTestMemoryStorage(withPeers("1"));
    const config = newTestConfig("1", 10, 1, storage);
    const rawNode = RawNode.NewRawNode(config);

    rawNode.raft.readStates = wrs;
    // ensure the ReadStates can be read out
    const hasReady = rawNode.hasReady();
    t.ok(hasReady, `hasReady() returns ${hasReady}, want ${true}`);

    let ready = rawNode.ready();
    t.strictSame(
        ready.readStates,
        wrs,
        "readStates = " + JSON.stringify(ready.readStates) + ", want " + JSON.stringify(wrs)
    );

    storage.Append(ready.entries);
    rawNode.advance(ready);
    // ensure raft.readStates is reset after advance
    t.ok(
        rawNode.raft.readStates.length === 0,
        `readStates = ${rawNode.raft.readStates}, want null/empty list`
    );

    const wRequestCtx = "somedata2";
    rawNode.campaign();

    while (true) {
        ready = rawNode.ready();
        storage.Append(ready.entries);

        if (ready.softState !== null && ready.softState.lead === rawNode.raft.id) {
            rawNode.advance(ready);
            rawNode.raft.step = appendStep;
            rawNode.readIndex(wRequestCtx);
            break;
        }
        rawNode.advance(ready);
    }
    // ensure that MsgReadIndex message is sent to the underlying raft
    t.equal(msgs.length, 1, `msgs.length = ${msgs.length}, want ${1}`);
    t.equal(
        msgs[0].type,
        MessageType.MsgReadIndex,
        `msg type = ${msgs[0].type}, want ${MessageType.MsgReadIndex}`
    );
    t.equal(
        msgs[0].entries[0].data,
        wRequestCtx,
        `data = ${msgs[0].entries[0].data}, want ` + wRequestCtx
    );

    t.end();
});

// TestBlockProposal from node_test.go has no equivalent in rawNode because
// there is no leader check in RawNode.

// TestNodeTick from node_test.go has no equivalent in rawNode because it
// reaches into the raft object which is not exposed.

// TestNodeStop from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeStart ensures that a node can be started correctly. Note that
// RawNode requires the application to bootstrap the state, i.e. it does not
// accept peers and will not create faux configuration change entries.
tap.test("rawnode rawNodeStart()", (t) => {
    const wantEntries: Entry[] = [
        new Entry({ term: 1, index: 2 }), // empty entry
        new Entry({ term: 1, index: 3, data: "foo" }), // empty entry
    ];
    const ss = new SoftState({
        lead: "1",
        raftState: StateType.StateLeader,
    });
    const hs = new HardState({ term: 1, vote: "1", commit: 3 });
    const want: Ready = new Ready({
        softState: ss,
        hardState: hs,
        entries: wantEntries,
        committedEntries: wantEntries,
        mustSync: true,
    });

    const storage = MemoryStorage.NewMemoryStorage();
    storage.ents[0].index = 1;

    // TO DO(tbg): this is a first prototype of what bootstrapping could look
    // like (without the annoying faux ConfChanges). We want to persist a
    // ConfState at some index and make sure that this index can't be reached
    // from log position 1, so that followers are forced to pick up the
    // ConfState in order to move away from log position 1 (unless they got
    // bootstrapped in the same way already). Failing to do so would mean that
    // followers diverge from the bootstrapped nodes and don't learn about the
    // initial config.
    //
    // NB: this is exactly what CockroachDB does. The Raft log really begins at
    // index 10, so empty followers (at index 1) always need a snapshot first.

    // extends interface storage
    type AppenderStorage = Storage & {
        ApplySnapshot(snap: Snapshot): NullableError;
    };

    function bootstrap(appenderStorage: AppenderStorage, confState: ConfState) {
        t.ok(confState.voters.length !== 0, "no voters specified");

        const [firstIndex, firstIndexErr] = appenderStorage.FirstIndex();
        t.equal(firstIndexErr, null, "error: " + firstIndexErr?.message);
        t.ok(firstIndex >= 2, "FirstIndex >= 2 is prerequisite for bootstrap");
        let [_, entriesErr] = appenderStorage.Entries(firstIndex, firstIndex, noLimit);
        // TO DO(tbg): match exact error
        t.ok(entriesErr !== null, "Should not have been able to load first index");

        const [lastIndex, lastIndexErr] = appenderStorage.LastIndex();
        t.equal(lastIndexErr, null, "error: " + firstIndexErr?.message);
        [_, entriesErr] = appenderStorage.Entries(lastIndex, lastIndex, noLimit);
        t.ok(entriesErr !== null, "should not have been able to load last index");

        const [hardState, initialConfState, initStateErr] = appenderStorage.InitialState();
        t.equal(initStateErr, null, "error: " + initStateErr?.message);
        t.ok(hardState.isEmptyHardState(), "HardState not empty");
        t.equal(initialConfState.voters.length, 0, "ConfState not empty");

        const meta: SnapshotMetadata = {
            index: 1,
            term: 0,
            confState: confState,
        };
        const snap: Snapshot = new Snapshot({ metadata: meta });
        return appenderStorage.ApplySnapshot(snap);
    }

    const err = bootstrap(storage, new ConfState({ voters: ["1"] }));
    t.equal(err, null, "error: " + err?.message);

    const rawNode: RawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, storage));
    t.notOk(rawNode.hasReady(), "hasReady: " + rawNode.hasReady() + ", want false.");

    rawNode.campaign();
    rawNode.propose("foo");
    t.ok(rawNode.hasReady(), "expected a ready. -> Expected true, got: " + rawNode.hasReady());

    const ready = rawNode.ready();
    storage.Append(ready.entries);
    rawNode.advance(ready);

    // ready.softState = null; want.softState = null; Change due to readonly
    // modifier

    const rd = new Ready({
        hardState: ready.hardState,
        readStates: ready.readStates,
        entries: ready.entries,
        snapshot: ready.snapshot,
        committedEntries: ready.committedEntries,
        messages: ready.messages,
        mustSync: ready.mustSync,
    });

    const w = new Ready({
        hardState: want.hardState,
        readStates: want.readStates,
        entries: want.entries,
        snapshot: want.snapshot,
        committedEntries: want.committedEntries,
        messages: want.messages,
        mustSync: want.mustSync,
    });

    t.strictSame(rd, w, "unexpected Ready:\n" + JSON.stringify(rd) + "\nvs\n" + JSON.stringify(w));
    t.notOk(rawNode.hasReady(), "hasReady: " + rawNode.hasReady() + ", want false.");

    t.end();
});

tap.test("rawnode rawNodeRestart()", (t) => {
    const entries: Entry[] = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 1, index: 2, data: "foo" }),
    ];
    const state: HardState = new HardState({ term: 1, commit: 1 });

    const want = new Ready({
        committedEntries: entries.slice(0, state.commit),
    });

    const storage = newTestMemoryStorage(withPeers("1"));
    storage.SetHardState(state);
    storage.Append(entries);
    const rawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, storage));

    const ready = rawNode.ready();
    t.strictSame(ready, want, "got: " + JSON.stringify(ready) + ", want: " + JSON.stringify(want));
    rawNode.advance(ready);
    t.notOk(rawNode.hasReady(), "hasReady want false, got: " + rawNode.hasReady());

    t.end();
});

tap.test("rawnode rawNodeRestartFromSnapshot()", (t) => {
    const snap: Snapshot = new Snapshot({
        metadata: {
            confState: new ConfState({ voters: ["1", "2"] }),
            index: 2,
            term: 1,
        },
    });
    const entries: Entry[] = [new Entry({ term: 1, index: 3, data: "foo" })];
    const state = new HardState({ term: 1, commit: 3 });

    const want = new Ready({
        committedEntries: entries,
        mustSync: false,
    });

    const storage = MemoryStorage.NewMemoryStorage();
    storage.SetHardState(state);
    storage.ApplySnapshot(snap);
    storage.Append(entries);

    const rawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, storage));

    const ready = rawNode.ready();
    t.strictSame(
        ready,
        want,
        "found ready: " + JSON.stringify(ready) + ", want: " + JSON.stringify(want)
    );
    rawNode.advance(ready);
    t.notOk(rawNode.hasReady(), "hasReady want false, got: " + rawNode.hasReady());

    t.end();
});

// TestNodeAdvance from node_test.go has no equivalent in rawNode because there
// is no dependency check between Ready() and Advance()

tap.test("rawnode rawNodeStatus()", (t) => {
    const storage = newTestMemoryStorage(withPeers("1"));
    const rawNode = RawNode.NewRawNode(newTestConfig("1", 10, 1, storage));

    let status = rawNode.status();
    t.equal(
        status.progress.size,
        0,
        "expected no Progress because not leader: " + JSON.stringify(status.progress)
    );

    const err = rawNode.campaign();
    t.equal(err, null, "error: " + err?.message);

    status = rawNode.status();
    t.equal(
        status.basicStatus.softState.lead,
        "1",
        "wanted lead (=1), but got " + status.basicStatus.softState.lead
    );
    t.equal(
        status.basicStatus.softState.raftState,
        StateType.StateLeader,
        "wanted " + StateType.StateLeader + ", but got " + status.basicStatus.softState.raftState
    );

    const exp = rawNode.raft.prs.progress.get("1");
    const act = status.progress.get("1");
    t.strictSame(act, exp, "want " + JSON.stringify(exp) + ", but got " + JSON.stringify(act));

    const jointMajConfig: MajorityConfig = new MajorityConfig();
    jointMajConfig.map.set("1", {});
    const jointConfig: JointConfig = new JointConfig();
    jointConfig.config[0] = jointMajConfig;
    const expCfg: Config = new Config({ voters: jointConfig });

    t.strictSame(
        status.config,
        expCfg,
        "want: " + JSON.stringify(expCfg) + ", got " + JSON.stringify(status.config)
    );

    t.end();
});

// TestRawNodeCommitPaginationAfterRestart is the RawNode version of
// TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
// Raft group would forget to apply entries:
//
// - node learns that index 11 is committed
// - nextEnts returns index 1..10 in CommittedEntries (but index 10 already
//   exceeds maxBytes), which isn't noticed internally by Raft
// - Commit index gets bumped to 10
// - the node persists the HardState, but crashes before applying the entries
// - upon restart, the storage returns the same entries, but `slice` takes a
//   different code path and removes the last entry.
// - Raft does not emit a HardState, but when the app calls Advance(), it bumps
//   its internal applied index cursor to 10 (when it should be 9)
// - the next Ready asks the app to apply index 11 (omitting index 10), losing a
//    write.
tap.test("rawnode rawNodeCommitPaginationAfterRestart()", (t) => {
    const storage = newTestMemoryStorage(withPeers("1"));
    const persistedHardState: HardState = new HardState({
        term: 1,
        vote: "1",
        commit: 10,
    });

    storage.hardState = persistedHardState;
    storage.ents = new Array(10).fill(null);
    let size: number = 0;

    const entForSizeComputation: Entry = {
        term: 1,
        index: 1,
        type: EntryType.EntryNormal,
        data: "a",
    };
    const entSize = JSON.stringify(entForSizeComputation).length;
    for (let i = 0; i < storage.ents.length; i++) {
        const ent: Entry = {
            term: 1,
            index: i + 1,
            type: EntryType.EntryNormal,
            data: "a",
        };

        storage.ents[i] = ent;
        size += entSize;
    }

    const cfg = newTestConfig("1", 10, 1, storage);
    // Set a MaxSizePerMsg that would suggest to Raft that the last committed
    // entry should not be included in the initial rd.CommittedEntries. However,
    // our storage will ignore this and *will* return it (which is how the
    // Commit index ended up being 10 initially).
    cfg.maxSizePerMsg = size - entSize - 1;

    storage.ents.push({
        term: 1,
        index: 11,
        type: EntryType.EntryNormal,
        data: "boom",
    });

    const rawNode = RawNode.NewRawNode(cfg);

    let highestApplied = 0;
    while (highestApplied !== 11) {
        const ready = rawNode.ready();
        const n = ready.committedEntries.length;

        t.notOk(n === 0, "stopped applying entries at index " + highestApplied);
        const next = ready.committedEntries[0].index;
        t.notOk(
            highestApplied !== 0 && highestApplied + 1 !== next,
            "attempting to apply index " +
            next +
            "after index " +
            highestApplied +
            ", leaving a gap"
        );

        highestApplied = ready.committedEntries[n - 1].index;
        rawNode.advance(ready);
        rawNode.step(
            new Message({
                type: MessageType.MsgHeartbeat,
                to: "1",
                from: "1", // illegal, but we get away with it
                term: 1,
                commit: 11,
            })
        );
    }

    t.end();
});

// TestRawNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
// partitioned from a quorum of nodes. It verifies that the leader's log is
// protected from unbounded growth even as new entries continue to be proposed.
// This protection is provided by the MaxUncommittedEntriesSize configuration.
tap.test("rawnode rawNodeBoundedLogGrowthWithPartition()", (t) => {
    const maxEntries = 16;
    const data = "testdata";
    const testEntry: Entry = new Entry({ data: data });
    const payloadSize = JSON.stringify(testEntry.data).length;
    const maxEntrySize = maxEntries * payloadSize;

    const storage = newTestMemoryStorage(withPeers("1"));
    const cfg = newTestConfig("1", 10, 1, storage);
    cfg.maxUncommittedEntriesSize = maxEntrySize;
    const rawNode = RawNode.NewRawNode(cfg);

    let ready = rawNode.ready();
    storage.Append(ready.entries);
    rawNode.advance(ready);

    // Become the leader.
    rawNode.campaign();
    while (true) {
        ready = rawNode.ready();
        storage.Append(ready.entries);
        if (ready.softState !== null && ready.softState.lead === rawNode.raft.id) {
            rawNode.advance(ready);
            break;
        }
        rawNode.advance(ready);
    }

    // Simulate a network partition while we make our proposals by never
    // committing anything. These proposals should not cause the leader's log to
    // grow indefinitely.
    for (let i = 0; i < 1024; i++) {
        rawNode.propose(data);
    }

    // Check the size of leader's uncommitted log tail. It should not exceed the
    // MaxUncommittedEntriesSize limit.
    function checkUncommitted(exp: number) {
        const a = rawNode.raft.uncommittedSize;
        t.equal(a, exp, "expected " + exp + " uncommitted entry bytes, found " + a);
    }
    checkUncommitted(maxEntrySize);

    // Recover from the partition. The uncommitted tail of the Raft log should
    // disappear as entries are committed.
    ready = rawNode.ready();
    t.equal(
        ready.committedEntries.length,
        maxEntries,
        "expected " + maxEntries + " entries, got " + ready.committedEntries.length
    );
    storage.Append(ready.entries);
    rawNode.advance(ready);
    checkUncommitted(0);

    t.end();
});

tap.test("rawnode rawNodeConsumeReady()", (t) => {
    // Check that readyWithoutAccept() does not call acceptReady (which resets
    // the messages) but Ready() does.
    const storage = newTestMemoryStorage(withPeers("1"));
    const rawNode = newTestRawNode("1", 3, 1, storage);
    const message1: Message = new Message({ context: "foo" });
    const message2: Message = new Message({ context: "bar" });

    // Inject first message, make sure it's visible via readyWithoutAccept.
    rawNode.raft.msgs.push(message1);
    let ready = rawNode.readyWithoutAccept();
    t.equal(
        ready.messages.length,
        1,
        "expected only 1 message sent, got " + ready.messages.length + " messages sent"
    );
    t.strictSame(
        ready.messages[0],
        message1,
        "expected: " +
        JSON.stringify(message1) +
        ", sent, but sent " +
        JSON.stringify(ready.messages[0])
    );
    t.equal(
        rawNode.raft.msgs.length,
        1,
        "expected only 1 message in raft.msgs, got " + rawNode.raft.msgs.length + " messages"
    );
    t.strictSame(
        rawNode.raft.msgs[0],
        message1,
        "expected: " +
        JSON.stringify(message1) +
        ", in raft.msgs, got " +
        JSON.stringify(rawNode.raft.msgs[0])
    );

    // Now call Ready() which should move the message into the Ready (as opposed
    // to leaving it in both places).
    ready = rawNode.ready();
    t.notOk(rawNode.raft.msgs.length > 0, "messages not reset: " + rawNode.raft.msgs);
    t.equal(
        ready.messages.length,
        1,
        "expected only m1 sent ,got " + ready.messages.length + "messages sent"
    );
    t.strictSame(
        ready.messages[0],
        message1,
        "expected: " +
        JSON.stringify(message1) +
        ", sent, but sent " +
        JSON.stringify(ready.messages[0])
    );

    // Add a message to raft to make sure that Advance() doesn't drop it.
    rawNode.raft.msgs.push(message2);
    rawNode.advance(ready);
    t.equal(
        rawNode.raft.msgs.length,
        1,
        "expected only 1 message in raft.msgs, got " + rawNode.raft.msgs.length + " messages"
    );
    t.strictSame(
        rawNode.raft.msgs[0],
        message2,
        "expected: " +
        JSON.stringify(message2) +
        ", in raft.msgs, got " +
        JSON.stringify(rawNode.raft.msgs[0])
    );

    t.end();
});

// Helper to set default values for all non provided values
function getConfState(c: ConfState): ConfState {
    return new ConfState({
        voters: c.voters ?? [],
        votersOutgoing: c.votersOutgoing ?? [],
        learners: c.learners ?? [],
        learnersNext: c.learnersNext ?? [],
        autoLeave: c.autoLeave ?? false,
    });
}

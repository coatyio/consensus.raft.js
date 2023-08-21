/*! Copyright 2023 Siemens AG

   Licensed under the Apache License, Version 2.0 (the "License"); you may not
   use this file except in compliance with the License. You may obtain a copy of
   the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   License for the specific language governing permissions and limitations under
   the License.
*/

import tap from "tap";

import {
    RaftConfChangeCommit,
    RaftInputCommit,
    RaftStateMachineWrapper
} from "../../../non-ported/raft-node-interfaces/state-machine-wrapper";
import { DeleteInput, KVRaftStateMachine, SetInput } from "../util";
import { RaftConfiguration } from "../../../non-ported/raft-controller";

const raftConfig: Required<RaftConfiguration> = {
    electionTick: 10,
    heartbeatTick: 1,
    maxSizePerMsg: 1024 * 1024,
    maxCommittedSizePerReady: 1000,
    maxUncommittedEntriesSize: Math.pow(2, 30),
    maxInflightMsgs: 256,
    tickTime: 100,
    defaultSnapCount: 1000,
    snapshotCatchUpEntriesN: 50,
    inputReproposeInterval: 1000,
    confChangeReproposeInterval: 1000,
    queuedUpInputProposalsLimit: 1000,
    runningJoinRequestsLimit: 1000,
    raftEntriesKey: "entries",
    raftHardstateKey: "hardstate",
    raftSnapshotKey: "snapshot",
    snapshotReceiveTimeout: 60000,
    joinRequestRetryInterval: 5000
};

tap.test("state-machine-wrapper processInputProposal()", (t) => {
    const toBeTested1 = new RaftStateMachineWrapper(new KVRaftStateMachine(), "1", "test", raftConfig);
    const toBeTested2 = new RaftStateMachineWrapper(new KVRaftStateMachine(), "2", "test", raftConfig);
    // Add 1 and 2 to the cluster
    const cc1 = toBeTested1.createConfChangeProposal("add", "1");
    const cc2 = toBeTested1.createConfChangeProposal("add", "2");
    // Inputs from node 1
    const input1 = toBeTested1.createInputProposal(SetInput("foo", "43"), false);
    const expected1: RaftInputCommit = {
        inputId: input1.inputId,
        resultingState: [["foo", "43"]],
        rejected: false
    };
    const input2 = toBeTested1.createInputProposal(SetInput("foo", "42"), false);
    const expected2: RaftInputCommit = {
        inputId: input2.inputId,
        resultingState: [["foo", "42"]],
        rejected: false
    };
    // Input from node 2
    const input3 = toBeTested2.createInputProposal(SetInput("hello", "world"), false);
    const expected3: RaftInputCommit = {
        inputId: input3.inputId,
        resultingState: [
            ["foo", "42"],
            ["hello", "world"],
        ],
        rejected: false
    };
    // NOOP input from node 1
    const input5 = toBeTested1.createInputProposal(DeleteInput("foo"), true);
    const expected5: RaftInputCommit = {
        inputId: input5.inputId,
        resultingState: [
            ["foo", "42"],
            ["hello", "world"],
        ],
        rejected: false
    };
    // Remove 1 from the cluster
    const cc3 = toBeTested1.createConfChangeProposal("remove", "1");
    const expected6: RaftInputCommit = {
        inputId: input5.inputId,
        resultingState: null,
        rejected: true
    }

    t.plan(6);
    // Add 1 and 2 to the cluster
    toBeTested1.processConfChangeProposal(cc1);
    toBeTested1.processConfChangeProposal(cc2);
    // Test processInputProposal
    t.strictSame(toBeTested1.processInputProposal(input1), expected1);
    t.strictSame(toBeTested1.processInputProposal(input2), expected2);
    t.strictSame(toBeTested1.processInputProposal(input3), expected3);
    t.strictSame(toBeTested1.processInputProposal(input2), expected2);
    t.strictSame(toBeTested1.processInputProposal(input5), expected5);
    // Remove 1 from the cluster
    toBeTested1.processConfChangeProposal(cc3);
    // Test processInputProposal
    t.strictSame(toBeTested1.processInputProposal(input5), expected6);
});

tap.test("state-machine-wrapper checkIfInputProposalAlreadyApplied()", (t) => {
    const toBeTested = new RaftStateMachineWrapper(new KVRaftStateMachine(), "1", "test", raftConfig);

    const cc1 = toBeTested.createConfChangeProposal("add", "1");
    const input1 = toBeTested.createInputProposal(SetInput("foo", "43"), false);
    const expected1: RaftInputCommit = {
        inputId: input1.inputId,
        resultingState: [["foo", "43"]],
        rejected: false
    };
    const input2 = toBeTested.createInputProposal(SetInput("foo", "22"), false);

    t.plan(3);
    // Add 1 to the cluster
    toBeTested.processConfChangeProposal(cc1);
    // Process input 1 with processInputProposal()
    const result1 = toBeTested.processInputProposal(input1);
    t.strictSame(result1, expected1);
    // Process input 1 with checkIfInputProposalAlreadyApplied()
    const result2 = toBeTested.checkIfInputProposalAlreadyApplied(input1);
    t.strictSame(result2, expected1);
    // Process input 2 with checkIfInputProposalAlreadyApplied()
    const result3 = toBeTested.checkIfInputProposalAlreadyApplied(input2);
    t.strictSame(result3, null);
});

tap.test("state-machine-wrapper processConfChangeProposal()", (t) => {
    const toBeTested = new RaftStateMachineWrapper(new KVRaftStateMachine(), "1", "test", raftConfig);

    // Add 1 -> applied
    const cc1 = toBeTested.createConfChangeProposal("add", "1");
    const expected1: RaftConfChangeCommit = {
        confChangeId: cc1.confChangeId,
        status: "applied",
    };

    // Add 1 again -> noop
    const cc2 = toBeTested.createConfChangeProposal("add", "1");
    const expected2: RaftConfChangeCommit = {
        confChangeId: cc2.confChangeId,
        status: "noop",
    };

    // Remove 1 -> applied
    const cc3 = toBeTested.createConfChangeProposal("remove", "1");
    const expected3: RaftConfChangeCommit = {
        confChangeId: cc3.confChangeId,
        status: "applied",
    };

    // Remove 1 again -> noop
    const cc4 = toBeTested.createConfChangeProposal("remove", "1");
    const expected4: RaftConfChangeCommit = {
        confChangeId: cc4.confChangeId,
        status: "noop",
    };

    // Add 1 again -> noop
    const cc5 = toBeTested.createConfChangeProposal("add", "1");
    const expected5: RaftConfChangeCommit = {
        confChangeId: cc5.confChangeId,
        status: "noop",
    };

    t.plan(5);
    t.strictSame(toBeTested.processConfChangeProposal(cc1), expected1);
    t.strictSame(toBeTested.processConfChangeProposal(cc2), expected2);
    t.strictSame(toBeTested.processConfChangeProposal(cc3), expected3);
    t.strictSame(toBeTested.processConfChangeProposal(cc4), expected4);
    t.strictSame(toBeTested.processConfChangeProposal(cc5), expected5);
});

tap.test("state-machine-wrapper checkIfConfChangeProposalAlreadyApplied()", (t) => {
    const toBeTested = new RaftStateMachineWrapper(new KVRaftStateMachine(), "1", "test", raftConfig);

    // processConfChangeProposal(): Add 1 -> applied
    const cc1 = toBeTested.createConfChangeProposal("add", "1");
    const expected1: RaftConfChangeCommit = {
        confChangeId: cc1.confChangeId,
        status: "applied",
    };

    // checkIfConfChangeProposalAlreadyApplied(): Add 1 again with same nodeId ->
    // noop
    const cc2 = toBeTested.createConfChangeProposal("add", "1");
    const expected2: RaftConfChangeCommit = {
        confChangeId: cc2.confChangeId,
        status: "noop",
    };

    // checkIfConfChangeProposalAlreadyApplied(): Remove 1 -> no commit possible
    const cc3 = toBeTested.createConfChangeProposal("remove", "1");
    const expected3 = null;

    // processConfChangeProposal(): Remove 1 -> applied
    const cc4 = toBeTested.createConfChangeProposal("remove", "1");
    const expected4: RaftConfChangeCommit = {
        confChangeId: cc4.confChangeId,
        status: "applied",
    };

    // checkIfConfChangeProposalAlreadyApplied(): Remove 1 -> noop
    const cc5 = toBeTested.createConfChangeProposal("remove", "1");
    const expected5: RaftConfChangeCommit = {
        confChangeId: cc5.confChangeId,
        status: "noop",
    };

    // checkIfConfChangeProposalAlreadyApplied(): Remove 2 -> no commit possible
    const cc6 = toBeTested.createConfChangeProposal("remove", "2");
    const expected6 = null;

    t.plan(6);
    t.strictSame(toBeTested.processConfChangeProposal(cc1), expected1);
    t.strictSame(toBeTested.checkIfConfChangeProposalAlreadyApplied(cc2), expected2);
    t.strictSame(toBeTested.checkIfConfChangeProposalAlreadyApplied(cc3), expected3);
    t.strictSame(toBeTested.processConfChangeProposal(cc4), expected4);
    t.strictSame(toBeTested.checkIfConfChangeProposalAlreadyApplied(cc5), expected5);
    t.strictSame(toBeTested.checkIfConfChangeProposalAlreadyApplied(cc6), expected6);
});

tap.test("state-machine-wrapper toSnapshotData() + setStateFromSnapshotData()", (t) => {
    const toBeTested1 = new RaftStateMachineWrapper(new KVRaftStateMachine(), "1", "test", raftConfig);
    const toBeTested2 = new RaftStateMachineWrapper(new KVRaftStateMachine(), "2", "test", raftConfig);
    const cc1 = toBeTested1.createConfChangeProposal("add", "1");
    const input1 = toBeTested1.createInputProposal(SetInput("foo", "42"), false);
    const input2 = toBeTested1.createInputProposal(SetInput("foo2", "42"), false);
    const cc2 = toBeTested1.createConfChangeProposal("add", "2");
    const cc3 = toBeTested1.createConfChangeProposal("remove", "1");
    const input3 = toBeTested2.createInputProposal(DeleteInput("foo2"), false);
    const input4 = toBeTested2.createInputProposal(DeleteInput("foo"), true); // NOOP
    const expectedSnapshotData = {
        wrapped: [["foo", "42"]],
        lastCommitPerNode: {
            "2": {
                inputId: input4.inputId,
                resultingState: [["foo", "42"]],
                rejected: false,
            },
        },
        currentClusterMembers: ["2"],
        previousClusterMembers: ["1"],
    };

    t.plan(2);
    toBeTested1.processConfChangeProposal(cc1);
    toBeTested1.processInputProposal(input1);
    toBeTested1.processInputProposal(input2);
    toBeTested1.processConfChangeProposal(cc2);
    toBeTested1.processConfChangeProposal(cc3);
    toBeTested1.processInputProposal(input3);
    toBeTested1.processInputProposal(input4);
    const snapshotData = toBeTested1.toSnapshotData();
    t.strictSame(JSON.parse(JSON.stringify(snapshotData)), expectedSnapshotData);

    toBeTested2.setStateFromSnapshotData(snapshotData);
    t.strictSame(JSON.parse(JSON.stringify(toBeTested2.toSnapshotData())), expectedSnapshotData);
});

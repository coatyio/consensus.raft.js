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

import { Observable, ReplaySubject, Subject } from "rxjs";
import { take } from "rxjs/operators";
import tap from "tap";

import { RaftConfiguration, RaftData } from "../../../non-ported/raft-controller";
import { RaftCommunication } from "../../../non-ported/raft-node-interfaces/communication";
import { RaftCommunicationWrapper } from "../../../non-ported/raft-node-interfaces/communication-wrapper";
import { Message, MessageType, Snapshot, SnapshotMetadata } from "../../../ported/raftpb/raft.pb";

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

class RaftCommunicationMock implements RaftCommunication {
    private _messageReceiveChannels = new Map<string, Subject<RaftData>>();
    private _joinRequestReceiveChannel = new Subject<[RaftData, () => void]>();

    sendRaftMessage(message: RaftData, receiverId: string, cluster: string): void {
        if (cluster !== "test") {
            // Tests for wrapper only require one "test" cluster
            throw new Error("Wrong cluster specified.");
        }
        const receiverChannel = this._messageReceiveChannels.get(receiverId);
        if (receiverChannel === undefined) {
            // Message is lost
        } else {
            // Write to channel
            receiverChannel.next(message);
        }
    }

    startReceivingRaftMessages(id: string, cluster: string): Observable<any> {
        if (cluster !== "test") {
            // Tests for wrapper only require one "test" cluster
            throw new Error("Wrong cluster specified.");
        }
        let channel = this._messageReceiveChannels.get(id);
        if (channel === undefined) {
            // Create a new channel if none exists yet
            channel = new Subject<RaftData>();
            this._messageReceiveChannels.set(id, channel);
        }
        return channel;
    }

    broadcastJoinRequest(joinRequest: any, cluster: string): Observable<void> {
        if (cluster !== "test") {
            // Tests for wrapper only require one "test" cluster
            throw new Error("Wrong cluster specified.");
        }
        // Set up answer channel
        const answerChannel = new ReplaySubject<void>(100);
        // Broadcast join request to all listening nodes
        this._joinRequestReceiveChannel.next([joinRequest, () => answerChannel.next()]);
        return answerChannel;
    }

    startAcceptingJoinRequests(cluster: string): Observable<[any, () => void]> {
        if (cluster !== "test") {
            // Tests for wrapper only require one "test" cluster
            throw new Error("Wrong cluster specified.");
        }
        // Return receive channel
        return this._joinRequestReceiveChannel;
    }
}

tap.test("communication-wrapper simple message send test 1", (t) => {
    const raftCom = new RaftCommunicationMock(); // Use same mock in both wrappers to enable communication
    const toBeTested1 = new RaftCommunicationWrapper(raftCom, "1", "test", raftConfig);
    const toBeTested2 = new RaftCommunicationWrapper(raftCom, "2", "test", raftConfig);
    const toBeSent = new Message({
        type: MessageType.MsgApp,
        to: "1",
        term: 42,
        context: "test",
    });
    t.plan(1);
    // Listen to msg
    toBeTested1
        .startReceivingRaftMessages()
        .pipe(take(1))
        .forEach((msg) => {
            t.strictSame(msg, toBeSent);
        });
    // Send msg
    toBeTested2.sendRaftMessage(toBeSent);
});

tap.test("communication-wrapper simple message send test 2", (t) => {
    const raftCom = new RaftCommunicationMock(); // Use same mock in both wrappers to enable communication
    const toBeTested1 = new RaftCommunicationWrapper(raftCom, "1", "test", raftConfig);
    const toBeTested2 = new RaftCommunicationWrapper(raftCom, "2", "test", raftConfig);
    const toBeSent = new Message({
        type: MessageType.MsgApp,
        to: "1",
        term: 42,
        context: "test",
    });
    t.plan(2);
    // Listen to msg
    toBeTested1
        .startReceivingRaftMessages()
        .pipe(take(2))
        .forEach((msg) => {
            t.strictSame(msg, toBeSent);
        });
    // Send msg twice
    toBeTested2.sendRaftMessage(toBeSent);
    toBeTested2.sendRaftMessage(toBeSent);
});

tap.test("sending MsgApp message with sendRaftSnapshotMessage should fail", (t) => {
    const toBeTested = new RaftCommunicationWrapper(new RaftCommunicationMock(), "1", "test", raftConfig);
    const toBeSent = new Message({
        type: MessageType.MsgApp,
        to: "2",
        term: 42,
        context: "test",
    });
    // Send msg
    t.throws(() => toBeTested.sendRaftSnapshotMessage(toBeSent));
    t.end();
});

tap.test("communication-wrapper testing snap response", (t) => {
    const raftCom = new RaftCommunicationMock(); // Use same mock in both wrappers to enable communication
    const toBeTested1 = new RaftCommunicationWrapper(raftCom, "1", "test", raftConfig);
    const toBeTested2 = new RaftCommunicationWrapper(raftCom, "2", "test", raftConfig);
    const toBeSentSnap = new Message({
        type: MessageType.MsgSnap,
        to: "2",
        context: "snap",
    });
    const toBeSentNormal1to2 = new Message({
        type: MessageType.MsgApp,
        to: "2",
        context: "normal1to2",
    });
    const toBeSentNormal2to1 = new Message({
        type: MessageType.MsgApp,
        to: "1",
        context: "normal2to1",
    });

    t.plan(4);

    // Listen to snap and (normal 1 -> 2) as 2
    let snd = false;
    toBeTested2
        .startReceivingRaftMessages()
        .pipe(take(2))
        .forEach((msg) => {
            if (!snd) {
                snd = true;
                t.strictSame(msg, toBeSentSnap);
            } else {
                t.strictSame(msg, toBeSentNormal1to2);
            }
        })
        .then(() => {
            // Send (normal 2 -> 1)
            toBeTested2.sendRaftMessage(toBeSentNormal2to1);
        });

    // Listen to (normal 2 -> 1) as 1
    toBeTested1
        .startReceivingRaftMessages()
        .pipe(take(1))
        .forEach((msg) => {
            t.strictSame(msg, toBeSentNormal2to1);
        });

    // Send (snap 1 -> 2)
    toBeTested1.sendRaftSnapshotMessage(toBeSentSnap).then((resp) => {
        t.ok(resp);
        // Send (normal 1 -> 2)
        toBeTested1.sendRaftMessage(toBeSentNormal1to2);
    });
});

// NOTE: Change _SNAPSHOT_RECEIVE_TIMEOUT inside communication wrapper to
// shorten this test
// tap.test("communication-wrapper testing timeout if no response", (t) => {
//     const toBeTested = new RaftCommunicationWrapper(new RaftCommunicationMock(), "1", "test");
//     const toBeSent = new Message({
//         type: MessageType.MsgSnap,
//         to: "2",
//         term: 42,
//         context: "test",
//     });
//     // Send msg
//     toBeTested.sendRaftSnapshotMessage(toBeSent).then((resp) => {
//         t.notOk(resp);
//         t.end();
//     });
// });

tap.test("communication-wrapper conversion to/from RaftData test", (t) => {
    const raftCom = new RaftCommunicationMock(); // Use same mock in both wrappers to enable communication
    const toBeTested1 = new RaftCommunicationWrapper(raftCom, "1", "test", raftConfig);
    const toBeTested2 = new RaftCommunicationWrapper(raftCom, "2", "test", raftConfig);
    const toBeSent = new Message({
        type: MessageType.MsgSnap,
        to: "1",
        snapshot: new Snapshot({
            metadata: new SnapshotMetadata({
                index: 42,
            }),
        }),
    });
    t.plan(1);
    // Listen to msg
    toBeTested1
        .startReceivingRaftMessages()
        .pipe(take(1))
        .forEach((msg) => {
            t.strictSame(msg, toBeSent);
        });
    // Send msg
    toBeTested2.sendRaftMessage(toBeSent);
});

tap.test("communication-wrapper proposeJoinRequest() test 1", (t) => {
    console.log("Test can take 5s or longer...");
    const raftCom = new RaftCommunicationMock(); // Use same mock in both wrappers to enable communication
    const toBeTested1 = new RaftCommunicationWrapper(raftCom, "1", "test", raftConfig);
    const toBeTested2 = new RaftCommunicationWrapper(raftCom, "2", "test", raftConfig);
    t.plan(2);
    toBeTested1.proposeJoinRequest().then(() => {
        t.pass();
    });
    toBeTested2
        .startAcceptingJoinRequests()
        .pipe(take(1))
        .subscribe(([request, answerCallback]) => {
            t.strictSame(request, "1");
            answerCallback();
        });
});

tap.test("communication-wrapper proposeJoinRequest() test 2", (t) => {
    console.log("Test can take 10s or longer...");
    const raftCom = new RaftCommunicationMock(); // Use same mock in both wrappers to enable communication
    const toBeTested1 = new RaftCommunicationWrapper(raftCom, "1", "test", raftConfig);
    const toBeTested2 = new RaftCommunicationWrapper(raftCom, "2", "test", raftConfig);
    t.plan(2);
    toBeTested1.proposeJoinRequest().then(() => {
        t.pass();
    });
    let answerIndex = 0;
    let firstAnswerCallback: () => void;

    toBeTested2
        .startAcceptingJoinRequests()
        .pipe(take(2))
        .subscribe(([request, answerCallback]) => {
            answerIndex++;
            if (answerIndex === 1) {
                firstAnswerCallback = answerCallback;
            } else if (answerIndex === 2) {
                t.strictSame(request, "1");
                firstAnswerCallback();
            }
        });
});

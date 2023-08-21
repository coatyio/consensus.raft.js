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

import { Runtime } from "@coaty/core";
import { Observable, Subscription } from "rxjs";
import { filter, map, take } from "rxjs/operators";

import { RaftCommunication } from "./communication";
import { Message, MessageType } from "../../ported/raftpb/raft.pb";
import { RaftConfiguration } from "../raft-controller";

/**
 * Wrapper for the `RaftCommunication` interface. Is used inside `RaftNode` to
 * send and receive messages and join requests. This additional layer on top of
 * `RaftCommunication` provides better usability to `RaftNode` while keeping the
 * `RaftCommunication` interface simple.
 *
 * - Sends and receives messages for raft
 * - Gives info on whether or not a sent out snapshot messages was received
 * - Broadcasts and listens to join requests
 * - Makes sure that sent out data is properly serialized/deserialized
 */
export class RaftCommunicationWrapper {
    private readonly _wrapped: RaftCommunication;
    private readonly _id: string;
    private readonly _cluster: string;
    private readonly _raftConfig: Required<RaftConfiguration>;

    constructor(wrapped: RaftCommunication, id: string, cluster: string, raftConfig: Required<RaftConfiguration>) {
        this._wrapped = wrapped;
        this._id = id;
        this._cluster = cluster;
        this._raftConfig = raftConfig;
    }

    /**
     * Sends the provided message to the `RaftNode` specified in the respective
     * `Message.to` field.
     *
     * @param message The message that will be send.
     */
    sendRaftMessage(message: Message) {
        const raftMessage: RaftMessage = {
            type: RaftMessageType.Normal,
            message: message,
        };
        this._wrapped.sendRaftMessage(raftMessage, message.to, this._cluster);
    }

    /**
     * Sends the provided snapshot message to the `RaftNode` specified in the
     * respective `Message.to` field.
     *
     * @param message The snapshot message that will be send.
     *
     * @returns A Promise resolving to true when the snapshot contained in the
     * provided message was received or false after a timeout.
     */
    sendRaftSnapshotMessage(message: Message): Promise<boolean> {
        if (message.type !== MessageType.MsgSnap) {
            throw new Error(
                "The message provided to sendRaftSnapshotMessage() is not a snapshot message. " +
                "Use sendRaftMessage() for messages that are not snapshot messages."
            );
        }
        return new Promise<boolean>((resolve) => {
            const openSendId = Runtime.newUuid();
            let waitForSnapshotAnswer: Subscription | undefined;
            let timeout: NodeJS.Timeout | undefined;

            // Has to be called before resolving this promise
            const cleanupSubscriptionAndTimer = () => {
                waitForSnapshotAnswer?.unsubscribe();
                clearTimeout(timeout);
            };

            // Resolve with true if RaftSnapshotAnswer was received
            waitForSnapshotAnswer = this._wrapped
                .startReceivingRaftMessages(this._id, this._cluster)
                .subscribe((msg: RaftMessage | RaftSnapshotMessage | RaftSnapshotAnswer) => {
                    if (
                        msg.type === RaftMessageType.SnapshotAnswer &&
                        msg.openSendId === openSendId
                    ) {
                        cleanupSubscriptionAndTimer();
                        resolve(true);
                    }
                });

            // Resolve with false after timeout
            timeout = setTimeout(() => {
                cleanupSubscriptionAndTimer();
                resolve(false);
            }, this._raftConfig.snapshotReceiveTimeout);

            // Send out RaftSnapshotMessage
            const raftSnapshotMessage: RaftSnapshotMessage = {
                type: RaftMessageType.Snapshot,
                openSendId: openSendId,
                senderId: this._id,
                message: message,
            };
            this._wrapped.sendRaftMessage(raftSnapshotMessage, message.to, this._cluster);
        });
    }

    /**
     * Returns an observable containing all messages sent to the Raft node
     * associated with this wrapper.
     */
    startReceivingRaftMessages(): Observable<Message> {
        return this._wrapped.startReceivingRaftMessages(this._id, this._cluster).pipe(
            filter((msg: RaftMessage | RaftSnapshotMessage | RaftSnapshotAnswer) => {
                // 1) Filter out RaftSnapshotAnswers
                return msg.type !== RaftMessageType.SnapshotAnswer;
            }),
            map((msg: RaftMessage | RaftSnapshotMessage) => {
                // 2) Respond to RaftSnapshotMessages with RaftSnapshotAnswer
                if (msg.type === RaftMessageType.Snapshot) {
                    const snapResponse: RaftSnapshotAnswer = {
                        type: RaftMessageType.SnapshotAnswer,
                        openSendId: msg.openSendId,
                    };
                    this._wrapped.sendRaftMessage(snapResponse, msg.senderId, this._cluster);
                }

                // 3) Convert from wrappers to actual messages
                return msg.message;
            }),
            map((msg) => {
                // 4) Readd functions lost from serialization
                return new Message(msg);
            })
        );
    }

    /**
     * Broadcasts a join request for the Raft node associated with this wrapper
     * and resolves once it was accepted. If the join request isn't accepted
     * within `this._joinRequestRetryInterval` a new broadcast is started. This
     * function also resolves if an old broadcast is accepted after a new one
     * was already started. After a limit of 100 simultaneously running
     * broadcasts is reached the oldest broadcast gets killed whenever a new one
     * is started which means that the accept answers to that broadcast are no
     * longer processed. Broadcasted join requests are accepted inside
     * `this.startAcceptingJoinRequests()`. Broadcasting a join request is
     * idempotent.
     */
    proposeJoinRequest(): Promise<void> {
        return new Promise<void>((resolve) => {
            let retryTimer: NodeJS.Timeout | undefined;
            const runningBroadcastSubscriptions: Subscription[] = [];

            // Has to be called before this promise is resolved
            const cleanupSubscriptionsAndTimer = () => {
                runningBroadcastSubscriptions.forEach((sub) => sub.unsubscribe());
                clearInterval(retryTimer);
            };

            // Call to start a new broadcast and save subscription to answer
            const startNewBroadcast = () => {
                if (runningBroadcastSubscriptions.length >= 100) {
                    // Restrict number of possible open broadcasts to 100
                    runningBroadcastSubscriptions.shift()?.unsubscribe();
                }
                runningBroadcastSubscriptions.push(
                    this._wrapped
                        .broadcastJoinRequest(this._id, this._cluster)
                        .pipe(take(1))
                        .subscribe(() => {
                            cleanupSubscriptionsAndTimer();
                            resolve();
                        })
                );
            };

            // Start broadcasting the join request
            retryTimer = setInterval(startNewBroadcast, this._raftConfig.joinRequestRetryInterval);
            startNewBroadcast();
        });
    }

    /**
     * @returns An observable that, whenever a new join request has been
     * broadcasted to the Raft node associated with this wrapper, emits a tuple
     * containing the node id that wants to join and an accept callback that can
     * be used to signal to the broadcaster that the join request was accepted.
     */
    startAcceptingJoinRequests(): Observable<[string, () => void]> {
        return this._wrapped.startAcceptingJoinRequests(this._cluster);
    }
}

enum RaftMessageType {
    Normal,
    Snapshot,
    SnapshotAnswer,
}

/**
 * Wraps around messages sent via `sentRaftMessage(...)`.
 */
interface RaftMessage {
    type: RaftMessageType.Normal;

    /**
     * The raftpb.Message to be sent.
     */
    message: any;
}

/**
 * Wraps around messages sent via `sendRaftSnapshotMessage(...)` and triggers a
 * `SnapshotReceivedAnswer` once it was received successfully.
 */
interface RaftSnapshotMessage {
    type: RaftMessageType.Snapshot;

    /**
     *  Identifies the snapshot this wrapper belongs to.
     */
    openSendId: string;

    /**
     * Identifies the node that will receive the SnapshotReceivedAnswer.
     */
    senderId: string;

    /**
     * The raftpb.Message holding the snapshot.
     */
    message: any;
}

/**
 * Is sent as confirmation that a snapshot has been received.
 */
interface RaftSnapshotAnswer {
    type: RaftMessageType.SnapshotAnswer;

    /**
     * Identifies the snapshot this answer belongs to.
     */
    openSendId: string;
}

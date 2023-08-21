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

import {
    CallEvent,
    ChannelEvent,
    CoatyObject,
    Controller,
    ReturnEvent
} from "@coaty/core";
import { Observable } from "rxjs";
import { map } from "rxjs/operators";

import { RaftData } from "../raft-controller";

/**
 * Interface used by Raft to send messages to and receive messages from other
 * nodes as well as to broadcast join requests to and answer join requests from
 * other nodes.
 *
 * @remark Users can overwrite
 * {@linkcode RaftController.getRaftCommunicationImplementation} to provide
 * their own implementation of this interface if needed.
 *
 * @category Raft Interfaces
 */
export interface RaftCommunication {
    /**
     * Sends the provided `message` to the Raft node specified by `receiverId`
     * in the cluster specified by `cluster`. The message will be lost if the
     * receiving node has not called `startReceivingRaftMessages()` yet.
     *
     * @param message The message that should be sent. Is of type
     * {@linkcode RaftData}.
     *
     * @param receiverId Identifies the node the message should be sent to.
     *
     * @param cluster Identifies the cluster of the node the message should be
     * sent to.
     *
     * @category Messages (one-to-one)
     */
    sendRaftMessage(message: RaftData, receiverId: string, cluster: string): void;

    /**
     * Returns an observable that can be used to observe messages sent to the
     * Raft node specified by the provided `receiverId` in the cluster specified
     * by `cluster` via `sendRaftMessage()`. The returned observable should emit
     * all messages sent to this node after this function was called.
     *
     * @param receiverId Specifies the Raft node whose messages will be
     * received.
     *
     * @param cluster Specifies the cluster of the Raft node whose messages will
     * be received.
     *
     * @returns An observable that, whenever a new message has been received,
     * emits it. The received messages should be of type {@linkcode RaftData}.
     *
     * @category Messages (one-to-one)
     */
    startReceivingRaftMessages(receiverId: string, cluster: string): Observable<RaftData>;

    /**
     * Broadcasts the provided `joinRequest` to all Raft nodes in the Raft
     * cluster specified by `cluster` that are listening through
     * `startAcceptingJoinRequests()` and returns an observable that emits
     * whenever a listening node accepts the join request.
     *
     * @param joinRequest The join request that should be broadcasted. Is of
     * type {@linkcode RaftData}.
     *
     * @param cluster Specifies the cluster inside which the join request should
     * be broadcasted.
     *
     * @returns An observable that, whenever a listening node accepts the join
     * request, emits.
     *
     * @category Join Requests (one-to-many)
     */
    broadcastJoinRequest(joinRequest: RaftData, cluster: string): Observable<void>;

    /**
     * Returns an observable that emits whenever a new join request has been
     * broadcasted via `broadcastJoinRequest()` in the cluster specified by
     * `cluster`. The emitted tuple consists of the broadcasted join request
     * together with an accept callback that can be used to signal to the
     * broadcaster that the join request was accepted.
     *
     * @param cluster Specifies the cluster whose broadcasted join requests
     * should be emitted from the returned observable.
     *
     * @returns An observable that, whenever a new join request has been
     * broadcasted, emits a tuple containing the join request and an accept
     * callback that can be used to signal to the broadcaster that the join
     * request was accepted. The join request should be of type
     * {@linkcode RaftData}.
     *
     * @category Join Requests (one-to-many)
     */
    startAcceptingJoinRequests(cluster: string): Observable<[RaftData, () => void]>;
}

/**
 * Implementation of the `RaftCommunication` interface. Uses Coaty to send and
 * receive messages and join requests.
 *
 * @remark Is used as "Sub-controller" inside `RaftController`. Gets dynamically
 * registered at runtime.
 */
export class RaftCommunicationController extends Controller implements RaftCommunication {
    private readonly _OBJECT_TYPE_CONSENSUS_RAFT_MESSAGE = "raft.Message";
    private readonly _OPERATION_NAME_RAFT_JOIN_REQUEST = "raft.JoinRequest";

    sendRaftMessage(message: RaftData, receiverId: string, cluster: string): void {
        const wrapper: ConsensusRaftMessage = {
            objectId: this.runtime.newUuid(),
            objectType: this._OBJECT_TYPE_CONSENSUS_RAFT_MESSAGE,
            coreType: "CoatyObject",
            name: "Raft Message",
            message: message,
        };
        this.communicationManager.publishChannel(
            ChannelEvent.withObject(wrapper.objectType + "." + this._createIdWithCluster(receiverId, cluster), wrapper)
        );
    }

    startReceivingRaftMessages(receiverId: string, cluster: string): Observable<RaftData> {
        return this.communicationManager
            .observeChannel(this._OBJECT_TYPE_CONSENSUS_RAFT_MESSAGE + "." + this._createIdWithCluster(receiverId, cluster))
            .pipe(
                map((event) => event.data.object as ConsensusRaftMessage),
                map((wrapper) => wrapper.message)
            );
    }

    broadcastJoinRequest(joinRequest: RaftData, cluster: string): Observable<void> {
        return this.communicationManager
            .publishCall(
                CallEvent.with(this._OPERATION_NAME_RAFT_JOIN_REQUEST + "." + cluster, {
                    joinRequest: joinRequest,
                })
            )
            .pipe(
                map((_) => {
                    return;
                })
            );
    }

    startAcceptingJoinRequests(cluster: string): Observable<[RaftData, () => void]> {
        return this.communicationManager.observeCall(this._OPERATION_NAME_RAFT_JOIN_REQUEST + "." + cluster).pipe(
            map((event) => {
                const joinRequest = event.data.getParameterByName("joinRequest");
                const answerCallback = () => event.returnEvent(ReturnEvent.withResult(null));
                return [joinRequest, answerCallback];
            })
        );
    }

    private _createIdWithCluster(id: string, cluster: string) {
        const maxPrefix = Number.MAX_SAFE_INTEGER.toString(36).length;
        const prefix = id.length.toString(36).padStart(maxPrefix, "0");
        return prefix + id + cluster;
    }
}

/**
 * The `RaftCommunicationController` uses this wrapper to send and receive raft
 * messages.
 */
interface ConsensusRaftMessage extends CoatyObject {
    /**
     * Message that should be sent to another node.
     */
    message: RaftData;
}

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

import { CallEvent, Controller } from "@coaty/core";
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { map, timeout } from "rxjs/operators";

import {
    RaftClusterConfiguration,
    OPERATION_NAME_REQUEST_RAFT_CLUSTER_CONFIGURATION,
    OBJECT_TYPE_RAFT_CLUSTER_CONFIGURATION,
    CONNECTION_STATE_LOST_ASSUMPTION_TIME,
    ConnectionState,
    ConnectionStateWrapper,
    OBJECT_TYPE_CONNECTION_STATE,
    OBJECT_TYPE_RAFT_STATE,
    OPERATION_NAME_CONNECT,
    OPERATION_NAME_DELETE,
    OPERATION_NAME_DISCONNECT,
    OPERATION_NAME_REQUEST_RAFT_STATE,
    OPERATION_NAME_SET,
    OPERATION_NAME_STOP,
    RaftClusterConfigurationWrapper,
    RaftState,
    RaftStateWrapper
} from "../shared";

export class RaftAgentController extends Controller {
    private _connectionStateSubject!: BehaviorSubject<ConnectionState>;
    private _raftStateSubject!: Subject<RaftState>;
    private _raftClusterConfigurationSubject!: Subject<RaftClusterConfiguration>;

    /**
     * Returns an observable that provides access to the `ConnectionState` of
     * the raft agent over time.
     */
    get connectionStateChange$(): Observable<ConnectionState> {
        return this._connectionStateSubject.asObservable();
    }

    /**
     * Returns an observable that provides access to the `RaftState` of the raft
     * cluster over time.
     */
    get raftStateChange$(): Observable<RaftState> {
        return this._raftStateSubject.asObservable();
    }

    /**
     * Returns an observable that provides access to the
     * `RaftClusterConfiguration` of the Raft cluster over time.
     */
    get raftClusterConfigurationChange$(): Observable<RaftClusterConfiguration> {
        return this._raftClusterConfigurationSubject.asObservable();
    }

    onInit(): void {
        super.onInit();
        this._connectionStateSubject = new BehaviorSubject<ConnectionState>(
            ConnectionState.AgentOffline
        );
        this._raftStateSubject = new Subject<RaftState>();
        this._raftClusterConfigurationSubject = new Subject<RaftClusterConfiguration>();
    }

    onDispose(): void {
        super.onDispose();
        this._connectionStateSubject.complete();
        this._raftStateSubject.complete();
        this._raftClusterConfigurationSubject.complete();
    }

    onCommunicationManagerStarting(): void {
        super.onCommunicationManagerStarting();
        this._startListeningToConnectionStateUpdates();
        this._startListeningToRaftStateUpdates();
        this._startListeningToRaftClusterConfigurationUpdates();
    }

    /**
     * The agent sends `ConnectionState` updates in form of
     * `ConnectionStateWrapper` objects to this controller via Coaty. This
     * happens in regular time intervals. This method starts listening to those
     * connection state updates and updates `this._connectionStateSubject`
     * accordingly. We assume a value of `ConnectionState.AgentOffline` whenever
     * the time since the last update exceeds the
     * `CONNECTION_STATE_LOST_ASSUMPTION_TIME`.
     */
    private _startListeningToConnectionStateUpdates(): void {
        this.communicationManager
            .observeAdvertiseWithObjectType(OBJECT_TYPE_CONNECTION_STATE)
            .pipe(
                map((event) => {
                    const wrapper = event.data.object as ConnectionStateWrapper;
                    return wrapper.connectionState;
                }),
                timeout(CONNECTION_STATE_LOST_ASSUMPTION_TIME)
            )
            .subscribe(
                (state) => {
                    // Received state update from raft agent
                    if (state !== this._connectionStateSubject.value) {
                        console.log(`Connection state changed to: ${state}`);
                        this._connectionStateSubject.next(state);
                        // Request Raft state whenever we switch to connected
                        // view
                        if (state === ConnectionState.ConnectedToRaft) {
                            this._requestRaftStateUpdate();
                            this._requestRaftClusterConfigurationUpdate();
                        }
                    }
                },
                () => {
                    // Request timed out => we assume that raft agent connection
                    // is interrupted
                    const state = ConnectionState.AgentOffline;
                    if (state !== this._connectionStateSubject.value) {
                        console.log(`Connection state changed to: ${state}`);
                        this._connectionStateSubject.next(state);
                    }
                    // Immediately start listening to connection state updates
                    // again
                    setTimeout(() => {
                        this._startListeningToConnectionStateUpdates();
                    });
                }
            );
    }

    /**
     * The agent sends `RaftState` updates in form of `RaftStateWrapper` objects
     * to this controller via Coaty. This happens whenever a new Raft state
     * change was detected by the agent's `RaftController`. This method starts
     * listening to those Raft state updates and updates
     * `this._raftStateSubject` accordingly.
     */
    private _startListeningToRaftStateUpdates(): void {
        this.communicationManager
            .observeAdvertiseWithObjectType(OBJECT_TYPE_RAFT_STATE)
            .pipe(
                map((event) => {
                    const wrapper = event.data.object as RaftStateWrapper;
                    return wrapper.raftState;
                })
            )
            .subscribe((state) => {
                this._raftStateSubject.next(state);
            });
    }

    /**
     * Requests the current `RaftState` from the raft agent.
     */
    private _requestRaftStateUpdate(): void {
        this.communicationManager
            .publishCall(CallEvent.with(OPERATION_NAME_REQUEST_RAFT_STATE))
            .pipe(map((event) => event.data.result))
            .subscribe((result) => {
                if (!result) {
                    console.error(
                        "Raft agent encountered an error while processing a Raft state request"
                    );
                } else {
                    // Resulting state will be observed via
                    // _startListeningToRaftStateUpdates()
                }
            });
    }

    /**
     * The agent sends `RaftClusterConfiguration` updates in form of
     * `RaftClusterConfigurationWrapper` objects to this controller via Coaty.
     * This happens whenever a new Raft cluster configuration change was
     * detected by the agent's `RaftController`. This method starts listening to
     * those Raft cluster configuration updates and updates
     * `this._raftClusterConfigurationSubject` accordingly.
     */
    private _startListeningToRaftClusterConfigurationUpdates(): void {
        this.communicationManager
            .observeAdvertiseWithObjectType(OBJECT_TYPE_RAFT_CLUSTER_CONFIGURATION)
            .pipe(
                map((event) => {
                    const wrapper = event.data.object as RaftClusterConfigurationWrapper;
                    return wrapper.raftClusterConfiguration;
                })
            )
            .subscribe((configuration) => {
                this._raftClusterConfigurationSubject.next(configuration);
            });
    }

    /**
     * Requests the current `RaftClusterConfiguration` from the raft agent.
     */
    private _requestRaftClusterConfigurationUpdate(): void {
        this.communicationManager
            .publishCall(CallEvent.with(OPERATION_NAME_REQUEST_RAFT_CLUSTER_CONFIGURATION))
            .pipe(map((event) => event.data.result))
            .subscribe((result) => {
                if (!result) {
                    console.error(
                        "Raft agent encountered an error while processing a Raft cluster configuration request"
                    );
                } else {
                    // Resulting configuration will be observed via
                    // _startListeningToRaftClusterConfigurationUpdates()
                }
            });
    }

    /**
     * Remotely calls `connect()` on the raft agents `RaftController`. The
     * returned promise resolves if connecting was successful or is rejected if
     * an error was encountered while trying to connect.
     */
    connect(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.communicationManager
                .publishCall(CallEvent.with(OPERATION_NAME_CONNECT))
                .pipe(map((event) => event.data.result))
                .subscribe((result) => {
                    if (result) {
                        console.log("Raft agent connected to Raft cluster");
                        resolve();
                    } else {
                        const errMsg = "Raft agent encountered an error while trying to connect";
                        console.error(errMsg);
                        reject(errMsg);
                    }
                });
        });
    }

    /**
     * Remotely calls `propose()` with a `KVSet` object  on the raft agents
     * `RaftController`. This sets/updates the specified key-value pair inside
     * the key value store managed by the Raft cluster. The returned promise
     * resolves if setting/updating was successful or is rejected if an error
     * was encountered while trying to set/update.
     */
    set(key: string, value: string): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.communicationManager
                .publishCall(CallEvent.with(OPERATION_NAME_SET, { key: key, value: value }))
                .pipe(map((event) => event.data.result))
                .subscribe((result) => {
                    if (result) {
                        resolve();
                    } else {
                        const errMsg = `Raft agent encountered an error while trying to set(k: '${key}',v: '${value}')`;
                        console.error(errMsg);
                        reject(errMsg);
                    }
                });
        });
    }

    /**
     * Remotely calls `propose()` with a `KVDelete` object  on the raft agents
     * `RaftController`. This deletes the specified key-value pair inside the
     * key value store managed by the Raft cluster. The returned promise
     * resolves if deleting was successful or is rejected if an error was
     * encountered while trying to delete.
     */
    delete(key: string): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.communicationManager
                .publishCall(CallEvent.with(OPERATION_NAME_DELETE, { key: key }))
                .pipe(map((event) => event.data.result))
                .subscribe((result) => {
                    if (result) {
                        resolve();
                    } else {
                        const errMsg = `Raft agent encountered an error while trying to delete(k: '${key}')`;
                        console.error(errMsg);
                        reject(errMsg);
                    }
                });
        });
    }

    /**
     * Remotely calls `stop()` on the raft agents `RaftController`. The returned
     * promise resolves if stopping was successful or is rejected if an error
     * was encountered while trying to stop.
     */
    stop(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this._connectionStateSubject.next(ConnectionState.Stopping);
            this.communicationManager
                .publishCall(CallEvent.with(OPERATION_NAME_STOP))
                .pipe(map((event) => event.data.result))
                .subscribe((result) => {
                    if (result) {
                        console.log("Raft agent stopped");
                        resolve();
                    } else {
                        const errMsg = "Raft agent encountered an error while trying to stop";
                        console.error(errMsg);
                        reject(errMsg);
                    }
                });
        });
    }

    /**
     * Remotely calls `disconnect()` on the raft agents `RaftController`. The
     * returned promise resolves if disconnecting was successful or is rejected
     * if an error was encountered while trying to disconnect.
     */
    disconnect(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this._connectionStateSubject.next(ConnectionState.DisconnectingFromRaft);
            this.communicationManager
                .publishCall(CallEvent.with(OPERATION_NAME_DISCONNECT))
                .pipe(map((event) => event.data.result))
                .subscribe((result) => {
                    if (result) {
                        console.log("Raft agent disconnected from Raft cluster");
                        resolve();
                    } else {
                        const errMsg = "Raft agent encountered an error while trying to disconnect";
                        console.error(errMsg);
                        reject(errMsg);
                    }
                });
        });
    }
}

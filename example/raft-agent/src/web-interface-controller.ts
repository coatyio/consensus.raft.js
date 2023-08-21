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

import { RaftController } from "@coaty/consensus.raft";
import { AdvertiseEvent, Controller, ReturnEvent } from "@coaty/core";
import Debug from "debug";
import { take } from "rxjs/operators";

import { deleteDBFile } from "./db";
import { DeleteInput, SetInput } from "./kv-state-machine";
import {
    CONNECTION_STATE_SEND_INTERVAL,
    ConnectionState,
    OPERATION_NAME_CONNECT,
    OPERATION_NAME_DELETE,
    OPERATION_NAME_DISCONNECT,
    OPERATION_NAME_REQUEST_RAFT_CLUSTER_CONFIGURATION,
    OPERATION_NAME_REQUEST_RAFT_STATE,
    OPERATION_NAME_SET,
    OPERATION_NAME_STOP,
    RaftClusterConfiguration,
    RaftState,
    wrapConnectionState,
    wrapRaftClusterConfiguration,
    wrapRaftState
} from "./shared";

/**
 * Coaty controller used to connect to the web interface. This
 * `WebInterfaceController` will relay the remote calls of the web interface to
 * this agent's `RaftController`.
 *
 * Web app <-> WebInterfaceController <-> RaftController <-> Raft cluster
 */
export class WebInterfaceController extends Controller {

    /**
     * Id of this agent in the Raft cluster.
     *
     * Gets assigned with `assignRaftController(...)` inside `agent.ts`.
     */
    private _id: string;

    /**
     * The `RaftController` used to connect to/disconnect from the Raft cluster
     * and access/modify the shared Raft state represented by a
     * `KVStateMachine`.
     *
     * Gets assigned with `assignRaftController(...)` inside `agent.ts`.
     */
    private _raftController: RaftController;

    /**
     * Logger used by this controller.
     *
     * Gets assigned with `assignRaftController(...)` inside `agent.ts`.
     */
    private _logger: Debug.Debugger;

    /**
     * This Raft agent sends `ConnectionState` updates in form of
     * `ConnectionStateWrapper` objects to the web interface via Coaty. This
     * happens in regular time intervals and whenever the connection state
     * changes.
     */
    private _connectionStateInternal: ConnectionState = ConnectionState.AgentOffline;
    private _connectionStateUpdateInterval: NodeJS.Timeout;

    private get _connectionState() {
        return this._connectionStateInternal;
    }

    private set _connectionState(newState: ConnectionState) {
        this._connectionStateInternal = newState;
        this.communicationManager.publishAdvertise(
            AdvertiseEvent.withObject(wrapConnectionState(newState))
        );
    }

    onCommunicationManagerStarting(): void {
        super.onCommunicationManagerStarting();
        this._startSendingConnectionStateUpdates();
    }

    onCommunicationManagerStopping(): void {
        super.onCommunicationManagerStopping();
        clearInterval(this._connectionStateUpdateInterval);
    }

    /**
     * This method is called once on startup inside `agent.ts`. It stores a
     * reference to this agent's `RaftController` inside the
     * `WebInterfaceController`. This is needed since the web interface will
     * remotely be calling methods on the `RaftController` through the
     * `WebInterfaceController`.
     *
     * Web interface <-> WebInterfaceController <-> RaftController <-> Raft
     * cluster
     */
    assignRaftController(raftController: RaftController, id: string, logger: Debug.Debugger) {
        this._raftController = raftController;
        this._id = id;
        this._logger = logger;
        this._connectionState = ConnectionState.DisconnectedFromRaft;
        this._startListeningToConnectRequest();
    }

    /**
     * Starts sending `ConnectionState` updates in form of
     * `ConnectionStateWrapper` objects to the web interface via Coaty. This
     * happens in regular time intervals.
     *
     * See `CONNECTION_STATE_SEND_INTERVAL`.
     */
    private _startSendingConnectionStateUpdates() {
        this._connectionStateUpdateInterval = setInterval(() => {
            this.communicationManager.publishAdvertise(
                AdvertiseEvent.withObject(wrapConnectionState(this._connectionState))
            );
        }, CONNECTION_STATE_SEND_INTERVAL);
    }

    /**
     * Starts listening to the "connect to raft" request by the web interface
     * and relays it to the `RaftController`.
     */
    private _startListeningToConnectRequest() {
        this.communicationManager
            .observeCall(OPERATION_NAME_CONNECT)
            .pipe(take(1))
            .subscribe((event) => {
                this._logger(`Connecting to Raft cluster`);
                this._connectionState = ConnectionState.ConnectingToRaft;
                this._raftController
                    .connect()
                    .then(() => {
                        this._logger(`Connected to Raft cluster`);
                        this._startListeningToKVSet();
                        this._startListeningToKVDelete();
                        this._startListeningToStopRequest();
                        this._startListeningToDisconnectRequest();
                        this._startSendingRaftStateUpdates();
                        this._startListeningToRaftStateRequests();
                        this._startSendingClusterConfigurationUpdates();
                        this._startListeningToClusterConfigurationRequests();
                        event.returnEvent(ReturnEvent.withResult(true));
                        this._connectionState = ConnectionState.ConnectedToRaft;
                    })
                    .catch((err: any) => {
                        this._logger("%s: %s", err.name, err.message);
                        event.returnEvent(ReturnEvent.withResult(false));
                    });
            });
    }

    /**
     * Starts listening to the set/update requests by the web interface and
     * relays them to the `RaftController`.
     */
    private _startListeningToKVSet() {
        this.communicationManager.observeCall(OPERATION_NAME_SET).subscribe((event) => {
            const key = event.data.getParameterByName("key");
            const value = event.data.getParameterByName("value");
            if (
                typeof key !== "string" ||
                typeof value !== "string" ||
                key === "" ||
                value === ""
            ) {
                // Invalid set request
                this._logger("%s: %s", "Error", `Received invalid Set(k: ${key}, v: ${value})`);
                event.returnEvent(ReturnEvent.withResult(false));
            } else {
                // Valid set request
                this._logger(`Proposing Set(k: ${key}, v: ${value}) to Raft cluster`);
                this._raftController
                    .propose(SetInput(key, value))
                    .then((resultingState: any) => {
                        this._logger(`Set(k: ${key}, v: ${value}) was accepted by Raft cluster - Resulting state: [${resultingState}]`
                        );
                        event.returnEvent(ReturnEvent.withResult(true));
                    })
                    .catch((err: any) => {
                        this._logger("%s: %s", err.name, err.message);
                        event.returnEvent(ReturnEvent.withResult(false));
                    });
            }
        });
    }

    /**
     * Starts listening to the delete requests by the web interface and relays
     * them to the `RaftController`.
     */
    private _startListeningToKVDelete() {
        this.communicationManager.observeCall(OPERATION_NAME_DELETE).subscribe((event) => {
            const key = event.data.getParameterByName("key");
            if (typeof key !== "string" || key === "") {
                // Invalid delete request
                this._logger("%s: %s", "Error", `Received invalid Delete(k: ${key})`);
                event.returnEvent(ReturnEvent.withResult(false));
            } else {
                // Valid delete request
                this._logger(`Proposing Delete(k: ${key}) to Raft cluster`);
                this._raftController
                    .propose(DeleteInput(key))
                    .then((resultingState: any) => {
                        this._logger(`Delete(k: ${key}) was accepted by Raft cluster - Resulting state: [${resultingState}]`
                        );
                        event.returnEvent(ReturnEvent.withResult(true));
                    })
                    .catch((err: any) => {
                        this._logger("%s: %s", err.name, err.message);
                        event.returnEvent(ReturnEvent.withResult(false));
                    });
            }
        });
    }

    /**
     * Starts listening to the stop request by the web interface and relays it
     * to the `RaftController`.
     */
    private _startListeningToStopRequest() {
        this.communicationManager
            .observeCall(OPERATION_NAME_STOP)
            .pipe(take(1))
            .subscribe((event) => {
                this._logger("Stopping");
                this._connectionState = ConnectionState.Stopping;
                this._raftController
                    .stop()
                    .then(() => {
                        this._logger("Stopped");
                        this._connectionState = ConnectionState.DisconnectedFromRaft;
                        this._startListeningToConnectRequest();
                        event.returnEvent(ReturnEvent.withResult(true));
                    })
                    .catch((err: any) => {
                        this._logger("%s: %s", err.name, err.message);
                        event.returnEvent(ReturnEvent.withResult(false));
                    });
            });
    }

    /**
     * Starts listening to the "disconnect from raft" request by the web
     * interface and relays it to the `RaftController`.
     */
    private _startListeningToDisconnectRequest() {
        this.communicationManager
            .observeCall(OPERATION_NAME_DISCONNECT)
            .pipe(take(1))
            .subscribe((event) => {
                this._logger("Disconnecting from Raft cluster");
                this._connectionState = ConnectionState.DisconnectingFromRaft;
                this._raftController
                    .disconnect()
                    .then(() => {
                        this._logger("Disconnected from Raft cluster");
                        this._connectionState = ConnectionState.DisconnectedFromRaft;
                        event.returnEvent(ReturnEvent.withResult(true));
                        this._shutdown();
                    })
                    .catch((err: any) => {
                        this._logger("%s: %s", err.name, err.message);
                        event.returnEvent(ReturnEvent.withResult(false));
                    });
            });
    }

    /**
     * Starts sending `RaftState` updates in form of `RaftStateWrapper` objects
     * to the web interface via Coaty. This happens whenever a new Raft state
     * change was detected by the `RaftController`.
     */
    private _startSendingRaftStateUpdates() {
        this._raftController
            .observeState()
            .forEach((stateUpdate: RaftState) => {
                this.communicationManager.publishAdvertise(
                    AdvertiseEvent.withObject(wrapRaftState(stateUpdate))
                );
            })
            .catch((err: any) => {
                this._logger("%s: %s", err.name, err.message);
            });
    }

    /**
     * Starts listening to Raft state requests by the web interface and answers
     * them by advertising the most recent Raft state. The web interface might
     * request a state update after losing the most recent Raft state (e.g.
     * through page reload).
     */
    private _startListeningToRaftStateRequests() {
        this.communicationManager
            .observeCall(OPERATION_NAME_REQUEST_RAFT_STATE)
            .subscribe(async (event) => {
                try {
                    const raftState = (await this._raftController.getState()) as RaftState;
                    this.communicationManager.publishAdvertise(
                        AdvertiseEvent.withObject(wrapRaftState(raftState))
                    );
                    event.returnEvent(ReturnEvent.withResult(true));
                } catch (err: any) {
                    this._logger("%s: %s", err.name, err.message);
                    event.returnEvent(ReturnEvent.withResult(false));
                }
            });
    }

    /**
     * Starts sending `RaftClusterConfiguration` updates in form of
     * `RaftClusterConfigurationWrapper` objects to the web interface via Coaty.
     * This happens whenever a new Raft cluster configuration change was
     * detected by the `RaftController`.
     */
    private _startSendingClusterConfigurationUpdates() {
        this._raftController
            .observeClusterConfiguration()
            .forEach((configUpdate: RaftClusterConfiguration) => {
                this.communicationManager.publishAdvertise(
                    AdvertiseEvent.withObject(wrapRaftClusterConfiguration(configUpdate))
                );
            })
            .catch((err: any) => {
                this._logger("%s: %s", err.name, err.message);
            });
    }

    /**
     * Starts listening to Raft cluster configuration requests by the web
     * interface and answers them by advertising the most recent Raft cluster
     * configuration. The web interface might request a cluster configuration
     * update after losing the most recent configuration (e.g. through page
     * reload).
     */
    private _startListeningToClusterConfigurationRequests() {
        this.communicationManager
            .observeCall(OPERATION_NAME_REQUEST_RAFT_CLUSTER_CONFIGURATION)
            .subscribe(async (event) => {
                try {
                    const clusterConfig =
                        (await this._raftController.getClusterConfiguration()) as RaftClusterConfiguration;
                    this.communicationManager.publishAdvertise(
                        AdvertiseEvent.withObject(wrapRaftClusterConfiguration(clusterConfig))
                    );
                    event.returnEvent(ReturnEvent.withResult(true));
                } catch (err) {
                    this._logger("%s: %s", err.name, err.message);
                    event.returnEvent(ReturnEvent.withResult(false));
                }
            });
    }

    private _shutdown() {
        this.container.shutdown();
        this._raftController.container.shutdown();
        deleteDBFile(this._id, this._logger);
        process.exit();
    }
}

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

/**
 * Code shared between raft agent and web interface. This file should be kept in
 * sync with the corresponding file inside the src folder of the raft agent.
 */

import { CoatyObject, Runtime } from "@coaty/core";

export const AGENT_INTERFACE_CONNECTION_NAMESPACE_PREFIX = "coaty.examples.raft.interface";

export const OBJECT_TYPE_CONNECTION_STATE = "coaty.examples.raft.ConnectionState";
export const OBJECT_TYPE_RAFT_STATE = "coaty.examples.raft.RaftState";
export const OBJECT_TYPE_RAFT_CLUSTER_CONFIGURATION = "coaty.examples.raft.RaftClusterConfiguration";

export const OPERATION_NAME_REQUEST_RAFT_STATE = "coaty.examples.raft.request-raft-state";
export const OPERATION_NAME_REQUEST_RAFT_CLUSTER_CONFIGURATION = "coaty.examples.raft.request-raft-cluster-configuration";
export const OPERATION_NAME_CONNECT = "coaty.examples.raft.connect";
export const OPERATION_NAME_SET = "coaty.examples.raft.set";
export const OPERATION_NAME_DELETE = "coaty.examples.raft.delete";
export const OPERATION_NAME_STOP = "coaty.examples.raft.stop";
export const OPERATION_NAME_DISCONNECT = "coaty.examples.raft.disconnect";

export const CONNECTION_STATE_SEND_INTERVAL = 400;
export const CONNECTION_STATE_LOST_ASSUMPTION_TIME = 1000;

/**
 * The raft agent sends `ConnectionState` updates in form of
 * `ConnectionStateWrapper` objects to the web interface via Coaty. This happens
 * in regular time intervals to detect on the web interface side if the agent
 * has crashed.
 *
 * (See `CONNECTION_STATE_SEND_INTERVAL`)
 */
export enum ConnectionState {
    AgentOffline = "Agent offline",
    DisconnectedFromRaft = "Disconnected from Raft",
    ConnectingToRaft = "Connecting to Raft",
    ConnectedToRaft = "Connected to Raft",
    Stopping = "Stopping",
    DisconnectingFromRaft = "Disconnecting from Raft",
}
export interface ConnectionStateWrapper extends CoatyObject {
    connectionState: ConnectionState;
}
export function wrapConnectionState(connectionState: ConnectionState): ConnectionStateWrapper {
    return {
        objectType: OBJECT_TYPE_CONNECTION_STATE,
        coreType: "CoatyObject",
        objectId: Runtime.newUuid(),
        name: "CoatyConnectionStateWrapper",
        connectionState: connectionState,
    };
}

/**
 * The Raft agent sends `RaftState` updates in form of `RaftStateWrapper`
 * objects to the web interface via Coaty. This happens whenever a new Raft
 * state change was detected by it's `RaftController` or the web interface
 * requested a Raft state update.
 */
export type RaftState = [string, string][];
export interface RaftStateWrapper extends CoatyObject {
    raftState: RaftState;
}
export function wrapRaftState(state: RaftState): RaftStateWrapper {
    return {
        objectType: OBJECT_TYPE_RAFT_STATE,
        coreType: "CoatyObject",
        objectId: Runtime.newUuid(),
        name: "CoatyRaftStateWrapper",
        raftState: state,
    };
}

/**
 * The Raft agent sends `RaftClusterConfiguration` updates in form of
 * `RaftClusterConfigurationWrapper` objects to the web interface via Coaty.
 * This happens whenever a new cluster configuration change was detected by it's
 * `RaftController` or the web interface requested a cluster configuration
 * update.
 */
export type RaftClusterConfiguration = string[];
export interface RaftClusterConfigurationWrapper extends CoatyObject {
    raftClusterConfiguration: RaftClusterConfiguration;
}
export function wrapRaftClusterConfiguration(
    clusterConfig: RaftClusterConfiguration
): RaftClusterConfigurationWrapper {
    return {
        objectType: OBJECT_TYPE_RAFT_CLUSTER_CONFIGURATION,
        coreType: "CoatyObject",
        objectId: Runtime.newUuid(),
        name: "CoatyRaftClusterConfigurationWrapper",
        raftClusterConfiguration: clusterConfig,
    };
}

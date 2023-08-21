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

import { CommunicationState, Components, Configuration, Container } from "@coaty/core";
import { filter, take, timeout } from "rxjs/operators";
import fse from "fs-extra";
import os from "os";
import path from "path";

import { RaftController, RaftControllerOptions } from "../../non-ported/raft-controller";
import { RaftData } from "../../non-ported/raft-controller";
import { RaftStateMachine } from "../../non-ported/raft-node-interfaces/state-machine";

export class NumberInputStateMachine implements RaftStateMachine {
    private _state: number[] = [];

    processInput(input: RaftData) {
        this._state.push(input);
    }

    getState(): RaftData {
        return this._state;
    }

    setState(state: RaftData): void {
        this._state = state;
    }
}

export class KVRaftStateMachine implements RaftStateMachine {
    private _state = new Map<string, string>();

    processInput(input: RaftData) {
        if (input.type === "set") {
            // Add or update key value pair
            this._state.set(input.key, input.value);
        } else if (input.type === "delete") {
            // Delete key value pair
            this._state.delete(input.key);
        }
    }

    getState(): RaftData {
        // Convert from Map to JSON compatible object
        return Array.from(this._state);
    }

    setState(state: RaftData): void {
        // Convert from JSON compatible object to Map
        this._state = new Map(state);
    }
}

// Use an input object of this type to set or update a key value pair
type KVSet = { type: "set"; key: string; value: string };

export function SetInput(key: string, value: string): KVSet {
    return { type: "set", key: key, value: value };
}

// Use an input object of this type to delete a key value pair
type KVDelete = { type: "delete"; key: string };

export function DeleteInput(key: string): KVDelete {
    return { type: "delete", key: key };
}

export function startController(
    id: string,
    stateMachine: RaftStateMachine,
    shouldCreateCluster: boolean = false,
    cluster: string = ""
): RaftController {
    // Define RaftControllerOptions
    const opts: RaftControllerOptions = {
        id: id,
        cluster: cluster,
        stateMachine: stateMachine,
        shouldCreateCluster: shouldCreateCluster,
    };

    // Create Components and Configuration
    const components: Components = { controllers: { RaftController } };
    const configuration: Configuration = {
        common: { agentIdentity: { name: `raft.testAgent-${id}` } },
        communication: {
            brokerUrl: "mqtt://localhost:1888",
            shouldAutoStart: true,
        },
        controllers: {
            RaftController: opts,
        },
        databases: {
            raftdb: {
                adapter: "SqLiteNodeAdapter",
                connectionString: getTestConnectionString(id),
            },
        },
    };

    // Bootstrap RaftController
    const container = Container.resolve(components, configuration);
    return container.getController<RaftController>("RaftController");
}

export function getTestConnectionString(id: string) {
    return path.join(os.tmpdir(), `consensus-raft-test-${id}.db`);
}

/**
 * Delete Raft test database for a given Raft node id.
 */
export function cleanupTestDatabase(id: string) {
    try {
        fse.unlinkSync(getTestConnectionString(id));
    } catch (err) {
        console.error("Error while deleting test database", err);
    }
}

export function testConnectionToBroker(controller: RaftController): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
        controller.communicationManager
            .observeCommunicationState()
            .pipe(
                filter((state) => state === CommunicationState.Online),
                take(1),
                timeout(3000)
            )
            .subscribe(
                (_) => resolve(true),
                (_) => resolve(false)
            );
    });
}

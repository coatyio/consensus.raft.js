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

import { RaftData, RaftStateMachine } from "@coaty/consensus.raft";

/**
 * `RaftStateMachine` implementation of a key value store. Can process `KVSet`
 * and `KVDelete` inputs. All other inputs will be ignored.
 */
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

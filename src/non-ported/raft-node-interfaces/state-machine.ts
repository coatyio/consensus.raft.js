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

import { RaftController, RaftData } from "../raft-controller";

/**
 * Users of the {@linkcode RaftController} have to implement a
 * `RaftStateMachine` and provide an instance of it in the
 * {@linkcode RaftControllerOptions} when bootstrapping the controller. The
 * provided state machine will be used internally to describe the state shared
 * between all agents with corresponding `RaftController`s.
 *
 * A call to {@linkcode RaftController.propose | RaftController.propose()} will
 * trigger `this.processInput()` after the input has been committed. After that
 * the state is retrieved with `this.getState()` and then used to resolve the
 * promise returned by the initial `RaftController.propose()` call.
 *
 * A typical `RaftStateMachine` implementation keeps track of some form of state
 * (e.g. a variable of type `Map`) and defines how inputs affect this state
 * inside `processInput()`. `getState()` and `setState()` are used to access the
 * tracked state.
 *
 * An example `RaftStateMachine` implementation of a key value store looks like
 * this:
 *
 * ```typescript
 * class KVStateMachine implements RaftStateMachine {
 *    private _state = new Map<string, string>();
 *
 *    processInput(input: RaftData) {
 *        if (input.type === "set") {
 *            // Add or update key value pair
 *            this._state.set(input.key, input.value);
 *        } else if (input.type === "delete") {
 *            // Delete key value pair
 *            this._state.delete(input.key);
 *        }
 *    }
 *
 *    getState(): RaftData {
 *        // Convert from Map to JSON compatible object
 *        return Array.from(this._state);
 *    }
 *
 *    setState(state: RaftData): void {
 *        // Convert from JSON compatible object to Map
 *        this._state = new Map(state);
 *    }
 * }
 *
 * // Propose an input object of this type to set or update a key value pair
 * type KVSet = { type: "set"; key: string; value: string };
 *
 * // Propose an input object of this type to delete a key value pair
 * type KVDelete = { type: "delete"; key: string };
 * ```
 *
 * @category Raft Interfaces
 */
export interface RaftStateMachine {
    /**
     * Defines how inputs proposed with
     * {@linkcode RaftController.propose | RaftController.propose()} affect this
     * `RaftStateMachine`s internal state after they were committed.
     *
     * @param input The input that should be processed. Is of type
     * {@linkcode RaftData}.
     */
    processInput(input: RaftData): void;

    /**
     * Gets this `RaftStateMachine`s internal state. This function should
     * convert the state tracked inside this state machine (e.g. with one or
     * multiple variables) into {@linkcode RaftData} and return it. The returned
     * state is used to resolve the promise returned by
     * {@linkcode RaftController.propose | RaftController.propose()} and to
     * provide the values for
     * {@linkcode RaftController.getState | RaftController.getState()} and
     * {@linkcode RaftController.observeState | RaftController.observeState()}.
     *
     * @remark `getState()` and `setState()` are counterparts. Setting the state
     * of one state machine instance to the state of another one
     * (`SM1.setState(SM2.getState())`) should result in both state machines
     * representing the exact same state and behaving exactly the same regarding
     * new state machine inputs.
     *
     * @returns This `RaftStateMachine`'s internal state. Must be of type
     * {@linkcode RaftData}.
     */
    getState(): RaftData;

    /**
     * Sets this `RaftStateMachine`s internal state. This function should use
     * the provided `state` to set the state tracked inside this state machine.
     * The structure of the provided state is implicitly defined inside
     * `getState()`. See remarks of `getState()`.
     *
     * @param state The state this state machine should be set to. Is of type
     * {@linkcode RaftData}.
     */
    setState(state: RaftData): void;
}

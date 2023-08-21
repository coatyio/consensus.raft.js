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

import { RaftPersistency } from "./persistency";
import { Entry, HardState, Snapshot } from "../../ported/raftpb/raft.pb";
import { MemoryStorage } from "../../ported/storage";
import { RaftConfiguration } from "../raft-controller";

/**
 * Wrapper for the `RaftPersistency` interface. Is used inside `RaftNode` to
 * access persistent memory. This additional layer on top of `RaftPersistency`
 * provides better usability to `RaftNode` while keeping the `RaftPersistency`
 * interface simple.
 *
 * - Persists Raft entries, hardstate and snapshot
 * - Can restore the state of a node after a crash
 */
export class RaftPersistencyWrapper {
    private readonly _wrapped: RaftPersistency;
    private readonly _id: string;
    private readonly _cluster: string;
    private readonly _raftConfig: Required<RaftConfiguration>;

    constructor(wrapped: RaftPersistency, id: string, cluster: string, raftConfig: Required<RaftConfiguration>) {
        this._wrapped = wrapped;
        this._id = id;
        this._cluster = cluster;
        this._raftConfig = raftConfig;
    }

    /**
     * Checks if persistent memory contains any persisted entries, hardstate or
     * snapshot for the Raft node associated with this wrapper.
     *
     * @returns A promise resolving to true if persistent data was found/false
     * if no data was found. Is rejected if an error occurred while accessing
     * persistent memory.
     */
    shouldRestartFromPersistedState(): Promise<boolean> {
        return new Promise<boolean>(async (resolve, reject) => {
            try {
                // Check for persisted entries
                const entries = await this._getPersistedEntries();
                if (entries !== null) {
                    resolve(true);
                }

                // Check for persisted hardstate
                const hardState = await this._getPersistedHardState();
                if (hardState !== null) {
                    resolve(true);
                }

                // Check for persisted snapshot
                const snapshot = await this._getPersistedSnapshot();
                if (snapshot !== null) {
                    resolve(true);
                }
                resolve(false);
            } catch (error) {
                // Error occurred while accessing persistent memory
                reject(error);
            }
        });
    }

    /**
     * Initializes an in memory implementation of the `Storage` interface with
     * entries, hardstate and snapshot from persistent memory of the Raft node
     * associated with this wrapper.
     *
     * @returns A promise resolving to the initialized `Storage` implementation.
     * Is rejected if an error occurred while accessing persistent memory or if
     * persistent memory doesn't contain any persisted data.
     */
    getPersistedStorage(): Promise<MemoryStorage> {
        return new Promise<MemoryStorage>(async (resolve, reject) => {
            try {
                // Load all persisted data into memory
                const entries = await this._getPersistedEntries();
                const hardState = await this._getPersistedHardState();
                const snapshot = await this._getPersistedSnapshot();

                // Reject if no persisted data was found
                if (entries === null && hardState === null && snapshot === null) {
                    reject("Could not find any persisted data");
                }

                // Initialize and setup storage with loaded data and resolve
                const storage = MemoryStorage.NewMemoryStorage();
                if (snapshot !== null) {
                    storage.ApplySnapshot(snapshot);
                }
                if (hardState !== null) {
                    storage.SetHardState(hardState);
                }
                if (entries !== null) {
                    storage.Append(entries);
                }
                resolve(storage);
            } catch (error) {
                // Error occurred while accessing persistent memory
                reject(error);
            }
        });
    }

    /**
     * Writes the provided entries, hardstate and snapshot of the Raft node
     * associated with this wrapper to persistent memory. Entries are merged,
     * hardstate and snapshot get overwritten if the provided values are not
     * `null`. Entries that are no longer needed because a snapshot with higher
     * index has been persisted are automatically deleted from persistent
     * memory.
     *
     * @remark The following order is used when writing to persistent memory:
     * Entries (1), Hardstate (2), Snapshot (3)
     *
     * @param entries The entries that will be merged into the existing
     * persisted entries. Provide `null` to avoid changing the existing
     * persisted entries.
     *
     * @param hardstate The hardstate that will be written to persistent memory.
     * Provide `null` or an empty hardstate to avoid overwriting the existing
     * persisted hardstate.
     *
     * @param snapshot The snapshot that will be written to persistent memory.
     * Provide `null` or an empty snapshot to avoid overwriting the existing
     * persisted hardstate.
     *
     * @returns Promise that is resolved after everything was persisted or
     * rejected if an error occurred while accessing persistent memory.
     */
    persist(
        entries: Entry[] | null,
        hardstate: HardState | null,
        snapshot: Snapshot | null
    ): Promise<void> {
        return new Promise<void>(async (resolve, reject) => {
            try {
                // Persist entries
                if (entries !== null && entries.length > 0) {
                    const oldEntries: Entry[] | null = await this._getPersistedEntries();
                    if (oldEntries !== null) {
                        // When writing an Entry with Index i, any previously-
                        // persisted entries with Index >= i must be discarded
                        const i = Math.min(...entries.map((entry) => entry.index));
                        const remainingOldEntries = oldEntries.filter(
                            (entry) => !(entry.index >= i)
                        );
                        const newEntries = [...remainingOldEntries, ...entries];
                        await this._persistEntries(newEntries);
                    } else {
                        await this._persistEntries(entries);
                    }
                }
                // Persist hardstate
                if (hardstate !== null && !hardstate.isEmptyHardState()) {
                    await this._persistHardState(hardstate);
                }
                // Persist snapshot
                if (snapshot !== null && !snapshot.isEmptySnap()) {
                    await this._persistSnapshot(snapshot);
                    // Cleanup entries by deleting entries with index <=
                    // snapshot.index
                    const oldEntries: Entry[] | null = await this._getPersistedEntries();
                    if (oldEntries !== null) {
                        const updatedEntries = oldEntries.filter((entry) => {
                            return entry.index > snapshot.metadata.index;
                        });
                        if (updatedEntries.length !== oldEntries.length) {
                            await this._persistEntries(updatedEntries);
                        }
                    }
                }
                // Resolve after everything has been persisted
                resolve();
            } catch (error) {
                // Error occurred while accessing persistent memory
                reject(error);
            }
        });
    }

    /**
     * Closes the connection to persistent memory for the Raft node associated
     * with this wrapper. This function can be used to close potential file
     * locks before the associated Raft node shuts down. If `eraseAllData` is
     * set to true, all persisted data is erased from persistent memory before
     * closing the connection.
     *
     * @param eraseAllData Specifies whether or not to erase all persisted data.
     *
     * @returns Promise that is resolved once the data has been erased (if
     * specified) and the connection has been closed. Is rejected if an error
     * occurred while accessing persistent memory.
     */
    close(eraseAllData: boolean): Promise<void> {
        return this._wrapped.close(eraseAllData, this._id, this._cluster);
    }

    private async _getPersistedEntries(): Promise<Entry[] | null> {
        const deserializedEntries = await this._wrapped.getPersistedData(this._raftConfig.raftEntriesKey, this._id, this._cluster);
        return deserializedEntries === null ? null : deserializedEntries.map((entry: any) => new Entry(entry));
    }

    private async _getPersistedHardState(): Promise<HardState | null> {
        const deserializedHardstate = await this._wrapped.getPersistedData(this._raftConfig.raftHardstateKey, this._id, this._cluster);
        return deserializedHardstate === null ? null : new HardState(deserializedHardstate);
    }

    private async _getPersistedSnapshot(): Promise<Snapshot | null> {
        const deserializedSnapshot = await this._wrapped.getPersistedData(this._raftConfig.raftSnapshotKey, this._id, this._cluster);
        return deserializedSnapshot === null ? null : new Snapshot(deserializedSnapshot);
    }

    private async _persistEntries(entries: Entry[]): Promise<void> {
        await this._wrapped.persistData(entries, this._raftConfig.raftEntriesKey, this._id, this._cluster)
    }

    private async _persistHardState(hardstate: HardState): Promise<void> {
        await this._wrapped.persistData(hardstate, this._raftConfig.raftHardstateKey, this._id, this._cluster);
    }

    private async _persistSnapshot(snapshot: Snapshot): Promise<void> {
        await this._wrapped.persistData(snapshot, this._raftConfig.raftSnapshotKey, this._id, this._cluster);
    }
}

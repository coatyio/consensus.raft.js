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

import { DbLocalContext } from "@coaty/core/db";

import { RaftData } from "../raft-controller";

/**
 * Interface used by Raft to persist data and retrieve it if necessary (e.g.
 * after a crash).
 *
 * @remark Users can overwrite
 * {@linkcode RaftController.getRaftPersistencyImplementation} to provide their
 * own implementation of this interface if needed.
 *
 * @category Raft Interfaces
 */
export interface RaftPersistency {
    /**
     * Persists the provided `data` for the given `key` for the Raft node
     * specified by `id` in the Raft cluster specified by `cluster`. Existing
     * persisted data is overwritten.
     *
     * @param key Can be used to retrieve the persisted data afterwards.
     *
     * @param data The data that should be persisted. Is of type
     * {@linkcode RaftData}.
     *
     * @param id Specifies the node for which data should be persisted.
     *
     * @param cluster Specifies the cluster of the node for which data should be
     * persisted.
     *
     * @returns Promise that is resolved after the data was persisted or
     * rejected if an error occurred while accessing persistent memory.
     */
    persistData(data: RaftData, key: string, id: string, cluster: string): Promise<void>;

    /**
     * Returns the data that is currently persisted for the given `key` for the
     * Raft node specified by `id` in the Raft cluster specified by `cluster`.
     * That is the data that last was persisted using `persistData()`.
     *
     * @param key Specifies the data that will be returned.
     *
     * @param id Specifies the node for which data should be returned.
     *
     * @param cluster Specifies the cluster of the node for which data should be
     * returned.
     *
     * @returns Promise containing the currently persisted data for the given
     * key or `null` if no such data was found. Is rejected if an error occurred
     * while accessing persistent memory.
     */
    getPersistedData(key: string, id: string, cluster: string): Promise<RaftData | null>;

    /**
     * Closes the connection to persistent memory for the Raft node specified by
     * `id` in the Raft cluster specified by `cluster`. This function can be
     * used to release potential file locks before this Raft node shuts down. If
     * `eraseAllData` is set to true, all persisted data is erased from
     * persistent memory before closing the connection.
     *
     * @param eraseAllData Specifies whether or not to erase all persisted data.
     *
     * @param id Specifies the node for which the connection to persistent
     * memory should be closed.
     *
     * @param cluster Specifies the cluster of the node for which the connection
     * to persistent memory should be closed.
     *
     * @returns Promise that is resolved once the data has been erased (if
     * specified) and the connection has been closed. Is rejected if an error
     * occurred while accessing persistent memory.
     */
    close(eraseAllData: boolean, id: string, cluster: string): Promise<void>;
}

/**
 * Implementation of the `RaftPersistency` interface. Uses the
 * `SqLiteNodeAdapter` from the Coaty Unified Storage API.
 */
export class RaftPersistencyStore implements RaftPersistency {
    private _context: DbLocalContext;

    constructor(context: DbLocalContext) {
        this._context = context;
    }

    persistData(data: RaftData, key: string, id: string, cluster: string): Promise<void> {
        const storeId = this._createIdWithCluster(id, cluster);
        return (
            this._context
                // Create new store for Raft persistency if necessary
                .addStore(storeId)

                // Persist value for specified key
                .then(() => this._context.setValue(key, data, storeId))
        );
    }

    getPersistedData(key: string, id: string, cluster: string): Promise<RaftData | null> {
        const storeId = this._createIdWithCluster(id, cluster);
        return (
            this._context
                // Create new store for Raft persistency if necessary
                .addStore(storeId)

                // Retrieve value for specified key
                .then(() => this._context.getValue(key, storeId))
                .then(value => value ?? null)
        );
    }

    async close(eraseAllData: boolean, id: string, cluster: string): Promise<void> {
        if (eraseAllData) {
            await this._context.removeStore(this._createIdWithCluster(id, cluster));
        }
        return this._context.close();
    }

    private _createIdWithCluster(id: string, cluster: string) {
        const maxPrefix = Number.MAX_SAFE_INTEGER.toString(36).length;
        const prefix = id.length.toString(36).padStart(maxPrefix, "0");
        return prefix + id + cluster;
    }
}

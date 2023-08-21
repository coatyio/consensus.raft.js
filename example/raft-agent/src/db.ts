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

import Debug from "debug";
import fse from "fs-extra";

/**
 * Returns the path to the db file that should be used by the agent with the
 * provided `id`.
 * @param id Specifies the agent whose db file path will be returned.
 */
export function getDBFilePath(id: string): string {
    return `db/example-${id}.db`;
}

/**
 * Deletes the db file used by the agent specified with `id`. This function is
 * called after an agent has disconnected from the cluster.
 * @param id Specifies the agent whose db file will be deleted.
 */
export function deleteDBFile(id: string, logger: Debug.Debugger) {
    const filePath = getDBFilePath(id);
    try {
        fse.unlinkSync(filePath);
    } catch (err) {
        logger("Error while trying to delete file %s: %s", filePath, err);
    }
}

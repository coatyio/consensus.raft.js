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

import { Configuration, Container } from "@coaty/core";
import { DbLocalContext } from "@coaty/core/db";
import { SqLiteNodeAdapter } from "@coaty/core/db/adapter-sqlite-node";
import tap from "tap";

import { RaftPersistencyStore } from "../../../non-ported/raft-node-interfaces/persistency";
import { Entry, HardState, Snapshot, SnapshotMetadata } from "../../../ported/raftpb/raft.pb";
import { cleanupTestDatabase, getTestConnectionString } from "../util";

tap.test("raft-persistency RaftPersistencyStore", async (t) => {
    const raftNodeTestId = "testNodeId";
    const [dbContext, container] = getDbContext(raftNodeTestId);
    t.teardown(async () => {
        container.shutdown();
        await new Promise(res => setTimeout(res, 1000));
    });
    const toBeTested = new RaftPersistencyStore(dbContext);

    const entries: Entry[] = [
        new Entry({ term: 1, index: 1, data: "this" }),
        new Entry({ term: 1, index: 2, data: "is" }),
        new Entry({ term: 2, index: 3, data: "a" }),
        new Entry({ term: 3, index: 4, data: "test" }),
    ];
    const hardstate1 = new HardState({
        term: 2,
        vote: "40",
        commit: 3,
    });
    const hardstate2 = new HardState({
        term: 3,
        vote: "42",
        commit: 4,
    });
    const snapshot = new Snapshot({
        data: "snap data",
        metadata: new SnapshotMetadata({
            index: 3,
            term: 2,
        }),
    });
    await toBeTested.persistData(entries, "e", raftNodeTestId, "");
    await toBeTested.persistData(hardstate1, "h", raftNodeTestId, "");
    await toBeTested.persistData(hardstate2, "h", raftNodeTestId, "");
    await toBeTested.persistData(snapshot, "s", raftNodeTestId, "");
    const resultingEntries = await toBeTested.getPersistedData("e", raftNodeTestId, "");
    const resultingHardstate = await toBeTested.getPersistedData("h", raftNodeTestId, "");
    const resultingSnapshot = await toBeTested.getPersistedData("s", raftNodeTestId, "");
    t.strictSame(resultingEntries, JSON.parse(JSON.stringify(entries)));
    t.strictSame(resultingHardstate, JSON.parse(JSON.stringify(hardstate2)));
    t.strictSame(resultingSnapshot, JSON.parse(JSON.stringify(snapshot)));
    await toBeTested.close(true, raftNodeTestId, "");
    const resultingEntries2 = await toBeTested.getPersistedData("e", raftNodeTestId, "");
    const resultingHardstate2 = await toBeTested.getPersistedData("h", raftNodeTestId, "");
    const resultingSnapshot2 = await toBeTested.getPersistedData("s", raftNodeTestId, "");
    t.strictSame(resultingEntries2, null);
    t.strictSame(resultingHardstate2, null);
    t.strictSame(resultingSnapshot2, null);
    await toBeTested.close(true, raftNodeTestId, "");

    cleanupTestDatabase(raftNodeTestId);

    t.end();
});

function getDbContext(id: string): [DbLocalContext, Container] {
    const configuration: Configuration = {
        common: {
            agentIdentity: { name: "test" },
        },
        communication: {
            brokerUrl: "mqtt://localhost:1888",
            shouldAutoStart: true,
        },
        databases: {
            raftdb: {
                adapter: "SqLiteNodeAdapter",
                connectionString: getTestConnectionString(id),
            },
        },
    };
    const container = Container.resolve({}, configuration);
    const connectionInfo = container.runtime.databaseOptions["raftdb"];
    return [new DbLocalContext(connectionInfo, SqLiteNodeAdapter), container];
}

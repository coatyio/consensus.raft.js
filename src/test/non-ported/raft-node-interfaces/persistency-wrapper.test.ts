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

import tap from "tap";

import { HardState } from "./../../../ported/raftpb/raft.pb";
import { RaftPersistency } from "../../../non-ported/raft-node-interfaces/persistency";
import { RaftPersistencyWrapper } from "../../../non-ported/raft-node-interfaces/persistency-wrapper";
import { noLimit } from "../../../ported/raft";
import { Entry, Snapshot, SnapshotMetadata } from "../../../ported/raftpb/raft.pb";
import { RaftConfiguration } from "../../../non-ported/raft-controller";

const raftConfig: Required<RaftConfiguration> = {
    electionTick: 10,
    heartbeatTick: 1,
    maxSizePerMsg: 1024 * 1024,
    maxCommittedSizePerReady: 1000,
    maxUncommittedEntriesSize: Math.pow(2, 30),
    maxInflightMsgs: 256,
    tickTime: 100,
    defaultSnapCount: 1000,
    snapshotCatchUpEntriesN: 50,
    inputReproposeInterval: 1000,
    confChangeReproposeInterval: 1000,
    queuedUpInputProposalsLimit: 1000,
    runningJoinRequestsLimit: 1000,
    raftEntriesKey: "entries",
    raftHardstateKey: "hardstate",
    raftSnapshotKey: "snapshot",
    snapshotReceiveTimeout: 60000,
    joinRequestRetryInterval: 5000
};

class RaftPersistencyMock implements RaftPersistency {

    // Indexed by id, then key.
    private _persistentMemory = new Map<string, Map<string, any>>();

    persistData(data: any, key: string, id: string, cluster: string): Promise<void> {
        if (cluster !== "test") {
            // Tests for wrapper only require one "test" cluster
            throw new Error("Wrong cluster specified.");
        }
        let keyStore = this._persistentMemory.get(id);
        if (keyStore === undefined) {
            keyStore = new Map();
            this._persistentMemory.set(id, keyStore);
        }
        keyStore.set(key, data);
        return Promise.resolve();
    }

    getPersistedData(key: string, id: string, cluster: string): Promise<any | null> {
        if (cluster !== "test") {
            // Tests for wrapper only require one "test" cluster
            throw new Error("Wrong cluster specified.");
        }
        return Promise.resolve(this._persistentMemory.get(id)?.get(key) ?? null);
    }

    close(eraseAllData: boolean, id: string, cluster: string): Promise<void> {
        if (cluster !== "test") {
            // Tests for wrapper only require one "test" cluster
            throw new Error("Wrong cluster specified.");
        }
        if (eraseAllData) {
            this._persistentMemory.delete(id);
        }
        return Promise.resolve();
    }

    // For testing
    getPersistedEntries(id: string): any | null {
        // key is RAFT_ENTRIES_KEY from persistency-wrapper class
        return this._persistentMemory.get(id)?.get("entries") ?? null;
    }

    getPersistedHardState(id: string): any | null {
        // key is RAFT_HARDSTATE_KEY from persistency-wrapper class
        return this._persistentMemory.get(id)?.get("hardstate") ?? null;
    }

    getPersistedSnapshot(id: string): any | null {
        // key is RAFT_SNAPSHOT_KEY from persistency-wrapper class
        return this._persistentMemory.get(id)?.get("snapshot") ?? null;
    }
}

tap.test("persistency-wrapper shouldRestartFromPersistedState()", async (t) => {
    const raftPersistencyImpl = new RaftPersistencyMock();
    const wrapper = new RaftPersistencyWrapper(raftPersistencyImpl, "1", "test", raftConfig);
    const entries = [
        new Entry({ term: 2, index: 2 }),
        new Entry({ term: 2, index: 3 }),
        new Entry({ term: 2, index: 4 }),
    ];
    const hardstate = new HardState({
        term: 2,
    });
    const snapshot = new Snapshot({
        data: { x: "snap data" },
        metadata: new SnapshotMetadata({
            index: 1,
            term: 1,
        }),
    });

    // Empty
    t.equal(await wrapper.shouldRestartFromPersistedState(), false);

    // Filled with entries only
    await wrapper.persist(entries, null, null);
    t.equal(await wrapper.shouldRestartFromPersistedState(), true);
    await wrapper.close(true);

    // Filled with hardstate only
    await wrapper.persist(null, hardstate, null);
    t.equal(await wrapper.shouldRestartFromPersistedState(), true);
    await wrapper.close(true);

    // Filled with snapshot only
    await wrapper.persist(null, null, snapshot);
    t.equal(await wrapper.shouldRestartFromPersistedState(), true);
});

tap.test("persistency-wrapper getPersistedStorage()", async (t) => {
    const wrapped = new RaftPersistencyMock();
    const toBeTested = new RaftPersistencyWrapper(wrapped, "1", "test", raftConfig);
    const entries = [
        new Entry({ term: 2, index: 2 }),
        new Entry({ term: 2, index: 3 }),
        new Entry({ term: 2, index: 4 }),
    ];
    const hardstate = new HardState({
        term: 2,
    });
    const snapshot = new Snapshot({
        data: { x: "snap data" },
        metadata: new SnapshotMetadata({
            index: 1,
            term: 1,
        }),
    });

    // Empty persistent storage => reject
    t.rejects(toBeTested.getPersistedStorage());

    // Normal case after crash
    await toBeTested.persist(entries, hardstate, snapshot);
    const storage = await toBeTested.getPersistedStorage();
    t.strictSame(storage.Entries(2, 5, noLimit)[0], entries);
    t.strictSame(storage.hardState, hardstate);
    t.strictSame(storage.snapshot, snapshot);
});

tap.test("persistency-wrapper persist()", async (t) => {
    const raftNodeTestId = "1";
    const wrapped = new RaftPersistencyMock();
    const toBeTested = new RaftPersistencyWrapper(wrapped, raftNodeTestId, "test", raftConfig);
    const entries = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 1, index: 2 }),
        new Entry({ term: 2, index: 3 }),
    ];
    const entriesAfterSnap = [new Entry({ term: 2, index: 3 })];
    const entries2 = [new Entry({ term: 2, index: 2 }), new Entry({ term: 3, index: 3 })];
    const expected1 = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 2, index: 2 }),
        new Entry({ term: 3, index: 3 }),
    ];
    const entries3 = [new Entry({ term: 4, index: 3 }), new Entry({ term: 4, index: 4 })];
    const expected2 = [
        new Entry({ term: 1, index: 1 }),
        new Entry({ term: 2, index: 2 }),
        new Entry({ term: 4, index: 3 }),
        new Entry({ term: 4, index: 4 }),
    ];
    const snapshot = new Snapshot({
        data: { x: "snap data" },
        metadata: new SnapshotMetadata({
            index: 2,
            term: 1,
        }),
    });

    // Normal case
    await toBeTested.persist(entries, null, snapshot);
    t.strictSame(wrapped.getPersistedEntries(raftNodeTestId), entriesAfterSnap);
    t.strictSame(wrapped.getPersistedHardState(raftNodeTestId), null);
    t.strictSame(wrapped.getPersistedSnapshot(raftNodeTestId), snapshot);
    await toBeTested.close(true);

    // Empty entries array shouldn't overwrite old entries
    await toBeTested.persist(entries, null, null);
    await toBeTested.persist([], null, null);
    t.strictSame(wrapped.getPersistedEntries(raftNodeTestId), entries);
    await toBeTested.close(true);

    // Check if following requirement holds: When writing an Entry with Index i,
    // any previously-persisted entries with Index >= i must be discarded
    await toBeTested.persist(entries, null, null);
    await toBeTested.persist(entries2, null, null);
    t.strictSame(wrapped.getPersistedEntries(raftNodeTestId), expected1);
    await toBeTested.persist(entries3, null, null);
    t.strictSame(wrapped.getPersistedEntries(raftNodeTestId), expected2);
});

tap.test("persistency-wrapper close()", async (t) => {
    const wrapped = new RaftPersistencyMock();
    const toBeTested = new RaftPersistencyWrapper(wrapped, "1", "test", raftConfig);
    const entries = [
        new Entry({ term: 2, index: 2 }),
        new Entry({ term: 3, index: 3 }),
        new Entry({ term: 3, index: 4 }),
    ];
    const snapshot = new Snapshot({
        data: { x: "snap data" },
        metadata: new SnapshotMetadata({
            index: 1,
            term: 1,
        }),
    });

    await toBeTested.persist(entries, null, snapshot);
    await toBeTested.getPersistedStorage();
    await toBeTested.close(true);
    t.rejects(toBeTested.getPersistedStorage());
});

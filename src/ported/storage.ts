// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This is a port of Raft – the original work is copyright by "The etcd Authors"
// and licensed under Apache-2.0 similar to the license of this file.

import { getLogger } from "./logger";
import { ConfState, Entry, HardState, Snapshot } from "./raftpb/raft.pb";
import { limitSize } from "./util";
import { NullableError } from "./util";
import { RaftData } from "../non-ported/raft-controller";

/**
 * ErrCompacted is returned by Storage.Entries/Compact when a requested index is
 * unavailable because it predates the last snapshot.
 */
export class ErrCompacted extends Error {
    constructor() {
        super("requested index is unavailable due to compaction");
        this.name = "ErrCompacted";
    }
}

/**
 * ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested index
 * is older than the existing snapshot.
 */
export class ErrSnapOutOfDate extends Error {
    constructor() {
        super("requested index is older than the existing snapshot");
        this.name = "ErrSnapOutOfDate";
    }
}

/**
 * ErrUnavailable is returned by Storage interface when the requested log
 * entries are unavailable.
 */
export class ErrUnavailable extends Error {
    constructor() {
        super("requested entry at index is unavailable");
        this.name = "ErrUnavailable";
    }
}

/**
 * ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when
 * the required snapshot is temporarily unavailable.
 */
export class ErrSnapshotTemporarilyUnavailable extends Error {
    constructor() {
        super("snapshot is temporarily unavailable");
        this.name = "ErrSnapshotTemporarilyUnavailable";
    }
}

/**
 * Storage is an interface that may be implemented by the application to
 * retrieve log entries from storage.
 *
 * If any Storage method returns an error, the raft instance will become
 * inoperable and refuse to participate in elections; the application is
 * responsible for cleanup and recovery in this case.
 */
export interface Storage {
    // RaftGo-TODO(tbg): split this into two interfaces, LogStorage and
    // StateStorage.

    /**
     * InitialState returns the saved HardState and ConfState information.
     */
    InitialState(): [HardState, ConfState, NullableError];

    /**
     * Entries returns a slice of log entries in the range [lo,hi). MaxSize
     * limits the total size of the log entries returned, but Entries returns at
     * least one entry if any.
     * @param lo lower bound of the range, included
     * @param hi upper bound of the range, excluded
     * @param maxSize Limits the total size of the log entries returned
     */
    Entries(lo: number, hi: number, maxSize: number): [Entry[], NullableError];

    /**
     * Term returns the term of entry i, which must be in the range
     * [FirstIndex()-1, LastIndex()]. The term of the entry before FirstIndex is
     * retained for matching purposes even though the rest of that entry may not
     * be available.
     * @param i the index of the entry to get the term of
     */
    Term(i: number): [number, NullableError];

    /**
     * LastIndex returns the index of the last entry in the log.
     */
    LastIndex(): [number, NullableError];

    /**
     * FirstIndex returns the index of the first log entry that is possibly
     * available via Entries (older entries have been incorporated into the
     * latest Snapshot; if storage only contains the dummy entry the first log
     * entry is not available).
     */
    FirstIndex(): [number, NullableError];

    /**
     * Snapshot returns the most recent snapshot. If snapshot is temporarily
     * unavailable, it should return ErrSnapshotTemporarilyUnavailable, so raft
     * state machine could know that Storage needs some time to prepare snapshot
     * and call Snapshot later.
     */
    Snapshot(): [Snapshot, NullableError];
}

/**
 * MemoryStorage implements the Storage interface backed by an in-memory array.
 */
export class MemoryStorage implements Storage {
    // sync.Mutex is not included here, since in NodeJS there is no need to
    // synchronize threads

    hardState: HardState;
    snapshot: Snapshot;

    /**
     * ents[i] has raft log position i+snapshot.Metadata.Index
     */
    ents: Entry[];

    constructor(
        param: {
            hardState?: HardState;
            snapshot?: Snapshot;
            ents?: Entry[];
        } = {}
    ) {
        this.hardState = param?.hardState ?? new HardState();
        this.snapshot = param?.snapshot ?? new Snapshot();
        this.ents = param?.ents ?? [];
    }

    static NewMemoryStorage(): MemoryStorage {
        return new MemoryStorage({
            // When starting from scratch populate the list with a dummy entry
            // at term zero.
            ents: [new Entry()],
        });
    }

    /**
     *  InitialState implements the Storage interface.
     */
    InitialState(): [HardState, ConfState, NullableError] {
        return [this.hardState, this.snapshot.metadata.confState, null];
    }

    /**
     * Entries implements the Storage interface.
     */
    Entries(lo: number, hi: number, maxSize: number): [Entry[], NullableError] {
        const offset = this.ents[0].index;
        if (lo <= offset) {
            return [[], new ErrCompacted()];
        }
        if (hi > this.lastIndex() + 1) {
            getLogger().panicf(
                "entries hi(%d) is out of bound lastindex(%d)",
                hi,
                this.lastIndex(),
            );
        }

        // only contains dummy entries.
        if (this.ents.length === 1) {
            return [[], new ErrUnavailable()];
        }

        const ents = this.ents.slice(lo - offset, hi - offset);
        return [limitSize(ents, maxSize), null];
    }

    /**
     * Term implements the Storage interface.
     */
    Term(i: number): [number, NullableError] {
        const offset = this.ents[0].index;
        if (i < offset) {
            return [0, new ErrCompacted()];
        }
        if (i - offset >= this.ents.length) {
            return [0, new ErrUnavailable()];
        }
        return [this.ents[i - offset].term, null];
    }

    /**
     * LastIndex implements the Storage interface.
     */
    LastIndex(): [number, NullableError] {
        return [this.lastIndex(), null];
    }

    /**
     * FirstIndex implements the Storage interface.
     */
    FirstIndex(): [number, NullableError] {
        return [this.firstIndex(), null];
    }

    /**
     * Snapshot implements the Storage interface.
     */
    Snapshot(): [Snapshot, NullableError] {
        return [this.snapshot, null];
    }

    /**
     * SetHardState saves the current HardState.
     */
    SetHardState(st: HardState): NullableError {
        this.hardState = st;
        return null;
    }
    /**
     * ApplySnapshot overwrites the contents of this Storage object with those
     * of the given snapshot.
     */
    ApplySnapshot(snap: Snapshot): NullableError {
        // handle check for old snapshot being applied
        const msIndex = this.snapshot.metadata.index;
        const snapIndex = snap.metadata.index;
        if (msIndex >= snapIndex) {
            return new ErrSnapOutOfDate();
        }

        this.snapshot = snap;
        this.ents = [new Entry({ term: snap.metadata.term, index: snap.metadata.index })];
        return null;
    }
    /**
     * CreateSnapshot makes a snapshot which can be retrieved with Snapshot()
     * and can be used to reconstruct the state at that point. If any
     * configuration changes have been made since the last compaction, the
     * result of the last ApplyConfChange must be passed in.
     */
    CreateSnapshot(i: number, cs: ConfState | null, data: RaftData): [Snapshot, NullableError] {
        if (i <= this.snapshot.metadata.index) {
            return [new Snapshot(), new ErrSnapOutOfDate()];
        }

        const offset = this.ents[0].index;
        if (i > this.lastIndex()) {
            getLogger().panicf(
                "snapshot %d is out of bound lastindex(%d)",
                i,
                this.lastIndex(),
            );
        }

        this.snapshot.metadata.index = i;
        this.snapshot.metadata.term = this.ents[i - offset].term;
        if (cs !== null) {
            this.snapshot.metadata.confState = cs;
        }
        this.snapshot.data = data;
        return [this.snapshot, null];
    }

    /**
     * Compact discards all log entries prior to compactIndex. It is the
     * application's responsibility to not attempt to compact an index greater
     * than raftLog.applied.
     */
    Compact(compactIndex: number): NullableError {
        const offset = this.ents[0].index;

        if (compactIndex <= offset) {
            return new ErrCompacted();
        }
        if (compactIndex > this.lastIndex()) {
            getLogger().panicf(
                "compact %d is out of bound lastindex(%d)",
                compactIndex,
                this.lastIndex(),
            );
        }

        const i = compactIndex - offset;
        const newFirstEntry: Entry = new Entry({
            index: this.ents[i].index,
            term: this.ents[i].term,
        });
        const ents: Entry[] = [newFirstEntry, ...this.ents.slice(i + 1)];
        this.ents = ents;
        return null;
    }

    /**
     * Append the new entries to storage. TO DO (xiangli): ensure the entries
     * are continuous and entries[0].Index > ms.entries[0].Index
     */
    Append(entries: Entry[]): NullableError {
        if (entries.length === 0) {
            return null;
        }

        const first = this.firstIndex();
        const last = entries[0].index + entries.length - 1;

        // shortcut if there is no new entry
        if (last < first) {
            return null;
        }

        // truncate compacted entries
        if (first > entries[0].index) {
            entries = entries.slice(first - entries[0].index);
        }

        const offset = entries[0].index - this.ents[0].index;
        if (this.ents.length > offset) {
            this.ents = [...this.ents.slice(0, offset), ...entries];
        } else if (this.ents.length === offset) {
            this.ents.push(...entries);
        } else {
            getLogger().panicf(
                "missing log entry [last: %d, append at: %d]",
                this.lastIndex(),
                entries[0].index,
            );
        }
        return null;
    }

    private lastIndex(): number {
        const firstEntryIndex = this.ents[0].index;
        return firstEntryIndex + this.ents.length - 1;
    }

    private firstIndex(): number {
        const firstEntryIndex = this.ents[0].index;
        return firstEntryIndex + 1;
    }
}

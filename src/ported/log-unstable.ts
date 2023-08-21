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

import { getLogger, Logger } from "./logger";
import { Entry, Snapshot } from "./raftpb/raft.pb";

/**
 * unstable.entries[i] has raft log position i+unstable.offset. Note that
 * unstable.offset may be less than the highest log position in storage; this
 * means that the next write to storage might need to truncate the log before
 * persisting unstable.entries.
 */
export class Unstable {
    snapshot: Snapshot | null; // Pointer in Go version
    entries: Entry[];
    offset: number;
    logger: Logger;

    constructor(
        param: {
            snapshot?: Snapshot;
            entries?: Entry[];
            offset?: number;
            logger?: Logger;
        } = {}
    ) {
        this.snapshot = param.snapshot ?? null;
        this.entries = param.entries ?? [];
        this.offset = param.offset ?? 0;
        this.logger = param.logger ?? getLogger();
    }

    /**
     * maybeFirstIndex returns the index of the first possible entry in entries
     * if it has a snapshot.
     */
    maybeFirstIndex(): [number, boolean] {
        if (this.snapshot !== null) {
            return [this.snapshot.metadata.index + 1, true];
        }
        return [0, false];
    }

    /**
     * maybeLastIndex returns the last index if it has at least one unstable
     * entry or snapshot.
     */
    maybeLastIndex(): [number, boolean] {
        const l = this.entries.length;
        if (l !== 0) {
            return [this.offset + l - 1, true];
        }
        if (this.snapshot !== null) {
            return [this.snapshot.metadata.index, true];
        }
        return [0, false];
    }

    /**
     * maybeTerm returns the term of the entry at index i, if there is any.
     */
    maybeTerm(i: number): [number, boolean] {
        if (i < this.offset) {
            if (this.snapshot !== null && this.snapshot.metadata.index === i) {
                return [this.snapshot.metadata.term, true];
            }
            return [0, false];
        }

        const [last, ok] = this.maybeLastIndex();
        if (!ok) {
            return [0, false];
        }
        if (i > last) {
            return [0, false];
        }

        return [this.entries[i - this.offset].term, true];
    }

    stableTo(i: number, t: number): void {
        const [gt, ok] = this.maybeTerm(i);
        if (!ok) {
            return;
        }
        // if i < offset, term is matched with the snapshot only update the
        // unstable entries if term is matched with an unstable entry.
        if (gt === t && i >= this.offset) {
            this.entries = this.entries.slice(i + 1 - this.offset, undefined);
            this.offset = i + 1;
            this.shrinkEntriesArray();
        }
    }

    /**
     * shrinkEntriesArray discards the underlying array used by the entries
     * slice if most of it isn't being used. This avoids holding references to a
     * bunch of potentially large entries that aren't needed anymore. Simply
     * clearing the entries wouldn't be safe because clients might still be
     * using them.
     */
    shrinkEntriesArray(): void {
        // -----------------------------------------------------------------------
        // NOTE: This method should no longer be needed in TypeScript since
        // TypeScript doesn't support Go slices. => this.entries is a simple
        // array
        // -----------------------------------------------------------------------
        // // We replace the array if we're using less than half of the space in
        // // it. This number is fairly arbitrary, chosen as an attempt to
        // balance memory usage vs number of allocations. It could probably be
        // improved with some focused tuning. const lenMultiple = 2 if
        // len(u.entries) == 0 { u.entries = nil } else if
        // len(u.entries)*lenMultiple < cap(u.entries) { newEntries :=
        // make([]pb.Entry, len(u.entries)) copy(newEntries, u.entries)
        // u.entries = newEntries
        // }
    }

    stableSnapTo(i: number): void {
        if (this.snapshot !== null && this.snapshot.metadata.index === i) {
            this.snapshot = null;
        }
    }

    restore(s: Snapshot): void {
        this.offset = s.metadata.index + 1;
        this.entries = [];
        this.snapshot = s;
    }

    truncateAndAppend(ents: Entry[]): void {
        const after = ents[0].index;
        if (after === this.offset + this.entries.length) {
            // after is the next index in the u.entries directly append
            this.entries = new Array().concat(this.entries, ents);
        } else if (after <= this.offset) {
            this.logger.infof("replace the unstable entries from index %d", after);
            // The log is being truncated to before our current offset portion,
            // so set the offset and replace the entries
            this.offset = after;
            this.entries = ents;
        } else {
            // truncate to after and copy to u.entries then append
            this.logger.infof("truncate the unstable entries before index %d", after);
            this.entries = this.slice(this.offset, after);
            this.entries = new Array().concat(this.entries, ents);
        }
    }

    slice(lo: number, hi: number): Entry[] {
        this.mustCheckOutOfBounds(lo, hi);
        return this.entries.slice(lo - this.offset, hi - this.offset);
    }

    /**
     * this.offset <= lo <= hi <= this.offset+this.entries.length
     */
    mustCheckOutOfBounds(lo: number, hi: number): void {
        if (lo > hi) {
            this.logger.fatalf("invalid unstable.slice %d > %d", lo, hi);
        }
        const upper = this.offset + this.entries.length;
        if (lo < this.offset || hi > upper) {
            this.logger.fatalf(
                "unstable.slice[%d,%d) out of bound [%d,%d]",
                lo,
                hi,
                this.offset,
                upper
            );
        }
    }
}

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

import { Unstable } from "./log-unstable";
import { getLogger, Logger } from "./logger";
import { noLimit } from "./raft";
import { Entry, Snapshot } from "./raftpb/raft.pb";
import { ErrCompacted, ErrUnavailable, Storage } from "./storage";
import { limitSize, NullableError } from "./util";

export class RaftLog {
    /**
     * storage contains all stable entries since the last snapshot.
     */
    storage: Storage;

    /**
     * unstable contains all unstable entries and snapshot. they will be saved
     * into storage.
     */
    unstable: Unstable;

    /**
     * committed is the highest log position that is known to be in stable
     * storage on a quorum of nodes.
     */
    committed: number = 0;

    /**
     * applied is the highest log position that the application has been
     * instructed to apply to its state machine. Invariant: applied <= committed
     */
    applied: number = 0;

    logger: Logger;

    /**
     * maxNextEntsSize is the maximum number aggregate byte size of the messages
     * returned from calls to nextEnts.
     */
    maxNextEntsSize: number;

    constructor(param: {
        // NOTE: The storage parameter is not optional. If not specified it
        // would have to default to "null" because it is modeled as interface in
        // the go implementation. That would require a lot of unwrapping inside
        // this class which we want to avoid.
        storage: Storage;
        unstable?: Unstable;
        committed?: number;
        applied?: number;
        logger?: Logger;
        maxNextEntsSize?: number;
    }) {
        this.storage = param.storage;
        this.unstable = param.unstable ?? new Unstable();
        this.committed = param.committed ?? 0;
        this.applied = param.applied ?? 0;
        this.logger = param.logger ?? getLogger();
        this.maxNextEntsSize = param.maxNextEntsSize ?? 0;
    }

    toString(): string {
        return (
            "committed=" +
            this.committed +
            ", applied=" +
            this.applied +
            ", unstable.offset=" +
            this.unstable.offset +
            ", unstable.Entries.length=" +
            this.unstable.entries.length
        );
    }

    /**
     * maybeAppend returns (0, false) if the entries cannot be appended.
     * Otherwise, it returns (last index of new entries, true).
     */
    maybeAppend(
        index: number,
        logTerm: number,
        committed: number,
        ...ents: Entry[]
    ): [lastnewi: number, ok: boolean] {
        if (this.matchTerm(index, logTerm)) {
            const lastNewI = index + ents.length;
            const ci = this.findConflict(ents);

            if (ci === 0) {
                // do nothing
            } else if (ci <= this.committed) {
                this.logger.panicf(
                    "entry %d conflict with committed entry [committed(%d)]",
                    ci,
                    this.committed,
                );
            } else {
                const offset = index + 1;
                this.append(...ents.slice(ci - offset, ents.length));
            }
            this.commitTo(Math.min(committed, lastNewI));
            return [lastNewI, true];
        }
        return [0, false];
    }

    append(...ents: Entry[]): number {
        if (ents.length === 0) {
            return this.lastIndex();
        }
        const after = ents[0].index - 1;
        if (after < this.committed) {
            this.logger.panicf(
                "after(%d) is out of range [committed(%d)]",
                after,
                this.committed,
            );
        }
        this.unstable.truncateAndAppend(ents);
        return this.lastIndex();
    }

    /**
     * findConflict finds the index of the conflict. It returns the first pair
     * of conflicting entries between the existing entries and the given
     * entries, if there are any. If there is no conflicting entries, and the
     * existing entries contains all the given entries, zero will be returned.
     * If there is no conflicting entries, but the given entries contains new
     * entries, the index of the first new entry will be returned. An entry is
     * considered to be conflicting if it has the same index but a different
     * term. The index of the given entries MUST be continuously increasing.
     */
    findConflict(ents: Entry[]): number {
        for (const ne of ents) {
            if (!this.matchTerm(ne.index, ne.term)) {
                if (ne.index <= this.lastIndex()) {
                    this.logger.infof(
                        "found conflict at index %d[existing term: %d, conflicting term: %d]",
                        ne.index,
                        this.zeroTermOnErrCompacted(...this.term(ne.index)),
                        ne.term,
                    );
                }
                return ne.index;
            }
        }
        return 0;
    }

    /**
     * findConflictByTerm takes an (index, term) pair (indicating a conflicting
     * log entry on a leader/follower during an append) and finds the largest
     * index in log l with a term <= `term` and an index <= `index`. If no such
     * index exists in the log, the log's first index is returned The index
     * provided MUST be equal to or less than l.lastIndex(). Invalid inputs log
     * a warning and the input index is returned.
     */
    findConflictByTerm(index: number, term: number): number {
        const li = this.lastIndex();
        if (index > li) {
            //  NB: such calls should not exist, but since there is a
            // straightforward way to recover, do it.
            //
            // It is tempting to also check something about the first index, but
            // there is odd behavior with peers that have no log, in which case
            // lastIndex will return zero and firstIndex will return one, which
            // leads to calls with an index of zero into this method.
            this.logger.warningf(
                "index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
                index,
                li,
            );
            return index;
        }

        while (true) {
            const [logTerm, err] = this.term(index);
            if (logTerm <= term || err !== null) {
                break;
            }
            index--;
        }
        return index;
    }

    unstableEntries(): Entry[] {
        return this.unstable.entries;
    }

    /**
     * nextEnts returns all the available entries for execution. If applied is
     * smaller than the index of snapshot, it returns all committed entries
     * after the index of snapshot.
     */
    nextEnts(): Entry[] {
        const off = Math.max(this.applied + 1, this.firstIndex());
        if (this.committed + 1 > off) {
            const [ents, err] = this.slice(off, this.committed + 1, this.maxNextEntsSize);
            if (err !== null) {
                this.logger.panicf("unexpected error when getting unapplied entries (%s)", err);
            }
            return ents;
        }
        return [];
    }

    /**
     * hasNextEnts returns if there is any available entries for execution. This
     * is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
     */
    hasNextEnts(): boolean {
        const off = Math.max(this.applied + 1, this.firstIndex());
        return this.committed + 1 > off;
    }

    /**
     * hasPendingSnapshot returns if there is pending snapshot waiting for
     * applying.
     */
    hasPendingSnapshot(): boolean {
        return this.unstable.snapshot !== null && !this.unstable.snapshot.isEmptySnap();
    }

    snapshot(): [Snapshot, NullableError] {
        if (this.unstable.snapshot !== null) {
            return [this.unstable.snapshot, null];
        }
        return this.storage.Snapshot();
    }

    firstIndex(): number {
        const [i, ok] = this.unstable.maybeFirstIndex();
        if (ok) {
            return i;
        }
        const [index, err] = this.storage.FirstIndex();
        if (err !== null) {
            throw err; // TO DO(bdarnell)
        }
        return index;
    }

    lastIndex(): number {
        const [i, ok] = this.unstable.maybeLastIndex();
        if (ok) {
            return i;
        }
        const [i2, err] = this.storage.LastIndex();
        if (err !== null) {
            throw err; // TO DO(bdarnell)
        }
        return i2;
    }

    commitTo(tocommit: number) {
        // never decrease commit
        if (this.committed < tocommit) {
            if (this.lastIndex() < tocommit) {
                this.logger.panicf(
                    "tocommit(%d) is out of range [lastIndex(%d). Was the raft log corrupted, truncated, or lost?",
                    tocommit,
                    this.lastIndex(),
                );
            }
            this.committed = tocommit;
        }
    }

    appliedTo(i: number): void {
        if (i === 0) {
            return;
        }
        if (this.committed < i || i < this.applied) {
            this.logger.panicf(
                "applied(%d) is out of range [prevApplied(%d), committed(%d)]",
                i,
                this.applied,
                this.committed,
            );
        }
        this.applied = i;
    }

    stableTo(i: number, t: number): void {
        this.unstable.stableTo(i, t);
    }

    stableSnapTo(i: number): void {
        this.unstable.stableSnapTo(i);
    }

    lastTerm(): number {
        const [t, err] = this.term(this.lastIndex());
        if (err !== null) {
            this.logger.panicf("unexpected error when getting the last term (%s)", err);
        }
        return t;
    }

    term(i: number): [number, NullableError] {
        // the valid term range is [index of dummy entry, last index]
        const dummyIndex = this.firstIndex() - 1;
        if (i < dummyIndex || i > this.lastIndex()) {
            // TO DO: return an error instead?
            return [0, null];
        }

        const [t, ok] = this.unstable.maybeTerm(i);
        if (ok) {
            return [t, null];
        }

        const [t2, err] = this.storage.Term(i);
        if (err === null) {
            return [t2, null];
        }
        if (err instanceof ErrCompacted || err instanceof ErrUnavailable) {
            return [0, err];
        }

        throw err; // TO DO(bdarnell)
    }

    entries(i: number, maxsize: number): [Entry[], NullableError] {
        if (i > this.lastIndex()) {
            return [[], null];
        }
        return this.slice(i, this.lastIndex() + 1, maxsize);
    }

    /**
     * allEntries returns all entries in the log.
     */
    allEntries(): Entry[] {
        const [ents, err] = this.entries(this.firstIndex(), noLimit);
        if (err === null) {
            return ents;
        }
        if (err instanceof ErrCompacted) {
            // try again if there was a racing compaction
            return this.allEntries();
        }
        // TO DO (xiangli): handle error?
        throw err;
    }

    /**
     * isUpToDate determines if the given (lastIndex,term) log is more
     * up-to-date by comparing the index and term of the last entries in the
     * existing logs. If the logs have last entries with different terms, then
     * the log with the later term is more up-to-date. If the logs end with the
     * same term, then whichever log has the larger lastIndex is more
     * up-to-date. If the logs are the same, the given log is up-to-date.
     */
    isUpToDate(lasti: number, term: number): boolean {
        return term > this.lastTerm() || (term === this.lastTerm() && lasti >= this.lastIndex());
    }

    matchTerm(i: number, term: number): boolean {
        const [t, err] = this.term(i);
        if (err !== null) {
            return false;
        }
        return t === term;
    }

    maybeCommit(maxIndex: number, term: number): boolean {
        if (
            maxIndex > this.committed &&
            this.zeroTermOnErrCompacted(...this.term(maxIndex)) === term
        ) {
            this.commitTo(maxIndex);
            return true;
        }
        return false;
    }

    restore(s: Snapshot): void {
        this.logger.infof(
            "log [%s] starts to restore snapshot [index: %d, term: %d]",
            this.toString(),
            s.metadata.index,
            s.metadata.term,
        );
        this.committed = s.metadata.index;
        this.unstable.restore(s);
    }

    /**
     * slice returns a slice of log entries from lo through hi-1, inclusive.
     */
    slice(lo: number, hi: number, maxSize: number): [Entry[], NullableError] {
        const err = this.mustCheckOutOfBounds(lo, hi);
        if (err !== null) {
            return [[], err];
        }
        if (lo === hi) {
            return [[], null];
        }

        let ents: Entry[] = [];
        if (lo < this.unstable.offset) {
            const [storedEnts, err2] = this.storage.Entries(
                lo,
                Math.min(hi, this.unstable.offset),
                maxSize * 8
            );
            if (err2 instanceof ErrCompacted) {
                return [[], err2];
            } else if (err2 instanceof ErrUnavailable) {
                this.logger.panicf(
                    "entries[%d:%d] is unavailable from storage",
                    lo,
                    Math.min(hi, this.unstable.offset),
                );
            } else if (err2 !== null) {
                throw err2; // TO DO(bdarnell)
            }

            // check if ents has reached the size limitation
            if (storedEnts.length < Math.min(hi, this.unstable.offset) - lo) {
                return [storedEnts, null];
            }

            ents = storedEnts;
        }

        if (hi > this.unstable.offset) {
            const unstable = this.unstable.slice(Math.max(lo, this.unstable.offset), hi);
            if (ents.length > 0) {
                const combined = ents.slice();
                combined.push(...unstable);
                ents = combined;
            } else {
                ents = unstable;
            }
        }
        return [limitSize(ents, maxSize), null];
    }

    /**
     * l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
     */
    mustCheckOutOfBounds(lo: number, hi: number): NullableError {
        if (lo > hi) {
            this.logger.panicf("invalid slice " + lo + " > " + hi);
        }
        const fi = this.firstIndex();
        if (lo < fi) {
            return new ErrCompacted();
        }

        const length = this.lastIndex() + 1 - fi;
        if (hi > fi + length) {
            this.logger.panicf(
                "slice[%d,%d] out of bound [%d,%d]",
                lo,
                hi,
                fi,
                this.lastIndex,
            );
        }
        return null;
    }

    zeroTermOnErrCompacted(t: number, err: NullableError): number {
        if (err === null) {
            return t;
        }
        if (err instanceof ErrCompacted) {
            return 0;
        }
        this.logger.panicf("unexpected error (%s)", err);
        return 0;
    }
}

/**
 * newLog returns log using the given storage and default options. It recovers
 * the log to the state that it just commits and applies the latest snapshot.
 */
export function newLog(storage: Storage, logger: Logger): RaftLog {
    return newLogWithSize(storage, logger, noLimit);
}

/**
 * newLogWithSize returns a log using the given storage and max message size.
 */
export function newLogWithSize(storage: Storage, logger: Logger, maxNextEntsSize: number): RaftLog {
    const log = new RaftLog({
        storage: storage,
        logger: logger,
        maxNextEntsSize: maxNextEntsSize,
    });
    const [firstIndex, errFirst] = storage.FirstIndex();
    if (errFirst !== null) {
        throw errFirst; // RaftGo-TODO(bdarnell)
    }
    const [lastIndex, errLast] = storage.LastIndex();
    if (errLast !== null) {
        throw errLast; // RaftGo-TODO(bdarnell)
    }
    log.unstable.offset = lastIndex + 1;
    log.unstable.logger = logger;
    // Initialize our committed and applied pointers to the time of the last
    // compaction.
    log.committed = firstIndex - 1;
    log.applied = firstIndex - 1;

    return log;
}

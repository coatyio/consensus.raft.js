// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.
//
// This is a port of Raft – the original work is copyright by "The etcd Authors"
// and licensed under Apache-2.0 similar to the license of this file.

import { AckedIndexer, Index, VoteResult } from "./quorum";
import { rid } from "../raftpb/raft.pb";

const maxUint64 = Number.MAX_SAFE_INTEGER;

/**
 * MajorityConfig is a set of IDs that uses majority quorums to make decisions.
 */
export class MajorityConfig {
    map: Map<rid, object> = new Map<rid, object>();

    toString(): string {
        const ids: rid[] = Array.from(this.map.keys());
        ids.sort();

        const result: string[] = ["("];
        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            if (i > 0) {
                result.push(" ");
            }
            result.push("" + id);
        }

        result.push(")");
        return result.join("");
    }

    length(): number {
        return this.map.size;
    }

    describe(l: AckedIndexer): string {
        if (this.map.size === 0) {
            return "<empty majority quorum>";
        }

        type tup = {
            id: rid;
            idx: Index;
            ok: boolean; // idx found?
            bar: number; // length of bar displayed for this tup
        };

        // Below, populate .bar so that the i-th largest commit index has bar i
        // (we plot this as sort of a progress bar). The actual code is a bit
        // more complicated and also makes sure that equal index => equal bar.
        const n = this.map.size;
        const info: tup[] = [];

        for (const id of this.map.keys()) {
            const [idx, ok] = l.ackedIndex(id);
            info.push({ id: id, idx: idx, ok: ok, bar: 0 });
        }

        // Sort by index
        info.sort((a, b) => {
            if (a.idx === b.idx) {
                return ('' + a.id).localeCompare(b.id);
            }
            return a.idx - b.idx;
        });

        // Populate .bar.
        for (let i = 0; i < info.length; i++) {
            if (i > 0 && info[i - 1].idx < info[i].idx) {
                info[i].bar = i;
            }
        }

        // Sort by Id.
        info.sort((a, b) => {
            return ('' + a.id).localeCompare(b.id);
        });

        // Print.
        const buf = [];
        buf.push(" ".repeat(n) + "    idx\n");
        for (const inf of info) {
            const bar = inf.bar;

            if (!inf.ok) {
                buf.push("?" + " ".repeat(n));
            } else {
                buf.push("x".repeat(bar) + ">" + " ".repeat(n - bar));
            }
            const idxString: string = "" + inf.idx;
            buf.push(" " + idxString.padStart(5, " ") + "    (id=" + inf.id + ")\n");
        }

        return buf.join("");
    }

    /**
     * Slice returns the MajorityConfig as a sorted list.
     */
    slice(): rid[] {
        const result = Array.from(this.map.keys());
        return result.sort();
    }

    /**
     * CommittedIndex computes the committed index from those supplied via the
     * provided AckedIndexer (for the active config).
     */
    committedIndex(l: AckedIndexer): Index {
        const n = this.map.size;
        if (n === 0) {
            // This plays well with joint quorums which, when one half is the
            // zero MajorityConfig, should behave like the other half.
            return maxUint64;
        }

        const srt: number[] = new Array(n).fill(0);
        {
            // Fill the slice with the indexes observed. Any unused slots will
            // be left as zero; these correspond to voters that may report in,
            // but haven't yet. We fill from the right (since the zeroes will
            // end up on the left after sorting below anyway).
            let i = n - 1;
            for (const id of this.map.keys()) {
                const [idx, ok] = l.ackedIndex(id);
                if (ok) {
                    srt[i] = idx;
                    i--;
                }
            }
        }

        // Sort by index. Use a bespoke algorithm (copied from the stdlib's sort
        // package) to keep srt on the stack. Use insertionSort as ported from
        // the go implementation
        this.insertionSort(srt);

        // The smallest index into the array for which the value is acked by a
        // quorum. In other words, from the end of the slice, move n/2+1 to the
        // left (accounting for zero-indexing).
        const pos = n - (Math.floor(n / 2) + 1);
        const result: Index = srt[pos];
        return result;
    }

    /**
     * VoteResult takes a mapping of voters to yes/no (true/false) votes and
     * returns a result indicating whether the vote is pending (i.e. neither a
     * quorum of yes/no has been reached), won (a quorum of yes has been
     * reached), or lost (a quorum of no has been reached).
     */
    voteResult(votes: Map<rid, boolean>): VoteResult {
        if (this.map.size === 0) {
            // By convention, the elections on an empty config win. This comes
            // in handy with joint quorums because it'll make a half-populated
            // joint quorum behave like a majority quorum.
            return VoteResult.VoteWon;
        }

        // vote counts for no and yes, respectively
        const ny: [number, number] = [0, 0];

        let missing: number = 0;
        for (const id of this.map.keys()) {
            const v: boolean | undefined = votes.get(id);
            if (v === undefined) {
                missing++;
                continue;
            }
            if (v) {
                ny[1]++;
            } else {
                ny[0]++;
            }
        }

        const q = Math.floor(this.map.size / 2) + 1;
        if (ny[1] >= q) {
            return VoteResult.VoteWon;
        }
        if (ny[1] + missing >= q) {
            return VoteResult.VotePending;
        }
        return VoteResult.VoteLost;
    }

    private insertionSort(sl: number[]) {
        const a = 0;
        const b = sl.length;
        for (let i = a + 1; i < b; i++) {
            for (let j = i; j > a && sl[j] < sl[j - 1]; j--) {
                const help = sl[j];
                sl[j] = sl[j - 1];
                sl[j - 1] = help;
            }
        }
    }
}

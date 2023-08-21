// Copyright 2019 The etcd Authors
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

import { MajorityConfig } from "../../../ported/quorum/majority";
import { AckedIndexer, Index, MapAckIndexer } from "../../../ported/quorum/quorum";
import { noLimit } from "../../../ported/raft";
import { rid } from "../../../ported/raftpb/raft.pb";

const maxUint64 = noLimit;

export enum Vote {
    // it is important that pending is 0, since makeLookuper removes indices
    // with 0 idxs (corresponds to voters with pending votes)
    Pending = 0,
    No = 1,
    Yes = 2,
}

// Helper that returns an AckedIndexer which has the specified indexes mapped to
// the right IDs.
export function makeLookuper(idxs: Index[], ids: rid[], idsj: rid[]): MapAckIndexer {
    const l: MapAckIndexer = new MapAckIndexer();
    let p: number = 0; // next to consume from idxs

    for (const id of [...ids, ...idsj]) {
        if (l.map.has(id)) {
            continue;
        }
        if (p < idxs.length) {
            // NB: this creates zero entries for placeholders that we remove
            // later. The upshot of doing it that way is to avoid having to
            // specify place- holders multiple times when omitting voters
            // present in both halves of a joint config.
            l.map.set(id, idxs[p]);
            p++;
        }
    }

    for (const id of l.map.keys()) {
        // Zero entries are created by _ placeholders; we don't want them in the
        // lookuper because "no entry" is different from "zero entry". Note that
        // we prevent tests from specifying zero commit indexes, so that there's
        // no confusion between the two concepts.
        if (l.map.get(id) === 0) {
            l.map.delete(id);
        }
    }
    return l;
}

// This is an alternative implementation of (MajorityConfig).CommittedIndex(l).
export function alternativeMajorityCommittedIndex(c: MajorityConfig, l: AckedIndexer): Index {
    if (c.length() === 0) {
        return maxUint64;
    }

    const idToIdx: Map<rid, Index> = new Map<rid, Index>();
    for (const id of c.map.keys()) {
        const [idx, ok] = l.ackedIndex(id);
        if (ok) {
            idToIdx.set(id, idx);
        }
    }

    // Build a map from index to voters who have acked that or any higher index.
    const idxToVotes: Map<Index, number> = new Map<Index, number>();
    for (const idx of idToIdx.values()) {
        idxToVotes.set(idx, 0);
    }

    for (const idx of idToIdx.values()) {
        for (const idy of idxToVotes.keys()) {
            if (idy > idx) {
                continue;
            }
            const currentValue = idxToVotes.get(idy)!;
            idxToVotes.set(idy, currentValue + 1);
        }
    }

    // Find the maximum index that has achieved quorum.
    const q = Math.floor(c.length() / 2) + 1;

    let maxQuorumIdx: Index = 0;
    for (const [idx, n] of idxToVotes) {
        if (n >= q && idx > maxQuorumIdx) {
            maxQuorumIdx = idx;
        }
    }

    return maxQuorumIdx;
}

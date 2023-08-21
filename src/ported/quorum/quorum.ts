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

import { noLimit } from "../raft";
import { rid } from "../raftpb/raft.pb";

/**
 * NOTE: This is not really maxUint64 but rather maximum number representable by
 * type number, change this if there are changes to noLimit
 */
const maxUint64 = noLimit;

/**
 * Index is a Raft log position.
 */
export type Index = number;

export function indexToString(i: Index) {
    if (i === maxUint64) {
        return "∞";
    }
    return "" + i;
}

/**
 * AckedIndexer allows looking up a commit index for a given ID of a voter from
 * a corresponding MajorityConfig.
 */
export interface AckedIndexer {
    ackedIndex(voterId: rid): [idx: Index, found: boolean];
}

export class MapAckIndexer implements AckedIndexer {
    map: Map<rid, Index> = new Map<rid, Index>();

    ackedIndex(voterId: rid): [idx: number, found: boolean] {
        if (this.map.has(voterId)) {
            return [this.map.get(voterId)!, true];
        } else {
            return [0, false];
        }
    }
}

/**
 * VoteResult indicates the outcome of a vote.
 */
export enum VoteResult {

    /**
     * VotePending indicates that the decision of the vote depends on future
     * votes, i.e. neither "yes" or "no" has reached quorum yet.
     */
    VotePending = 1,

    /**
     * VoteLost indicates that the quorum has voted "no".
     */
    VoteLost = 2,

    /**
     * VoteWon indicates that the quorum has voted "yes".
     */
    VoteWon = 3,
}

const strMap = ["VotePending", "VoteLost", "VoteWon"];

export function voteResultToString(result: VoteResult) {
    return strMap[result - 1];
}

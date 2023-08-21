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

import tap from "tap";

import { alternativeMajorityCommittedIndex } from "./util";
import { MajorityConfig } from "../../../ported/quorum/majority";
import { Index, MapAckIndexer } from "../../../ported/quorum/quorum";
import { rid } from "../../../ported/raftpb/raft.pb";

// Heuristically assert that the main implementation of
// (MajorityConfig).CommittedIndex agrees with a "dumb" alternative version.
tap.test("quorum quick", (t) => {
    for (let i = 0; i < 50000; i++) {
        const memberMap = generateMemberMap();
        const idxMap = generateIdxMap();

        const actualConfig = new MajorityConfig();
        actualConfig.map = memberMap;
        const actualIndexer = new MapAckIndexer();
        actualIndexer.map = idxMap;
        const actual = actualConfig.committedIndex(actualIndexer);

        const wantConfig = new MajorityConfig();
        wantConfig.map = memberMap;
        const wantIndexer = new MapAckIndexer();
        wantIndexer.map = idxMap;
        const want = alternativeMajorityCommittedIndex(wantConfig, wantIndexer);

        t.equal(
            actual,
            want,
            "#" +
            i +
            ": memberMap: " +
            mapToString(memberMap) +
            " idxMap: " +
            mapToString(idxMap) +
            "actual " +
            actual +
            ", want " +
            want
        );
    }

    t.end();
});

// smallRandIdxMap returns a reasonably sized map of ids to commit indexes.
function smallRandIdxMap(): Map<rid, Index> {
    // Hard-code a reasonably small size here
    const size = 10;

    const n = Math.floor(Math.random() * size);

    const to2n = [];
    for (let i = 0; i < 2 * n; i++) {
        to2n[i] = i;
    }
    shuffle(to2n);

    const ids = to2n.slice(0, n);
    const idxs: Index[] = [];
    for (let i = 0; i < ids.length; i++) {
        idxs[i] = Math.floor(Math.random() * n);
    }

    const m = new Map<rid, Index>();
    for (let i = 0; i < ids.length; i++) {
        m.set(ids[i].toString(), idxs[i]);
    }

    return m;
}

type IdxMap = Map<rid, Index>;

function generateIdxMap(): IdxMap {
    return smallRandIdxMap();
}

type MemberMap = Map<rid, object>;

function generateMemberMap(): MemberMap {
    const m = smallRandIdxMap();
    const mm = new Map<rid, object>();
    for (const id of m.keys()) {
        mm.set(id, {});
    }
    return mm;
}

// Fisher-Yates algorithm for permuting an array
function shuffle(array: any[]): void {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * i);
        const temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
}

function mapToString(m: Map<rid, any>): string {
    const result: string[] = ["("];
    const keys: rid[] = Array.of(...m.keys()).sort();

    for (const k of keys) {
        result.push(k + ": " + m.get(k) + ", ");
    }

    result.push(")");
    return result.join("");
}

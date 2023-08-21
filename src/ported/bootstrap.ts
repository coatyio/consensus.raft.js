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

import { Peer } from "./node";
import {
    ConfChange,
    ConfChangeType,
    Entry,
    EntryType,
    HardState,
    ridnone
} from "./raftpb/raft.pb";
import { RawNode } from "./rawnode";
import { NullableError } from "./util";

/** Bootstrap initializes the RawNode for first use by appending configuration
 * changes for the supplied peers. This method returns an error if the Storage
 * is nonempty.
 *
 * It is recommended that instead of calling this method, applications bootstrap
 * their state manually by setting up a Storage that has a first index > 1 and
 * which stores the desired ConfState as its InitialState.
 */
export function Bootstrap(rn: RawNode, peers: Peer[]): NullableError {
    if (peers.length === 0) {
        return new Error("must provide at least one peer to Bootstrap");
    }

    const [lastIndex, err] = rn.raft.raftLog.storage.LastIndex();
    if (err !== null) {
        return err;
    }

    if (lastIndex !== 0) {
        return new Error("can't bootstrap a nonempty Storage");
    }

    // We've faked out initial entries above, but nothing has been persisted.
    // Start with an empty HardState (thus the first Ready will emit a HardState
    // update for the app to persist).
    rn.prevHardSt = new HardState();

    // RaftGo-TODO(tbg): remove StartNode and give the application the right
    // tools to bootstrap the initial membership in a cleaner way.
    rn.raft.becomeFollower(1, ridnone);
    const ents: Entry[] = [];
    for (let i = 0; i < peers.length; i++) {
        const peer = peers[i];
        const cc = new ConfChange({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: peer.id,
            context: peer.context,
        });
        ents.push(
            new Entry({
                type: EntryType.EntryConfChange,
                term: 1,
                index: i + 1,
                data: cc,
            })
        );
    }
    rn.raft.raftLog.append(...ents);

    // Now apply them, mainly so that the application can call Campaign
    // immediately after StartNode in tests. Note that these nodes will be added
    // to raft twice: here and when the application's Ready loop calls
    // ApplyConfChange. The calls to addNode must come after all calls to
    // raftLog.append so progress.next is set after these bootstrapping entries
    // (it is an error if we try to append these entries since they have already
    // been committed). We do not set raftLog.applied so the application will be
    // able to observe all conf changes via Ready.CommittedEntries.
    //
    // RaftGo-TODO(bdarnell): These entries are still unstable; do we need to
    // preserve the invariant that committed < unstable?

    rn.raft.raftLog.committed = ents.length;
    for (const peer of peers) {
        rn.raft.applyConfChange(
            new ConfChange({
                nodeId: peer.id,
                type: ConfChangeType.ConfChangeAddNode,
            }).asV2()
        );
    }
    return null;
}

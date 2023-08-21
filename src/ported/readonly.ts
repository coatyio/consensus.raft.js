// Copyright 2016 The etcd Authors
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

import { ReadOnlyOption } from "./raft";
import { Message, rid } from "./raftpb/raft.pb";
import { emptyRaftData, RaftData } from "../non-ported/raft-controller";

/**
 * ReadState provides state for read only query. It's caller's responsibility to
 * call ReadIndex first before getting this state from ready, it's also caller's
 * duty to differentiate if this state is what it requests through RequestCtx,
 * eg. given a unique id as RequestCtx
 */
export class ReadState {
    index: number;
    requestCtx: RaftData;

    constructor(
        param: {
            index?: number;
            requestCtx?: RaftData;
        } = {}
    ) {
        this.index = param?.index ?? 0;
        this.requestCtx = param?.requestCtx ?? emptyRaftData;
    }
}

export class ReadIndexStatus {
    req: Message;
    index: number;

    // NB: this never records 'false', but it's more convenient to use this
    // instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
    // this becomes performance sensitive enough (doubtful), quorum.VoteResult
    // can change to an API that is closer to that of CommittedIndex.
    acks: Map<rid, boolean>;

    constructor(
        param: {
            req?: Message;
            index?: number;
            acks?: Map<rid, boolean>;
        } = {}
    ) {
        this.req = param?.req ?? new Message();
        this.index = param?.index ?? 0;
        this.acks = param?.acks ?? new Map<rid, boolean>();
    }
}

export class ReadOnly {
    option: ReadOnlyOption;
    // pendingReadIndex is a pointer in Go
    pendingReadIndex: Map<RaftData, ReadIndexStatus | null>;
    readIndexQueue: RaftData[];

    constructor(
        param: {
            option?: ReadOnlyOption;
            pendingReadIndex?: Map<RaftData, ReadIndexStatus | null>;
            readIndexQueue?: RaftData[];
        } = {}
    ) {
        this.option = param?.option ?? ReadOnlyOption.ReadOnlySafe;
        this.pendingReadIndex =
            param?.pendingReadIndex ?? new Map<RaftData, ReadIndexStatus | null>();
        this.readIndexQueue = param?.readIndexQueue ?? [];
    }

    static NewReadOnly(option: ReadOnlyOption): ReadOnly {
        return new ReadOnly({
            option: option,
            pendingReadIndex: new Map<RaftData, ReadIndexStatus | null>(),
        });
    }

    /**
     * addRequest adds a read only request into readonly struct. `index` is the
     * commit index of the raft state machine when it received the read only
     * request. `m` is the original read only request message from the local or
     * remote node.
     */
    addRequest(index: number, m: Message) {
        const s = m.entries[0].data;

        const ok = this.pendingReadIndex.get(s);
        if (ok) {
            return;
        }
        this.pendingReadIndex.set(s, {
            index: index,
            req: m,
            acks: new Map<rid, boolean>(),
        });
        this.readIndexQueue.push(s);
    }

    /**
     * recvAck notifies the readonly struct that the raft state machine received
     * an acknowledgment of the heartbeat that attached with the read only
     * request context.
     */
    recvAck(id: rid, context: RaftData): Map<rid, boolean> {
        const s = context;
        const res = this.pendingReadIndex.get(s);
        if (!res) {
            return new Map<rid, boolean>();
        }

        res.acks.set(id, true);
        return res.acks;
    }

    /**
     * advance advances the read only request queue kept by the readonly struct.
     * It dequeues the requests until it finds the read only request that has
     * the same context as the given `m`.
     */
    advance(m: Message): ReadIndexStatus[] {
        let i: number = 0;
        let found: boolean = false;

        const ctx = m.context;
        const rss: ReadIndexStatus[] = [];

        for (const value of this.readIndexQueue) {
            i++;
            const rs = this.pendingReadIndex.get(value);
            if (!rs) {
                throw new Error("cannot find corresponding read state from pending map");
            }
            rss.push(rs);
            if (value === ctx) {
                found = true;
                break;
            }
        }

        if (found) {
            this.readIndexQueue = this.readIndexQueue.slice(i);
            for (const value of rss) {
                const key = value.req.entries[0].data;
                this.pendingReadIndex.delete(key);
            }
            return rss;
        }

        return [];
    }

    /**
     * lastPendingRequestCtx returns the context of the last pending read only
     * request in readonly struct.
     */
    lastPendingRequestCtx(): RaftData {
        if (!this.readIndexQueue || this.readIndexQueue.length === 0) {
            return emptyRaftData;
        }
        return this.readIndexQueue[this.readIndexQueue.length - 1];
    }
}

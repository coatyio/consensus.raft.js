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

import { Raft, RaftConfig, noLimit } from "../../ported/raft";
import { Entry, Message, HardState, MessageType, rid } from "../../ported/raftpb/raft.pb";
import { RawNode } from "../../ported/rawnode";
import { MemoryStorage, Storage } from "../../ported/storage";
import { Progress } from "../../ported/tracker/progress";
import { MakeProgressTracker } from "../../ported/tracker/tracker";
import { NullableError } from "../../ported/util";

/**
 * This contains all function and class definitions from raft_test.go This is
 * extracted from the test file (raft.test.ts), to avoid executing all tests in
 * that file each time parts of it are imported in another file.
 */

export type ConnemString = string;

// nextEnts returns the applicable entries and updates the applied index
export function nextEnts(r: Raft, s: MemoryStorage): Entry[] {
    // Transfer all unstable entries to "stable" storage.
    s.Append(r.raftLog.unstableEntries());
    r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm());

    const ents = r.raftLog.nextEnts();
    r.raftLog.appliedTo(r.raftLog.committed);
    return ents;
}

export function mustAppendEntry(r: Raft, ...ents: Entry[]) {
    if (!r.appendEntry(...ents)) {
        throw new Error("entry unexpectedly dropped");
    }
}

export type StateMachine = StateMachineI | null;

export interface StateMachineI {
    Step(m: Message): NullableError;
    readMessages(): Message[];
}

/**
 * Test class in order to be able to add the method readMessages to an object of
 * type raft
 */
export class RaftForTest extends Raft implements StateMachineI {
    // This is a lengthy constructor that needs to be here...
    constructor(cfg: RaftConfig) {
        const r = Raft.NewRaft(cfg);
        super({
            raftLog: r.raftLog,
            logger: r.logger,
            readOnly: r.readOnly,
            id: r.id,
            lead: r.lead,
            isLearner: r.isLearner,
            maxMsgSize: r.maxMsgSize,
            maxUncommittedSize: r.maxUncommittedSize,
            prs: r.prs,
            heartbeatTimeout: r.heartbeatTimeout,
            electionTimeout: r.electionTimeout,
            checkQuorum: r.checkQuorum,
            preVote: r.preVote,
            disableProposalForwarding: r.disableProposalForwarding,
            term: r.term,
            vote: r.vote,
            readStates: r.readStates,
            state: r.state,
            msgs: r.msgs,
            leadTransferee: r.leadTransferee,
            pendingConfIndex: r.pendingConfIndex,
            uncommittedSize: r.uncommittedSize,
            electionElapsed: r.electionElapsed,
            heartbeatElapsed: r.heartbeatElapsed,
            randomizedElectionTimeout: r.randomizedElectionTimeout,
            tick: r.tick ?? undefined,
            step: r.step ?? undefined,
            pendingReadIndexMessages: r.pendingReadIndexMessages,
        });
    }

    readMessages(): Message[] {
        const msgs = this.msgs;

        this.msgs = [];

        return msgs;
    }
}

export class Connem {
    from: rid;
    to: rid;

    constructor(from: rid, to: rid) {
        this.from = from;
        this.to = to;
    }

    /**
     * Gives a strict representation of this Connem for comparison to other
     * Connems
     *
     * In TypeScript, objects as keys in maps do not work as expected. TS
     * compares the references to the object, there is no way to implement
     * something like an equals method in Java Therefore, this method is used to
     * convert Connems to strings to be able to get values from a map
     */
    toMapKey(): string {
        return this.from + ":" + this.to;
    }
}

export class BlackHole implements StateMachineI {
    Step(_: Message): NullableError {
        return null;
    }

    readMessages(): Message[] {
        return [];
    }
}

export const nopStepper: BlackHole = new BlackHole();

export function entsWithConfig(
    configFunc: ((arg0: RaftConfig) => void) | null,
    ...terms: number[]
): RaftForTest {
    const storage = MemoryStorage.NewMemoryStorage();
    terms.forEach((term, i) => {
        const entry: Entry = new Entry({ index: i + 1, term: term });
        storage.Append([entry]);
    });
    const cfg = newTestConfig("1", 5, 1, storage);
    if (configFunc !== null) {
        configFunc(cfg);
    }
    const sm = new RaftForTest(cfg);
    sm.reset(terms[terms.length - 1]);
    return sm;
}

// votedWithConfig creates a raft state machine with Vote and Term set to the
// given value but no log entries (indicating that it voted in the given term
// but has not received any logs).
export function votedWithConfig(
    configFunc: ((arg0: RaftConfig) => void) | null,
    vote: rid,
    term: number
): RaftForTest {
    const storage = MemoryStorage.NewMemoryStorage();
    storage.SetHardState(new HardState({ term: term, vote: vote }));
    const cfg = newTestConfig("1", 5, 1, storage);
    if (configFunc !== null) {
        configFunc(cfg);
    }
    const sm = new RaftForTest(cfg);
    sm.reset(term);
    return sm;
}

export type Network = {
    peers: Map<rid, StateMachine>;
    storage: Map<rid, MemoryStorage>;
    // This is Map<Connem, number> in go, unfortunately comparing objects as
    // keys does not work as expected. Therefore, turn Connems to string using
    // Connem.toMapKey
    dropm: Map<ConnemString, number>;
    ignorem: Map<MessageType, boolean>;

    // msgHook is called for each message sent. It may inspect the message and
    // return true to send it or false to drop it.
    msgHook: ((arg0: Message) => boolean) | null;
};

export function newNetwork(...peers: StateMachine[]): Network {
    return newNetworkWithConfig(null, ...peers);
}

export function newNetworkWithConfig(
    configFunc: ((arg0: RaftConfig) => void) | null,
    ...peers: (StateMachine | null)[]
): Network {
    const size = peers.length;
    const peerAddrs = idsBySize(size);

    const nPeers = new Map<rid, StateMachine>();
    const nStorage = new Map<rid, MemoryStorage>();

    peers.forEach((p, i) => {
        const id = peerAddrs[i];
        // NOTE: Make sure this is not called with a peer of type Raft, but
        // instead of type RaftForTest
        switch (p ? p.constructor : null) {
            case null:
                nStorage.set(id, newTestMemoryStorage(withPeers(...peerAddrs)));
                const cfg = newTestConfig(id, 10, 1, nStorage.get(id)!);
                if (configFunc !== null) {
                    configFunc(cfg);
                }
                const sm = new RaftForTest(cfg);
                nPeers.set(id, sm);
                break;
            case RaftForTest:
                const pAsRaft = p as unknown as RaftForTest;
                // TO DO(tbg): this is all pretty confused. Clean this up.
                const learners = new Map<rid, boolean>();
                for (const k of pAsRaft.prs.config.learners.keys()) {
                    learners.set(k, true);
                }
                pAsRaft.id = id;
                pAsRaft.prs = MakeProgressTracker(pAsRaft.prs.maxInflight);

                if (learners.size > 0) {
                    pAsRaft.prs.config.learners = new Map<rid, object>();
                }
                for (let k = 0; k < size; k++) {
                    const progress = new Progress({});
                    if (learners.has(peerAddrs[k])) {
                        progress.isLearner = true;
                        pAsRaft.prs.config.learners.set(peerAddrs[k], {});
                    } else {
                        pAsRaft.prs.config.voters.config[0].map.set(peerAddrs[k], {});
                    }
                    pAsRaft.prs.progress.set(peerAddrs[k], progress);
                }

                pAsRaft.reset(pAsRaft.term);
                nPeers.set(id, pAsRaft);

                break;
            case BlackHole:
                const pAsBlackHole = p as unknown as BlackHole;
                nPeers.set(id, pAsBlackHole);
                break;
            default:
                throw new Error("unexpected state machine type: " + p);
        }
    });

    const result: Network = {
        peers: nPeers,
        storage: nStorage,
        dropm: new Map<ConnemString, number>(),
        ignorem: new Map<MessageType, boolean>(),
        msgHook: null,
    };
    return result;
}

export function preVoteConfig(cfg: RaftConfig): void {
    cfg.preVote = true;
}

export function send(nw: Network, ...msgs: Message[]) {
    let loopCounter = 0;
    while (msgs.length > 0 && loopCounter < 10000) {
        const m = msgs[0];
        const p = nw.peers.get(m.to)!;
        p.Step(m);
        msgs = msgs.slice(1, msgs.length);
        msgs.push(...filter(nw, p.readMessages()));

        loopCounter++;
    }
    if (loopCounter >= 10000) {
        throw new Error("Sent over 10000 messages, probably infinite loop?");
    }
}

export function drop(nw: Network, from: rid, to: rid, perc: number) {
    nw.dropm.set(new Connem(from, to).toMapKey(), perc);
}

export function cut(nw: Network, one: rid, other: rid) {
    drop(nw, one, other, 2.0); // always drop
    drop(nw, other, one, 2.0); // always drop
}

export function isolate(nw: Network, id: rid) {
    for (let i = 0; i < nw.peers.size; i++) {
        const nid = (i + 1).toString();
        if (nid !== id) {
            drop(nw, id, nid, 1.0); // always drop
            drop(nw, nid, id, 1.0); // always drop
        }
    }
}

export function ignore(nw: Network, t: MessageType) {
    nw.ignorem.set(t, true);
}

export function recover(nw: Network) {
    nw.dropm = new Map<ConnemString, number>();
    nw.ignorem = new Map<MessageType, boolean>();
}

export function filter(nw: Network, msgs: Message[]): Message[] {
    const mm: Message[] = [];
    msgs.forEach((m) => {
        if (nw.ignorem.get(m.type)) {
            return;
        }

        switch (m.type) {
            case MessageType.MsgHup:
                // hups never go over the network, so don't drop them but panic
                throw new Error("unexpected msgHup");
            default:
                const perc = nw.dropm.get(new Connem(m.from, m.to).toMapKey())!;
                const n = Math.random();
                if (n < perc) {
                    return;
                }
        }

        if (nw.msgHook !== null) {
            if (!nw.msgHook(m)) {
                return;
            }
        }
        mm.push(m);
    });

    return mm;
}

export function idsBySize(size: number): rid[] {
    const ids: rid[] = [];
    for (let i = 0; i < size; i++) {
        ids.push((i + 1).toString());
    }
    return ids;
}

// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
export function setRandomizedElectionTimeout(r: Raft, v: number) {
    r.randomizedElectionTimeout = v;
}

export function newTestConfig(
    id: rid,
    election: number,
    heartbeat: number,
    storage: Storage
): RaftConfig {
    const c = new RaftConfig();
    c.id = id;
    c.electionTick = election;
    c.heartbeatTick = heartbeat;
    c.storage = storage;
    c.maxSizePerMsg = noLimit;
    c.maxInflightMsgs = 256;
    return c;
}

export type TestMemoryStorageOptions = (m: MemoryStorage) => void;

export function withPeers(...peers: rid[]): TestMemoryStorageOptions {
    return (ms: MemoryStorage) => {
        ms.snapshot.metadata.confState.voters = peers;
    };
}

export function withLearners(...learners: rid[]): TestMemoryStorageOptions {
    return (ms: MemoryStorage) => {
        ms.snapshot.metadata.confState.learners = learners;
    };
}

export function newTestMemoryStorage(...opt: TestMemoryStorageOptions[]): MemoryStorage {
    const ms = MemoryStorage.NewMemoryStorage();
    for (const o of opt) {
        o(ms);
    }
    return ms;
}

export function newTestRaft(
    id: rid,
    election: number,
    heartbeat: number,
    storage: Storage
): RaftForTest {
    const testConf = newTestConfig(id, election, heartbeat, storage);
    testConf.validate();
    return new RaftForTest(testConf);
}

export function newTestLearnerRaft(
    id: rid,
    election: number,
    heartbeat: number,
    storage: Storage
): RaftForTest {
    const cfg = newTestConfig(id, election, heartbeat, storage);
    return new RaftForTest(cfg);
}

// newTestRawNode sets up a RawNode with the given peers. The configuration will
// not be reflected in the Storage.
export function newTestRawNode(
    id: rid,
    election: number,
    heartbeat: number,
    storage: Storage
): RawNode {
    const cfg = newTestConfig(id, election, heartbeat, storage);
    const rn = RawNode.NewRawNode(cfg);
    return rn;
}

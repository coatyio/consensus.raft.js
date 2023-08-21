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

import Channel, { ReadChannel, ReadWriteChannel } from "../@nodeguy/channel/lib";

import { Bootstrap } from "./bootstrap";
import { Raft, RaftConfig, StateType } from "./raft";
import { marshalConfChange } from "./raftpb/confchange";
import {
    ConfChangeI,
    ConfChangeV2,
    ConfState,
    Entry,
    HardState,
    Message,
    MessageType,
    rid,
    ridnone,
    Snapshot
} from "./raftpb/raft.pb";
import { RawNode } from "./rawnode";
import { ReadState } from "./readonly";
import { Status } from "./status";
import { isLocalMsg, isResponseMsg, NullableError } from "./util";
import { emptyRaftData, RaftData } from "../non-ported/raft-controller";

export enum SnapshotStatus {
    SnapshotFinish = 1,
    SnapshotFailure = 2,
}

/**
 * errStopped is returned by methods on Nodes that have been stopped.
 */
export const errStopped = new Error("raft: stopped");

/**
 * SoftState provides state that is useful for logging and debugging. The state
 * is volatile and does not need to be persisted to the WAL.
 */
export class SoftState {
    lead: rid;
    raftState: StateType;

    constructor(
        param: {
            lead?: rid;
            raftState?: StateType;
        } = {}
    ) {
        this.lead = param?.lead ?? ridnone;
        this.raftState = param?.raftState ?? 0;
    }

    equal(a: SoftState | null): boolean {
        return !(a === null) && this.lead === a.lead && this.raftState === a.raftState;
    }
}

/**
 * Ready encapsulates the entries and messages that are ready to read, be saved
 * to stable storage, committed or sent to other peers. All fields in Ready are
 * read-only.
 */
export class Ready {
    /**
     * The current volatile state of a Node. SoftState will be null if there is
     * no update. It is not required to consume or store SoftState. NOTE:
     * embedded struct pointer in Go, hence the possibility to nullify it.
     */
    readonly softState: SoftState | null;

    /**
     * The current state of a Node to be saved to stable storage BEFORE Messages
     * are sent. HardState will be equal to empty state if there is no update.
     * NOTE: embedded struct in Go
     */
    readonly hardState: HardState;

    /**
     * ReadStates can be used for node to serve linearizable read requests
     * locally when its applied index is greater than the index in ReadState.
     * Note that the readState will be returned when raft receives msgReadIndex.
     * The returned is only valid for the request that requested to read.
     */
    readonly readStates: ReadState[];

    /**
     * Entries specifies entries to be saved to stable storage BEFORE Messages
     * are sent.
     */
    readonly entries: Entry[];

    /**
     * Snapshot specifies the snapshot to be saved to stable storage.
     */
    readonly snapshot: Snapshot;

    /**
     * CommittedEntries specifies entries to be committed to a
     * store/state-machine. These have previously been committed to stable
     * store.
     */
    readonly committedEntries: Entry[];

    /**
     * Messages specifies outbound messages to be sent AFTER Entries are
     * committed to stable storage. If it contains a MsgSnap message, the
     * application MUST report back to raft when the snapshot has been received
     * or has failed by calling reportSnapshot.
     */
    readonly messages: Message[];

    /**
     * MustSync indicates whether the HardState and Entries must be
     * synchronously written to disk or if an asynchronous write is permissible.
     */
    readonly mustSync: boolean;

    constructor(
        param: {
            softState?: SoftState;
            hardState?: HardState;
            readStates?: ReadState[];
            entries?: Entry[];
            snapshot?: Snapshot;
            committedEntries?: Entry[];
            messages?: Message[];
            mustSync?: boolean;
        } = {}
    ) {
        this.softState = param?.softState ?? null;
        this.hardState = param?.hardState ?? new HardState();
        this.readStates = param?.readStates ?? [];
        this.entries = param?.entries ?? [];
        this.snapshot = param?.snapshot ?? new Snapshot();
        this.committedEntries = param?.committedEntries ?? [];
        this.messages = param?.messages ?? [];
        this.mustSync = param?.mustSync ?? false;
    }

    containsUpdates(): boolean {
        return (
            this.softState !== null ||
            !this.hardState.isEmptyHardState() ||
            !this.snapshot.isEmptySnap() ||
            this.entries.length > 0 ||
            this.committedEntries.length > 0 ||
            this.messages.length > 0 ||
            this.readStates.length !== 0
        );
    }

    /**
     * appliedCursor extracts from the Ready the highest index the client has
     * applied (once the Ready is confirmed via Advance). If no information is
     * contained in the Ready, returns zero
     */
    appliedCursor(): number {
        const cELength = this.committedEntries.length;
        if (cELength > 0) {
            return this.committedEntries[cELength - 1].index;
        }
        const index = this.snapshot.metadata.index;
        if (index > 0) {
            return index;
        }
        return 0;
    }
}

/**
 * Node represents a node in a raft cluster.
 */
export interface Node {
    /**
     * tick() increments the internal logical clock for the NodeInterface by a
     * single tick. Election timeouts and heartbeat timeouts are in units of
     * ticks.
     */
    tick(): Promise<void>;

    /**
     * Campaign causes the Node to transition to candidate state and start
     * campaigning to become leader.
     */
    campaign(): Promise<NullableError>;

    /**
     * Propose proposes that data be appended to the log. Note that proposals
     * can be lost without notice, therefore it is user's job to ensure proposal
     * retries.
     */
    propose(data: RaftData): Promise<NullableError>;

    /**
     * ProposeConfChange proposes a configuration change. Like any proposal, the
     * configuration change may be dropped with or without an error being
     * returned. In particular, configuration changes are dropped unless the
     * leader has certainty that there is no prior unapplied configuration
     * change in its log.
     *
     * The method accepts a ConfChangeI, which is an abstraction over a V1
     * ConfChange (deprecated) and V2 ConfChange (deprecated) message. The
     * latter allows arbitrary configuration changes via joint consensus,
     * notably including replacing a voter. Passing a ConfChangeI.asV2() message
     * is only allowed if all Nodes participating in the cluster run a version
     * of this library aware of the V2 API. See ConfChangeV2 for usage details
     * and semantics.
     */
    proposeConfChange(cc: ConfChangeI): Promise<NullableError>;

    /**
     * Step advances the state machine using the given message. Error will be
     * returned, if any.
     */
    step(msg: Message): Promise<NullableError>;

    /**
     * Ready returns a channel that returns the current point-in-time state.
     * Users of the Node must call advance() after retrieving the state returned
     * by ready(). NOTE: No committed entries from the next Ready may be applied
     * until all committed entries and snapshots from the previous one have
     * finished.
     */
    ready(): ReadChannel<Ready>;

    /**
     * Advance notifies the Node that the application has saved progress up to
     * the last Ready. It prepares the node to return the next available Ready.
     * The application should generally call Advance after it applies the
     * entries in last Ready.
     *
     * However, as an optimization, the application may call Advance while it is
     * applying the commands. For example. when the last Ready contains a
     * snapshot, the application might take a long time to apply the snapshot
     * data. To continue receiving Ready without blocking raft progress, it can
     * call Advance before finishing applying the last ready.
     */
    advance(): Promise<void>;

    /**
     * ApplyConfChange applies a config change (previously passed to
     * ProposeConfChange) to the node. This must be called whenever a config
     * change is observed in Ready.CommittedEntries, except when the app decides
     * to reject the configuration change (i.e. treats it as a noop instead), in
     * which case it must not be called. Returns an opaque non-nil ConfState
     * protobuf which must be recorded in snapshots.
     */
    applyConfChange(cc: ConfChangeI): Promise<ConfState | null>;

    /**
     * TransferLeadership attempts to transfer leadership to the given
     * transferee.
     */
    transferLeadership(lead: rid, transferee: rid): Promise<void>;

    /**
     * ReadIndex request a read state. The read state will be set in the ready.
     * Read state has a read index. Once the application advances further than
     * the read index, any linearizable read requests issued before the read
     * request can be processed safely. The read state will have the same rctx
     * attached. Note that request can be lost without notice, therefore it is
     * user's job to ensure read index retries.
     */
    readIndex(rctx: RaftData): Promise<NullableError>;

    /**
     * Status returns the current status of the raft state machine.
     */
    status(): Promise<Status>;

    /**
     * ReportUnreachable reports the given node is not reachable for the last
     * send.
     */
    reportUnreachable(id: rid): Promise<void>;

    /**
     * ReportSnapshot reports the status of the sent snapshot. The id is the
     * raft ID of the follower who is meant to receive the snapshot, and the
     * status is SnapshotFinish or SnapshotFailure. Calling ReportSnapshot with
     * SnapshotFinish is a no-op. But, any failure in applying a snapshot (for
     * e.g., while streaming it from leader to follower), should be reported to
     * the leader with SnapshotFailure. When leader sends a snapshot to a
     * follower, it pauses any raft log probes until the follower can apply the
     * snapshot and advance its state. If the follower can't do that, for e.g.,
     * due to a crash, it could end up in a limbo, never getting any updates
     * from the leader. Therefore, it is crucial that the application ensures
     * that any failure in snapshot sending is caught and reported back to the
     * leader; so it can resume raft log probing in the follower.
     */
    reportSnapshot(id: rid, status: SnapshotStatus): Promise<void>;

    /**
     * Stop performs any necessary termination of the Node.
     */
    stop(): Promise<void>;
}

export class Peer {
    id: rid;
    context: RaftData;

    constructor(
        param: {
            id?: rid;
            context?: RaftData;
        } = {}
    ) {
        this.id = param?.id ?? ridnone;
        this.context = param?.context ?? emptyRaftData;
    }
}

/**
 * StartNode returns a new Node given configuration and a list of raft peers. It
 * appends a ConfChangeAddNode entry for each given peer to the initial log.
 *
 * Peers must not be zero length; call RestartNode in that case.
 */
export function startNode(c: RaftConfig, peers: Peer[]): Node {
    if (peers.length === 0) {
        throw new Error("no peers given; use RestartNode instead");
    }
    const rn = RawNode.NewRawNode(c);

    const err = Bootstrap(rn, peers);
    if (err) {
        c.logger?.warningf("error occurred during starting a new node: %s", err);
    }

    const n = Node.NewNode(rn);

    n.run();
    return n;
}

/**
 * RestartNode is similar to StartNode but does not take a list of peers. The
 * current membership of the cluster will be restored from the Storage. If the
 * caller has an existing state machine, pass in the last log index that has
 * been applied to it; otherwise use zero.
 */
export function restartNode(c: RaftConfig): Node {
    const rn = RawNode.NewRawNode(c);
    const n = Node.NewNode(rn);
    n.run();
    return n;
}

/**
 * Do not export this interface, since it is internal.
 */
export class MsgWithResult {
    msg: Message;
    // This implementation makes the following channel nullable.
    result: ReadWriteChannel<NullableError> | null;

    constructor(
        param: {
            msg?: Message;
            result?: ReadWriteChannel<NullableError>;
        } = {}
    ) {
        this.msg = param?.msg ?? new Message();
        this.result = param?.result ?? null;
    }
}

/**
 * Node class is the canonical implementation of the Node interface
 */
export class Node implements Node {
    private _propc: ReadWriteChannel<MsgWithResult>;
    private _recvc: ReadWriteChannel<Message>;
    private _confc: ReadWriteChannel<ConfChangeV2>;
    private _confstatec: ReadWriteChannel<ConfState>;
    private _readyc: ReadWriteChannel<Ready>;
    private _advancec: ReadWriteChannel<{}>;
    private _tickc: ReadWriteChannel<{}>;
    private _done: ReadWriteChannel<{}>;
    private _stop: ReadWriteChannel<{}>;
    private _status: ReadWriteChannel<ReadWriteChannel<Status>>;

    // In our implementation the rn property must always be set, to avoid
    // unnecessary force unwrapping.
    private rn: RawNode;

    // In our implementation we force these channels to always be non null
    public constructor(param: {
        propc: ReadWriteChannel<MsgWithResult>;
        recvc: ReadWriteChannel<Message>;
        confc: ReadWriteChannel<ConfChangeV2>;
        confstatec: ReadWriteChannel<ConfState>;
        readyc: ReadWriteChannel<Ready>;
        advancec: ReadWriteChannel<{}>;
        tickc: ReadWriteChannel<{}>;
        done: ReadWriteChannel<{}>;
        stop: ReadWriteChannel<{}>;
        status: ReadWriteChannel<ReadWriteChannel<Status>>;
        rn: RawNode;
    }) {
        this._propc = param.propc;
        this._recvc = param.recvc;
        this._confc = param.confc;
        this._confstatec = param.confstatec;
        this._readyc = param.readyc;
        this._advancec = param.advancec;
        this._tickc = param.tickc;
        this._done = param.done;
        this._stop = param.stop;
        this._status = param.status;
        this.rn = param.rn;
    }

    static NewNode(rn: RawNode): Node {
        return new Node({
            propc: new Channel<MsgWithResult>(),
            recvc: new Channel<Message>(),
            confc: new Channel<ConfChangeV2>(),
            confstatec: new Channel<ConfState>(),
            readyc: new Channel<Ready>(),
            advancec: new Channel<{}>(),
            // make tickc a buffered chan, so raft node can buffer some ticks
            // when the node is busy processing raft messages. Raft node will
            // resume process buffered ticks when it becomes idle.
            tickc: new Channel<{}>(128),
            done: new Channel<{}>(),
            stop: new Channel<{}>(),
            status: new Channel<ReadWriteChannel<Status>>(),
            rn: rn,
        });
    }

    // NOTE: Used for testing inside node.test.ts
    public getRecvChannel(): ReadWriteChannel<Message> {
        return this._recvc;
    }
    public setRecvChannel(recvc: ReadWriteChannel<Message>) {
        this._recvc = recvc;
    }
    public getPropChannel(): ReadWriteChannel<MsgWithResult> {
        return this._propc;
    }
    public setPropChannel(propc: ReadWriteChannel<MsgWithResult>) {
        this._propc = propc;
    }
    public getDoneChannel(): ReadWriteChannel<{}> {
        return this._done;
    }
    public setDoneChannel(done: ReadWriteChannel<{}>) {
        this._done = done;
    }

    public async stop() {
        switch (await Channel.select([this._stop.push({}), this._done.shift()])) {
            case this._stop:
                // Not already stopped, so trigger it
                break;
            case this._done:
                // Node has already been stopped - no need to do anything
                return;
        }
        // Block until the stop has been acknowledged by run()
        await this._done.shift();
    }

    // NOTE: needs to be public because of startNode()
    public async run() {
        let propc: ReadWriteChannel<MsgWithResult> | null = null;
        let readyc: ReadWriteChannel<Ready> | null = null;
        let advancec: ReadWriteChannel<{}> | null = null;
        let rd = new Ready();

        const r: Raft = this.rn.raft;
        let lead = ridnone;

        while (true) {
            if (advancec !== null) {
                readyc = null;
            } else if (this.rn.hasReady()) {
                // Populate a Ready. Note that this Ready is not guaranteed to
                // actually be handled. We will arm readyc, but there's no
                // guarantee that we will actually send on it. It's possible
                // that we will service another channel instead, loop around,
                // and then populate the Ready again. We could instead force the
                // previous Ready to be handled first, but it's generally good
                // to emit larger Readys plus it simplifies testing (by emitting
                // less frequently and more predictably).
                rd = this.rn.readyWithoutAccept();
                readyc = this._readyc;
            }
            if (lead !== r.lead) {
                if (r.hasLeader()) {
                    if (lead === ridnone) {
                        r.logger.infof(
                            "raft.node: %s elected leader %s at term %d",
                            r.id,
                            r.lead,
                            r.term
                        );
                    } else {
                        r.logger.infof(
                            "raft.node: %s changed leader from %s to %s at term %d",
                            r.id,
                            lead,
                            r.lead,
                            r.term
                        );
                    }
                    propc = this._propc;
                } else {
                    r.logger.infof("raft.node: %s lost leader %s at term %d", r.id, lead, r.term);
                    propc = null;
                }
                lead = r.lead;
            }

            switch (
            await Channel.select([
                // RaftGo-TODO: Maybe buffer the config propose if there exists
                // one (the way described in raft dissertation) Currently it is
                // dropped in Step silently.
                (propc ?? new Channel<MsgWithResult>()).shift(),
                this._recvc.shift(),
                this._confc.shift(),
                this._tickc.shift(),
                (readyc ?? new Channel<Ready>()).push(rd),
                (advancec ?? new Channel<{}>()).shift(),
                this._status.shift(),
                this._stop.shift(),
            ])
            ) {
                case propc:
                    const pm = propc!.value();
                    const m1 = pm.msg;
                    m1.from = r.id;
                    const err = r.Step(m1);
                    if (pm.result !== null) {
                        await pm.result.push(err);
                        pm.result.close();
                    }
                    break;
                case this._recvc:
                    const m2 = this._recvc.value();
                    // filter out response message from unknown From.
                    if (r.prs.progress.get(m2.from) !== undefined || !isResponseMsg(m2.type)) {
                        r.Step(m2);
                    }
                    break;
                case this._confc:
                    const cc = this._confc.value();
                    const okBefore = r.prs.progress.get(r.id) !== undefined;
                    // NOTE: Error is ignored here
                    const [cs, _] = r.applyConfChange(cc);
                    // If the node was removed, block incoming proposals. Note
                    // that we only do this if the node was in the config
                    // before. Nodes may be a member of the group without
                    // knowing this (when they're catching up on the log and
                    // don't have the latest config) and we don't want to block
                    // the proposal channel in that case.
                    //
                    // NB: propc is reset when the leader changes, which, if we
                    // learn about it, sort of implies that we got readded,
                    // maybe? This isn't very sound and likely has bugs.
                    const okAfter = r.prs.progress.get(r.id) !== undefined;
                    if (okBefore && !okAfter) {
                        let found = false;
                        outer: for (const sl of [cs.voters, cs.votersOutgoing]) {
                            for (const id of sl) {
                                if (id === r.id) {
                                    found = true;
                                    break outer;
                                }
                            }
                        }
                        if (!found) {
                            propc = null;
                        }
                    }
                    switch (await Channel.select([this._confstatec.push(cs), this._done.shift()])) {
                        case this._confstatec:
                            break;
                        case this._done:
                            break;
                    }
                    break;
                case this._tickc:
                    this.rn.tick();
                    break;
                case readyc:
                    this.rn.acceptReady(rd);
                    advancec = this._advancec;
                    break;
                case advancec:
                    this.rn.advance(rd);
                    rd = new Ready();
                    advancec = null;
                    break;
                case this._status:
                    const c = this._status.value();
                    await c.push(Status.getStatus(r));
                    break;
                case this._stop:
                    this._done.close();
                    return;
            }
        }
    }

    /**
     * Tick increments the internal logical clock for this Node. Election
     * timeouts and heartbeat timeouts are in units of ticks.
     */
    public async tick() {
        const closedDefault = Channel();
        closedDefault.close();
        switch (
        await Channel.select([this._tickc.push({}), this._done.shift(), closedDefault.shift()])
        ) {
            case this._tickc:
                break;
            case this._done:
                break;
            default:
                this.rn.raft.logger.warningf(
                    "raft.node: A tick missed to fire. Node %s blocks too long!",
                    this.rn.raft.id
                );
        }
    }

    public async campaign(): Promise<NullableError> {
        return this._step(
            new Message({
                type: MessageType.MsgHup,
            })
        );
    }

    public async propose(data: RaftData): Promise<NullableError> {
        return this._stepWait(
            new Message({
                type: MessageType.MsgProp,
                entries: [
                    new Entry({
                        data: data,
                    }),
                ],
            })
        );
    }

    public async step(m: Message): Promise<NullableError> {
        // ignore unexpected local messages receiving over network
        if (isLocalMsg(m.type)) {
            // RaftGo-TODO: return an error?
            return null;
        }
        return this._step(m);
    }

    public async proposeConfChange(cc: ConfChangeI): Promise<NullableError> {
        const msg = confChangeToMsg(cc);
        return this._step(msg);
    }

    public ready(): ReadWriteChannel<Ready> {
        return this._readyc;
    }

    public async advance() {
        switch (
        await Channel.select([this._advancec.push({}), this._done.shift()])
        // There are no statements in the cases here
        ) {
        }
    }

    public async applyConfChange(cc: ConfChangeI): Promise<ConfState> {
        let cs: ConfState;
        switch (
        await Channel.select([this._confc.push(cc.asV2()), this._done.shift()])
        // Cases are empty here
        ) {
        }
        return new Promise<ConfState>(async (resolve) => {
            switch (await Channel.select([this._confstatec.shift(), this._done.shift()])) {
                case this._confstatec:
                    cs = this._confstatec.value();
                    resolve(cs);
            }
        });
    }

    public async status(): Promise<Status> {
        const c = new Channel<Status>();
        return new Promise<Status>(async (resolve) => {
            switch (await Channel.select([this._status.push(c), this._done.shift()])) {
                case this._status:
                    resolve(await c.shift());
                    break;
                case this._done:
                    resolve(new Status());
                    break;
            }
        });
    }

    public async reportUnreachable(id: rid) {
        switch (
        await Channel.select([
            this._recvc.push(new Message({ type: MessageType.MsgUnreachable, from: id })),
            this._done.shift(),
        ])
        ) {
            case this._recvc:
                break;
            case this._done:
                break;
        }
    }

    public async reportSnapshot(id: rid, status: SnapshotStatus) {
        const rej = status === SnapshotStatus.SnapshotFailure;
        switch (
        await Channel.select([
            this._recvc.push(
                new Message({
                    type: MessageType.MsgSnapStatus,
                    from: id,
                    reject: rej,
                })
            ),
            this._done.shift(),
        ])
        ) {
            case this._recvc:
                break;
            case this._done:
                break;
        }
    }

    public async transferLeadership(lead: rid, transferee: rid) {
        switch (
        await Channel.select([
            this._recvc.push(
                new Message({
                    type: MessageType.MsgTransferLeader,
                    from: transferee,
                    to: lead,
                })
            ),
            this._done.shift(),
        ])
        ) {
            case this._recvc:
                break;
            case this._done:
                break;
        }
    }

    public async readIndex(rctx: RaftData): Promise<NullableError> {
        return this.step(
            new Message({
                type: MessageType.MsgReadIndex,
                entries: [new Entry({ data: rctx })],
            })
        );
    }

    private async _step(m: Message): Promise<NullableError> {
        return this._stepWithWaitOption(m, false);
    }

    private async _stepWait(m: Message): Promise<NullableError> {
        return this._stepWithWaitOption(m, true);
    }

    /**
     * Step advances the state machine using msgs. The ctx.Err() will be
     * returned, if any.
     */
    private async _stepWithWaitOption(m: Message, wait: boolean): Promise<NullableError> {
        if (m.type !== MessageType.MsgProp) {
            switch (
            await Channel.select([
                this._recvc.push(m),
                // ctx is omitted
                this._done.shift(),
            ])
            ) {
                case this._recvc:
                    return null;
                case this._done:
                    return errStopped;
            }
        }

        const ch = this._propc;
        const pm = new MsgWithResult({ msg: m });

        if (wait) {
            pm.result = new Channel<Error>(1);
        }

        switch (
        await Channel.select([
            ch.push(pm),
            // ctx is omitted
            this._done.shift(),
        ])
        ) {
            case ch:
                if (!wait) {
                    return null;
                }
                break;
            case this._done:
                return errStopped;
        }

        switch (
        await Channel.select([
            // Force unwrapping here is safe
            pm.result!.shift(),
            // ctx is omitted
            this._done.shift(),
        ])
        ) {
            case pm.result:
                const err = pm.result!.value();
                if (err !== null) {
                    return err;
                }
                break;
            case this._done:
                return errStopped;
        }

        return null;
    }
}

export function newReady(r: Raft, prevSoftSt: SoftState | null, prevHardSt: HardState): Ready {
    return new Ready({
        entries: r.raftLog.unstableEntries(),
        committedEntries: r.raftLog.nextEnts(),
        messages: r.msgs,
        softState: !r.softState().equal(prevSoftSt) ? r.softState() : undefined,
        hardState: !r.hardState().isHardStateEqual(prevHardSt) ? r.hardState() : undefined,
        snapshot: r.raftLog.unstable.snapshot ?? undefined,
        readStates: r.readStates.length !== 0 ? r.readStates : undefined,
        mustSync: mustSync(r.hardState(), prevHardSt, r.raftLog.unstableEntries().length),
    });
}

export function confChangeToMsg(c: ConfChangeI): Message {
    // NOTE: marshalConfChange does not return an error in this version, should
    // this change this error value should be propagated by this function too
    const [typ, data] = marshalConfChange(c);
    const e: Entry[] = [new Entry({ type: typ, data: data })];
    const msg: Message = new Message({ type: MessageType.MsgProp, entries: e });
    return msg;
}

/**
 * MustSync returns true if the hard state and count of Raft entries indicate
 * that a synchronous write to persistent storage is required.
 */
export function mustSync(st: HardState, prevst: HardState, entsnum: number): boolean {
    // Persistent state on all servers: (Updated on stable storage before
    // responding to RPCs) currentTerm votedFor log entries[]
    return entsnum !== 0 || st.vote !== prevst.vote || st.term !== prevst.term;
}

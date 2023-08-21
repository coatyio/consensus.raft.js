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

import { Changer } from "./confchange/confchange";
import { Restore } from "./confchange/restore";
import { newLogWithSize, RaftLog } from "./log";
import { getLogger, Logger } from "./logger";
import { Ready, SoftState } from "./node";
import { VoteResult } from "./quorum/quorum";
import { ReadOnly, ReadState } from "./readonly";
import { ErrSnapshotTemporarilyUnavailable, Storage } from "./storage";
import { NewInflights } from "./tracker/inflights";
import { Progress, ProgressMap } from "./tracker/progress";
import { ProgressStateType } from "./tracker/state";
import { Config, MakeProgressTracker, ProgressTracker } from "./tracker/tracker";
import { assertConfStatesEquivalent, NullableError, payloadSize, voteRespMsgType } from "./util";
import { emptyRaftData } from "../non-ported/raft-controller";
import { RaftData } from "../non-ported/raft-controller";
import {
    HardState,
    Message,
    ConfState,
    ConfChangeV2,
    MessageType,
    Snapshot,
    Entry,
    EntryType,
    ConfChangeI,
    ConfChange,
    rid,
    ridnone,
} from "./raftpb/raft.pb";

/**
 * Highest integer in TypeScript that is guaranteed to be represented accurately
 */
export const noLimit = Number.MAX_SAFE_INTEGER;

export enum ReadOnlyOption {
    ReadOnlySafe,
    ReadOnlyLeaseBased,
}

export const errProposalDropped = new Error("raft proposal dropped");

/**
 * StateType represents the role of a node in a cluster.
 */
export enum StateType {
    StateFollower,
    StateCandidate,
    StateLeader,
    StatePreCandidate,
    numStates,
}

export enum CampaignType {
    /**
     * campaignPreElection represents the first phase of a normal election when
     * Config.PreVote is true.
     */
    campaignPreElection = "CampaignPreElection",

    /**
     * campaignElection represents a normal (time-based) election (the second
     * phase of the election when Config.PreVote is true).
     */
    campaignElection = "CampaignElection",

    /**
     * campaignTransfer represents the type of leader transfer
     */
    campaignTransfer = "CampaignTransfer",
}

export class RaftConfig {
    /**
     * ID is the identity of the local raft. ID cannot be 0.
     */
    id: rid;

    /**
     * ElectionTick is the number of Node.Tick invocations that must pass
     * between elections. That is, if a follower does not receive any message
     * from the leader of current term before ElectionTick has elapsed, it will
     * become candidate and start an election. ElectionTick must be greater than
     * HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
     * unnecessary leader switching.
     */
    electionTick: number;

    /**
     * HeartbeatTick is the number of Node.Tick invocations that must pass
     * between heartbeats. That is, a leader sends heartbeat messages to
     * maintain its leadership every HeartbeatTick ticks.
     */
    heartbeatTick: number;

    /**
     * Storage is the storage for raft. raft generates entries and states to be
     * stored in storage. raft reads the persisted entries and states out of
     * Storage when it needs. raft reads out the previous state and
     * configuration out of storage when restarting.
     */
    storage: Storage | null;

    /**
     * Applied is the last applied index. It should only be set when restarting
     * raft. raft will not return entries to the application smaller or equal to
     * Applied. If Applied is unset when restarting, raft might return previous
     * applied entries. This is a very application dependent configuration.
     */
    applied: number;

    /**
     * MaxSizePerMsg limits the max byte size of each append message. Smaller
     * value lowers the raft recovery cost(initial probing and message lost
     * during normal operation). On the other side, it might affect the
     * throughput during normal replication. Note: math.MaxUint64 for unlimited,
     * 0 for at most one entry per message.
     */
    maxSizePerMsg: number;

    /**
     * MaxCommittedSizePerReady limits the size of the committed entries which
     * can be applied.
     */
    maxCommittedSizePerReady: number;

    /**
     * MaxUncommittedEntriesSize limits the aggregate byte size of the
     * uncommitted entries that may be appended to a leader's log. Once this
     * limit is exceeded, proposals will begin to return ErrProposalDropped
     * errors. Note: 0 for no limit.
     */
    maxUncommittedEntriesSize: number;

    /**
     * MaxInflightMsgs limits the max number of in-flight append messages during
     * optimistic replication phase. The application transportation layer
     * usually has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs
     * to avoid overflowing that sending buffer. RaftGo-TODO(xiangli): feedback
     * to application to limit the proposal rate?
     */
    maxInflightMsgs: number;

    /**
     * CheckQuorum specifies if the leader should check quorum activity. Leader
     * steps down when quorum is not active for an electionTimeout.
     */
    checkQuorum: boolean;

    /**
     * PreVote enables the Pre-Vote algorithm described in raft thesis section
     * 9.6. This prevents disruption when a node that has been partitioned away
     * rejoins the cluster.
     */
    preVote: boolean;

    /**
     * ReadOnlyOption specifies how the read only request is processed.
     *
     * ReadOnlySafe guarantees the linearizability of the read only request by
     * communicating with the quorum. It is the default and suggested option.
     *
     * ReadOnlyLeaseBased ensures linearizability of the read only request by
     * relying on the leader lease. It can be affected by clock drift. If the
     * clock drift is unbounded, leader might keep the lease longer than it
     * should (clock can move backward/pause without any bound). ReadIndex is
     * not safe in that case. CheckQuorum MUST be enabled if ReadOnlyOption is
     * ReadOnlyLeaseBased.
     */
    readOnlyOption: ReadOnlyOption;

    /**
     * Logger is the logger used for raft log. For multinode which can host
     * multiple raft group, each raft group can have its own logger.
     */
    logger: Logger | null;

    /**
     * DisableProposalForwarding set to true means that followers will drop
     * proposals, rather than forwarding them to the leader. One use case for
     * this feature would be in a situation where the Raft leader is used to
     * compute the data of a proposal, for example, adding a timestamp from a
     * hybrid logical clock to data in a monotonically increasing way.
     * Forwarding should be disabled to prevent a follower with an inaccurate
     * hybrid logical clock from assigning the timestamp and then forwarding the
     * data to the leader.
     */
    disableProposalForwarding: boolean;

    constructor(
        param: {
            id?: rid;
            electionTick?: number;
            heartbeatTick?: number;
            storage?: Storage;
            applied?: number;
            maxSizePerMsg?: number;
            maxCommittedSizePerReady?: number;
            maxUncommittedEntriesSize?: number;
            maxInflightMsgs?: number;
            checkQuorum?: boolean;
            preVote?: boolean;
            readOnlyOption?: ReadOnlyOption;
            logger?: Logger;
            disableProposalForwarding?: boolean;
        } = {}
    ) {
        this.id = param.id ?? ridnone;
        this.electionTick = param.electionTick ?? 0;
        this.heartbeatTick = param.heartbeatTick ?? 0;
        this.storage = param.storage ?? null;
        this.applied = param.applied ?? 0;
        this.maxSizePerMsg = param.maxSizePerMsg ?? 0;
        this.maxCommittedSizePerReady = param.maxCommittedSizePerReady ?? 0;
        this.maxUncommittedEntriesSize = param.maxUncommittedEntriesSize ?? 0;
        this.maxInflightMsgs = param.maxInflightMsgs ?? 0;
        this.checkQuorum = param.checkQuorum ?? false;
        this.preVote = param.preVote ?? false;
        this.readOnlyOption = param.readOnlyOption ?? ReadOnlyOption.ReadOnlySafe;
        this.logger = param.logger ?? null;
        this.disableProposalForwarding = param.disableProposalForwarding ?? false;
    }

    validate(): NullableError {
        if (this.id === ridnone) {
            return new Error("cannot use none as id");
        }

        if (this.heartbeatTick <= 0) {
            return new Error("heartbeat tick must be greater than 0");
        }

        if (this.electionTick <= this.heartbeatTick) {
            return new Error("election tick must be greater than heartbeat tick");
        }

        if (this.storage === null) {
            return new Error("storage cannot be nil");
        }

        if (this.maxUncommittedEntriesSize === 0) {
            this.maxUncommittedEntriesSize = noLimit;
        }

        // default MaxCommittedSizePerReady to MaxSizePerMsg because they were
        // previously the same parameter.
        if (this.maxCommittedSizePerReady === 0) {
            this.maxCommittedSizePerReady = this.maxSizePerMsg;
        }

        if (this.maxInflightMsgs <= 0) {
            return new Error("max inflight messages must be greater than 0");
        }

        if (this.logger === null) {
            this.logger = getLogger();
        }

        if (this.readOnlyOption === ReadOnlyOption.ReadOnlyLeaseBased && !this.checkQuorum) {
            return new Error(
                "CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased"
            );
        }

        return null;
    }
}

export class Raft {
    id: rid;

    term: number;
    vote: rid;

    readStates: ReadState[];

    /**
     * the log
     */
    raftLog: RaftLog;

    maxMsgSize: number;
    maxUncommittedSize: number;
    // RaftGo-TODO(tbg): rename to trk.
    prs: ProgressTracker;

    state: StateType;

    /**
     * isLearner is true if the local raft node is a learner.
     */
    isLearner: boolean;

    msgs: Message[];

    /**
     * the leader id
     */
    lead: rid;

    /**
     * leadTransferee is id of the leader transfer target when its value is not
     * zero. Follow the procedure defined in raft thesis 3.10.
     */
    leadTransferee: rid;

    /**
     * Only one conf change may be pending (in the log, but not yet applied) at
     * a time. This is enforced via pendingConfIndex, which is set to a value >=
     * the log index of the latest pending configuration change (if any). Config
     * changes are only allowed to be proposed if the leader's applied index is
     * greater than this value.
     */
    pendingConfIndex: number;

    /**
     * an estimate of the size of the uncommitted tail of the Raft log. Used to
     * prevent unbounded log growth. Only maintained by the leader. Reset on
     * term changes.
     */
    uncommittedSize: number;

    readOnly: ReadOnly;

    /**
     * number of ticks since it reached last electionTimeout when it is leader
     * or candidate. number of ticks since it reached last electionTimeout or
     * received a valid message from current leader when it is a follower.
     */
    electionElapsed: number;

    /**
     * number of ticks since it reached last heartbeatTimeout. only leader keeps
     * heartbeatElapsed.
     */
    heartbeatElapsed: number;

    checkQuorum: boolean;
    preVote: boolean;

    heartbeatTimeout: number;
    electionTimeout: number;

    /**
     * randomizedElectionTimeout is a random number between [electionTimeout, 2
     * times electionTimeout - 1]. It gets reset when raft changes its state to
     * follower or candidate.
     */
    randomizedElectionTimeout: number;
    disableProposalForwarding: boolean;
    logger: Logger;

    /**
     * pendingReadIndexMessages is used to store messages of type MsgReadIndex
     * that can't be answered as new leader didn't committed any log in current
     * term. Those will be handled as fast as first log is committed in current
     * term.
     */
    pendingReadIndexMessages: Message[];

    tick: () => void;

    step: StepFunc;

    constructor(param: {
        // NOTE: The raftLog parameter is not optional. If not specified it
        // would have to default to "null" because it is modeled as pointer in
        // the go implementation. That would require a lot of unwrapping inside
        // this class which we want to avoid. Same goes for logger and readOnly.

        // Necessary / not optional
        raftLog: RaftLog;
        logger: Logger;
        readOnly: ReadOnly;

        // optional
        id?: rid;
        lead?: rid;
        isLearner?: boolean;
        maxMsgSize?: number;
        maxUncommittedSize?: number;
        prs?: ProgressTracker;
        heartbeatTimeout?: number;
        electionTimeout?: number;
        checkQuorum?: boolean;
        preVote?: boolean;
        disableProposalForwarding?: boolean;
        term?: number;
        vote?: rid;
        readStates?: ReadState[];
        state?: StateType;
        msgs?: Message[];
        leadTransferee?: rid;
        pendingConfIndex?: number;
        uncommittedSize?: number;
        electionElapsed?: number;
        heartbeatElapsed?: number;
        randomizedElectionTimeout?: number;
        tick?: () => void;
        step?: StepFunc;
        pendingReadIndexMessages?: Message[];
    }) {
        // Necessary / not optional
        this.raftLog = param.raftLog;
        this.logger = param.logger;
        this.readOnly = param.readOnly;

        // optional
        this.id = param.id ?? ridnone;
        this.lead = param.lead ?? ridnone;
        this.isLearner = param.isLearner ?? false;
        this.maxMsgSize = param.maxMsgSize ?? 0;
        this.maxUncommittedSize = param.maxUncommittedSize ?? 0;
        this.prs = param.prs ?? new ProgressTracker();
        this.heartbeatTimeout = param.heartbeatTimeout ?? 0;
        this.electionTimeout = param.electionTimeout ?? 0;
        this.checkQuorum = param.checkQuorum ?? false;
        this.preVote = param.preVote ?? false;
        this.disableProposalForwarding = param.disableProposalForwarding ?? false;
        this.term = param.term ?? 0;
        this.vote = param.vote ?? ridnone;
        this.readStates = param.readStates ?? [];
        this.state = param.state ?? StateType.StateFollower;
        this.msgs = param.msgs ?? [];
        this.leadTransferee = param.leadTransferee ?? ridnone;
        this.pendingConfIndex = param.pendingConfIndex ?? 0;
        this.uncommittedSize = param.uncommittedSize ?? 0;
        this.electionElapsed = param.electionElapsed ?? 0;
        this.heartbeatElapsed = param.heartbeatElapsed ?? 0;
        this.randomizedElectionTimeout = param.randomizedElectionTimeout ?? 0;
        this.tick = param.tick!;
        this.step = param.step!;
        this.pendingReadIndexMessages = param.pendingReadIndexMessages ?? [];
    }

    static NewRaft(config: RaftConfig): Raft {
        const configError = config.validate();
        if (configError !== null) {
            throw configError;
        }
        // NOTE: Force unwrapping is now possible since config.validate()
        // succeeded
        const raftLog = newLogWithSize(
            config.storage!,
            config.logger!,
            config.maxCommittedSizePerReady
        );
        const [hState, cState, _error] = config.storage!.InitialState();

        if (_error !== null) {
            throw new Error(_error.message);
        }
        const r = new Raft({
            id: config.id,
            lead: ridnone,
            isLearner: false,
            raftLog: raftLog,
            maxMsgSize: config.maxSizePerMsg,
            maxUncommittedSize: config.maxUncommittedEntriesSize,
            prs: MakeProgressTracker(config.maxInflightMsgs),
            electionTimeout: config.electionTick,
            heartbeatTimeout: config.heartbeatTick,
            logger: config.logger!,
            checkQuorum: config.checkQuorum,
            preVote: config.preVote,
            readOnly: ReadOnly.NewReadOnly(config.readOnlyOption),
            disableProposalForwarding: config.disableProposalForwarding,
        });

        const [conf, progMap, err] = Restore(new Changer(r.prs, raftLog.lastIndex()), cState);

        if (err !== null) {
            throw err;
        }
        assertConfStatesEquivalent(r.logger, cState, r.switchToConfig(conf, progMap));

        if (!hState.isEmptyHardState()) {
            r.loadState(hState);
        }
        if (config.applied > 0) {
            raftLog.appliedTo(config.applied);
        }

        r.becomeFollower(r.term, ridnone);

        const nodesStrs: string[] = [];
        r.prs.voterNodes().forEach((value) => {
            nodesStrs.push(`${value}`);
        });

        r.logger.infof(
            "newRaft %s [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
            r.id,
            nodesStrs.join(","),
            r.term,
            r.raftLog.committed,
            r.raftLog.applied,
            r.raftLog.lastIndex(),
            r.raftLog.lastTerm()
        );

        return r;
    }

    hasLeader(): boolean {
        return this.lead !== ridnone;
    }

    softState(): SoftState {
        return new SoftState({
            lead: this.lead,
            raftState: this.state,
        });
    }

    hardState(): HardState {
        return new HardState({
            term: this.term,
            vote: this.vote,
            commit: this.raftLog.committed,
        });
    }

    /**
     * send schedules persisting state to a stable storage and AFTER that
     * sending the message (as part of next Ready message processing).
     */
    send(m: Message) {
        if (m.from === ridnone) {
            m.from = this.id;
        }
        if (
            m.type === MessageType.MsgVote ||
            m.type === MessageType.MsgVoteResp ||
            m.type === MessageType.MsgPreVote ||
            m.type === MessageType.MsgPreVoteResp
        ) {
            if (m.term === 0) {
                // All {pre-,}campaign messages need to have the term set when
                // sending.
                // - MsgVote: m.Term is the term the node is campaigning for,
                //   non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                //   granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.Term is the term the node will campaign,
                //   non-zero as we use m.Term to indicate the next term we'll
                //   be campaigning for
                // - MsgPreVoteResp: m.Term is the term received in the original
                //   MsgPreVote if the pre-vote was granted, non-zero for the
                //   same reasons MsgPreVote is
                throw new Error(
                    `term should be set when sending ${MessageType[m.type]}, term: ${m.term}`
                );
            }
        } else {
            if (m.term && m.term !== 0) {
                throw new Error(`term should not be set when sending ${m.type} (was ${m.term})`);
            }
            // do not attach term to MsgProp, MsgReadIndex proposals are a way
            // to forward to the leader and should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            if (m.type !== MessageType.MsgProp && m.type !== MessageType.MsgReadIndex) {
                m.term = this.term;
            }
        }

        this.msgs.push(m);
    }

    /**
     * sendAppend sends an append RPC with new entries (if any) and the current
     * commit index to the given peer.
     */
    sendAppend(to: rid) {
        this.maybeSendAppend(to, true);
    }

    /**
     * maybeSendAppend sends an append RPC with new entries to the given peer,
     * if necessary. Returns true if a message was sent. The sendIfEmpty
     * argument controls whether messages with no entries will be sent ("empty"
     * messages are useful to convey updated Commit indexes, but are undesirable
     * when we're sending multiple messages in a batch).
     */
    maybeSendAppend(to: rid, sendIfEmpty: boolean): boolean {
        const pr = this.prs.progress.get(to);
        if (pr === undefined) {
            // NOTE: Go implementation doesn't check for this case => We assume
            // that this should never happen
            throw new Error();
        }
        if (pr.IsPaused()) {
            return false;
        }
        const m = new Message();
        m.to = to;

        const [term, errT] = this.raftLog.term(pr.next - 1);
        const [ent, err] = this.raftLog.entries(pr.next, this.maxMsgSize);
        if (ent.length === 0 && !sendIfEmpty) {
            return false;
        }

        if (errT !== null || err !== null) {
            // send snapshot if we failed to get term or entries
            if (!pr.recentActive) {
                this.logger.debugf(
                    "ignore sending snapshot to %s since it is not recently active",
                    to
                );
                return false;
            }

            m.type = MessageType.MsgSnap;
            const snap = this.raftLog.snapshot();
            if (snap[1] !== null) {
                if (typeof snap[1] === typeof ErrSnapshotTemporarilyUnavailable) {
                    this.logger.debugf(
                        "%s failed to send snapshot to %s because snapshot is temporarily unavailable",
                        this.id,
                        to
                    );
                    return false;
                }
                throw new Error(snap[1].message);
            }
            if (snap[0].isEmptySnap()) {
                throw new Error("need non-empty snapshot");
            }
            m.snapshot = snap[0];
            const sindex = snap[0].metadata.index;
            const sterm = snap[0].metadata.term;
            this.logger.debugf(
                "%s [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %s [%o]",
                this.id,
                this.raftLog.firstIndex(),
                this.raftLog.committed,
                sindex,
                sterm,
                to,
                pr
            );
            pr.BecomeSnapshot(sindex);
            this.logger.debugf(
                "%s paused sending replication messages to %s [%o]",
                this.id,
                to,
                pr
            );
        } else {
            m.type = MessageType.MsgApp;
            m.index = pr.next - 1;
            m.logTerm = term;
            m.entries = ent;
            m.commit = this.raftLog.committed;
            const length = m.entries ? m.entries.length : 0;
            if (length !== 0) {
                switch (pr.state) {
                    // optimistically increase the next when in StateReplicate
                    case ProgressStateType.StateReplicate:
                        const last = m.entries[length - 1].index;
                        pr.OptimisticUpdate(last);
                        pr.inflights!.add(last);
                        break;
                    case ProgressStateType.StateProbe:
                        pr.probeSent = true;
                        break;
                    default:
                        this.logger.panicf(
                            "%s is sending append in unhandled state %o",
                            this.id,
                            pr.state
                        );
                }
            }
        }
        this.send(m);
        return true;
    }

    /**
     * sendHeartbeat sends a heartbeat RPC to the given peer.
     */
    sendHeartbeat(to: rid, ctx: RaftData) {
        // Attach the commit as min(to.matched, r.committed). When the leader
        // sends out heartbeat message, the receiver(follower) might not be
        // matched with the leader or it might not have all the committed
        // entries. The leader MUST NOT forward the follower's commit to an
        // unmatched index.
        const commit = Math.min(this.prs.progress.get(to)?.match ?? 0, this.raftLog.committed);
        const m = new Message({
            to: to,
            type: MessageType.MsgHeartbeat,
            commit: commit,
            context: ctx,
        });
        this.send(m);
    }

    /**
     * bcastAppend sends RPC, with entries to all peers that are not up-to-date
     * according to the progress recorded in r.prs.
     */
    bcastAppend() {
        this.prs.visit((id: rid, _: Progress) => {
            if (id === this.id) {
                return;
            }
            this.sendAppend(id);
        });
    }

    /**
     * bcastHeartbeat sends RPC, without entries to all the peers.
     */
    bcastHeartbeat() {
        const lastCtx = this.readOnly.lastPendingRequestCtx();
        if (lastCtx === emptyRaftData) {
            this.bcastHeartbeatWithCtx(emptyRaftData);
        } else {
            this.bcastHeartbeatWithCtx(lastCtx);
        }
    }

    bcastHeartbeatWithCtx(ctx: RaftData) {
        this.prs.visit((id: rid, _: Progress) => {
            if (id === this.id) {
                return;
            }
            this.sendHeartbeat(id, ctx);
        });
    }

    advance(rd: Ready) {
        this.reduceUncommittedSize(rd.committedEntries);

        // If entries were applied (or a snapshot), update our cursor for the
        // next Ready. Note that if the current HardState contains a new Commit
        // index, this does not mean that we're also applying all of the new
        // entries due to commit pagination by size.
        const newApplied = rd.appliedCursor();
        if (newApplied > 0) {
            const oldApplied = this.raftLog.applied;
            this.raftLog.appliedTo(newApplied);

            if (
                this.prs.config.autoLeave &&
                oldApplied <= this.pendingConfIndex &&
                newApplied >= this.pendingConfIndex &&
                this.state === StateType.StateLeader
            ) {
                // TODO make sure that null unmarshalls to empty ConfChangeV2
                // later

                // If the current (and most recent, at least for this leader's
                // term) configuration should be auto-left, initiate that now.
                // We use a nil Data which unmarshalls into an empty
                // ConfChangeV2 and has the benefit that appendEntry can never
                // refuse it based on its size (which registers as zero).
                const ent = new Entry({
                    type: EntryType.EntryConfChangeV2,
                    data: emptyRaftData,
                });
                // There's no way in which this proposal should be able to be
                // rejected.
                if (!this.appendEntry(ent)) {
                    throw new Error("refused un-refusable auto-leaving ConfChangeV2");
                }
                this.pendingConfIndex = this.raftLog.lastIndex();
                this.logger.infof("initiating automatic transition out of joint configuration %o", this.prs.config);
            }
        }

        if (rd.entries ? rd.entries.length : 0 > 0) {
            const e = rd.entries[rd.entries.length - 1];
            this.raftLog.stableTo(e.index, e.term);
        }

        if (!rd.snapshot.isEmptySnap()) {
            this.raftLog.stableSnapTo(rd.snapshot.metadata.index);
        }
    }

    /**
     * maybeCommit attempts to advance the commit index. Returns true if the
     * commit index changed (in which case the caller should call
     * r.bcastAppend).
     */
    maybeCommit(): boolean {
        const mci = this.prs.committed();
        return this.raftLog.maybeCommit(mci, this.term);
    }

    reset(term: number) {
        if (this.term !== term) {
            this.term = term;
            this.vote = ridnone;
        }
        this.lead = ridnone;

        this.electionElapsed = 0;
        this.heartbeatElapsed = 0;
        this.resetRandomizedElectionTimeout();

        this.abortLeaderTransfer();

        this.prs.resetVotes();
        this.prs.visit((id: rid, pr: Progress) => {
            pr.match = 0;
            pr.next = this.raftLog.lastIndex() + 1;
            pr.inflights = NewInflights(this.prs.maxInflight);
            pr.isLearner = pr.isLearner;
            // pr is a pointer in etcd/raft and is set to a newly initialized
            // progress. So set all other fields to the default values
            pr.state = ProgressStateType.StateProbe;
            pr.pendingSnapshot = 0;
            pr.recentActive = false;
            pr.probeSent = false;

            if (id === this.id) {
                pr.match = this.raftLog.lastIndex();
            }
        });

        this.pendingConfIndex = 0;
        this.uncommittedSize = 0;
        this.readOnly = ReadOnly.NewReadOnly(this.readOnly.option);
    }

    appendEntry(...es: Entry[]): boolean {
        let li = this.raftLog.lastIndex();
        es.forEach((value, index) => {
            value.term = this.term;
            value.index = li + 1 + index;
        });
        // Track the size of this uncommitted proposal.
        if (!this.increaseUncommittedSize(es)) {
            this.logger.debugf(
                "%s appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
                this.id,
            );
            // Drop the proposal.
            return false;
        }
        // use latest "last" index after truncate/append
        li = this.raftLog.append(...es);
        const p = this.prs.progress.get(this.id);
        if (p === undefined) {
            // NOTE: Go implementation doesn't check for this case => We assume
            // that this should never happen
            throw new Error();
        }
        p.MaybeUpdate(li);
        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        this.maybeCommit();
        return true;
    }

    /**
     * tickElection is run by followers and candidates after
     * this.electionTimeout.
     */
    tickElection() {
        this.electionElapsed++;

        if (this.promotable() && this.pastElectionTimeout()) {
            this.electionElapsed = 0;
            this.Step(new Message({ from: this.id, type: MessageType.MsgHup }));
        }
    }

    /**
     * tickHeartbeat is run by leaders to send a MsgBeat after
     * r.heartbeatTimeout.
     */
    tickHeartbeat() {
        this.heartbeatElapsed++;
        this.electionElapsed++;

        if (this.electionElapsed >= this.electionTimeout) {
            this.electionElapsed = 0;
            if (this.checkQuorum) {
                this.Step(
                    new Message({
                        from: this.id,
                        type: MessageType.MsgCheckQuorum,
                    })
                );
            }
            // If current leader cannot transfer leadership in electionTimeout,
            // it becomes leader again.
            if (this.state === StateType.StateLeader && this.leadTransferee !== ridnone) {
                this.abortLeaderTransfer();
            }
        }

        if (this.state !== StateType.StateLeader) {
            return;
        }

        if (this.heartbeatElapsed >= this.heartbeatTimeout) {
            this.heartbeatElapsed = 0;
            this.Step(new Message({ from: this.id, type: MessageType.MsgBeat }));
        }
    }

    becomeFollower(term: number, lead: rid) {
        this.step = stepFollower;
        this.reset(term);
        this.tick = this.tickElection;
        this.lead = lead;
        this.state = StateType.StateFollower;
        this.logger.infof("%s became follower at term %d", this.id, this.term);
    }

    becomeCandidate() {
        // TO DO(xiangli) remove the panic when the raft implementation is
        // stable
        if (this.state === StateType.StateLeader) {
            throw new Error("invalid transition [leader -> candidate]");
        }
        this.step = stepCandidate;
        this.reset(this.term + 1);
        this.tick = this.tickElection;
        this.vote = this.id;
        this.state = StateType.StateCandidate;
        this.logger.infof("%s became candidate at term %d", this.id, this.term);
    }

    becomePreCandidate() {
        // TO DO(xiangli) remove the panic when the raft implementation is
        // stable
        if (this.state === StateType.StateLeader) {
            throw new Error("invalid transition [leader -> candidate]");
        }
        // Becoming a pre-candidate changes our step functions and state, but
        // doesn't change anything else. In particular it does not increase
        // r.Term or change r.Vote.
        this.step = stepCandidate;
        this.prs.resetVotes();
        this.tick = this.tickElection;
        this.lead = ridnone;
        this.state = StateType.StatePreCandidate;
        this.logger.infof("%s became pre-candidate at term %d", this.id, this.term);
    }

    becomeLeader() {
        // TO DO(xiangli) remove the panic when the raft implementation is
        // stable
        if (this.state === StateType.StateFollower) {
            throw new Error("invalid transition [follower -> leader]");
        }
        this.step = stepLeader;
        this.reset(this.term);
        this.tick = this.tickHeartbeat;
        this.lead = this.id;
        this.state = StateType.StateLeader;
        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        const p = this.prs.progress.get(this.id);
        if (p === undefined) {
            // NOTE: Go implementation doesn't check for this case => We assume
            // that this should never happen
            throw new Error();
        }
        p.BecomeReplicate();

        // Conservatively set the pendingConfIndex to the last index in the log.
        // There may or may not be a pending config change, but it's safe to
        // delay any future proposals until we commit all our pending log
        // entries, and scanning the entire tail of the log could be expensive.
        this.pendingConfIndex = this.raftLog.lastIndex();

        const emptyEnt = new Entry();

        if (!this.appendEntry(emptyEnt)) {
            // This won't happen because we just called reset() above.
            this.logger.panicf("empty entry was dropped");
        }
        // As a special case, don't count the initial empty entry towards the
        // uncommitted log quota. This is because we want to preserve the
        // behavior of allowing one entry larger than quota if the current usage
        // is zero.
        this.reduceUncommittedSize([emptyEnt]);
        this.logger.infof("%s became leader at term %d", this.id, this.term);
    }

    hup(t: CampaignType) {
        if (this.state === StateType.StateLeader) {
            this.logger.debugf("%s ignoring MsgHup because already leader", this.id);
            return;
        }

        if (!this.promotable()) {
            this.logger.warningf("%s is unpromotable and can not campaign", this.id);
            return;
        }

        const [ent, err] = this.raftLog.slice(
            this.raftLog.applied + 1,
            this.raftLog.committed + 1,
            noLimit
        );
        if (err !== null) {
            this.logger.panicf("unexpected error getting unapplied entries (%s)", err);
        }

        const n = numOfPendingConf(ent);
        if (n !== 0 && this.raftLog.committed > this.raftLog.applied) {
            this.logger.warningf(
                "%s cannot campaign at term %d since there are still %d pending configuration changes to apply",
                this.id,
                this.term,
                n,
            );
            return;
        }

        this.logger.infof("%s is starting a new election at term %d", this.id, this.term);
        this.campaign(t);
    }

    /**
     * campaign transitions the raft instance to candidate state. This must only
     * be called after verifying that this is a legitimate transition.
     */
    campaign(t: CampaignType) {
        if (!this.promotable()) {
            // This path should not be hit (callers are supposed to check), but
            // better safe than sorry.
            this.logger.warningf("%s is unpromotable; campaign() should have been called", this.id);
        }
        let term: number;
        let voteMsg: MessageType;
        if (t === CampaignType.campaignPreElection) {
            this.becomePreCandidate();
            voteMsg = MessageType.MsgPreVote;
            // PreVote RPCs are sent for the next term before we've incremented
            // r.Term.
            term = this.term + 1;
        } else {
            this.becomeCandidate();
            voteMsg = MessageType.MsgVote;
            term = this.term;
        }
        const res = this.poll(this.id, voteRespMsgType(voteMsg), true)[2];
        if (res === VoteResult.VoteWon) {
            // We won the election after voting for ourselves (which must mean
            // that this is a single-node cluster). Advance to the next state.
            if (t === CampaignType.campaignPreElection) {
                this.campaign(CampaignType.campaignElection);
            } else {
                this.becomeLeader();
            }
            return;
        }

        const ids = [...this.prs.config.voters.ids().keys()].sort((a, b) => ('' + a).localeCompare(b));

        for (const id of ids) {
            if (id === this.id) {
                continue;
            }
            this.logger.infof(
                "%s [logterm: %d, index: %d] sent %d request to %s at term %d",
                this.id,
                this.raftLog.lastTerm(),
                this.raftLog.lastIndex(),
                voteMsg,
                id,
                this.term,
            );

            let ctx: RaftData = emptyRaftData;
            if (t === CampaignType.campaignTransfer) {
                ctx = t;
            }
            this.send(
                new Message({
                    term: term,
                    to: id,
                    type: voteMsg,
                    index: this.raftLog.lastIndex(),
                    logTerm: this.raftLog.lastTerm(),
                    context: ctx,
                })
            );
        }
    }

    poll(
        id: rid,
        t: MessageType,
        v: boolean
    ): [granted: number, rejected: number, result: VoteResult] {
        if (v) {
            this.logger.infof("%s received %d from %s at term %d", this.id, t, id, this.term);
        } else {
            this.logger.infof("%s received %d rejection from %s at term %d", this.id, t, id, this.term);
        }
        this.prs.recordVote(id, v);
        return this.prs.tallyVotes();
    }

    Step(m: Message): NullableError {
        // Handle the message term, which may result in our stepping down to a
        // follower.
        if (m.term === 0) {
            // Local message
        } else if (m.term > this.term) {
            if (m.type === MessageType.MsgVote || m.type === MessageType.MsgPreVote) {
                const force = m.context === CampaignType.campaignTransfer;
                const inLease =
                    this.checkQuorum &&
                    this.lead !== ridnone &&
                    this.electionElapsed < this.electionTimeout;
                if (!force && inLease) {
                    // If a server receives a RequestVote request within the
                    // minimum election timeout of hearing from a current
                    // leader, it does not update its term or grant its vote
                    this.logger.infof(
                        "%s [logterm: %d, index: %d, vote: %s] ignored %d from %s [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
                        this.id,
                        this.raftLog.lastTerm(),
                        this.raftLog.lastIndex(),
                        this.vote,
                        m.type,
                        m.from,
                        m.logTerm,
                        m.index,
                        m.term,
                        this.electionTimeout - this.electionElapsed,
                    );
                    return null;
                }
            }
            if (
                !(m.type === MessageType.MsgPreVote) &&
                !(m.type === MessageType.MsgPreVoteResp && !m.reject)
            ) {
                this.logger.infof(
                    "%s [term: %d] received a %d message with higher term from %s [term: %d]",
                    this.id,
                    this.term,
                    m.type,
                    m.from,
                    m.term,
                );
                if (
                    m.type === MessageType.MsgApp ||
                    m.type === MessageType.MsgHeartbeat ||
                    m.type === MessageType.MsgSnap
                ) {
                    this.becomeFollower(m.term, m.from);
                } else {
                    this.becomeFollower(m.term, ridnone);
                }
            }
        } else if (m.term < this.term) {
            if (
                (this.checkQuorum || this.preVote) &&
                (m.type === MessageType.MsgHeartbeat || m.type === MessageType.MsgApp)
            ) {
                // We have received messages from a leader at a lower term. It
                // is possible that these messages were simply delayed in the
                // network, but this could also mean that this node has advanced
                // its term number during a network partition, and it is now
                // unable to either win an election or to rejoin the majority on
                // the old term. If checkQuorum is false, this will be handled
                // by incrementing term numbers in response to MsgVote with a
                // higher term, but if checkQuorum is true we may not advance
                // the term on MsgVote and must generate other messages to
                // advance the term. The net result of these two features is to
                // minimize the disruption caused by nodes that have been
                // removed from the cluster's configuration: a removed node will
                // send MsgVotes (or MsgPreVotes) which will be ignored, but it
                // will not receive MsgApp or MsgHeartbeat, so it will not
                // create disruptive term increases, by notifying leader of this
                // node's activeness. The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election
                // ending up with a higher term than leader, although it won't
                // receive enough votes to win the election. When it regains
                // connectivity, this response with "pb.MsgAppResp" of higher
                // term would force leader to step down. However, this
                // disruption is inevitable to free this stuck node with fresh
                // election. This can be prevented with Pre-Vote phase.
                this.send(new Message({ to: m.from, type: MessageType.MsgAppResp }));
            } else if (m.type === MessageType.MsgPreVote) {
                // Before Pre-Vote enable, there may have candidate with higher
                // term, but less log. After update to Pre-Vote, the cluster may
                // deadlock if we drop messages with a lower term.
                this.logger.infof(
                    "%s [logterm: , index: %d, vote: %s] rejected %d from %s [logterm: %d, index: %d] at term %d",
                    this.id,
                    this.raftLog.lastTerm(),
                    this.raftLog.lastIndex(),
                    this.vote,
                    m.type,
                    m.from,
                    m.logTerm,
                    m.index,
                    m.term,
                );
                this.send(
                    new Message({
                        to: m.from,
                        term: this.term,
                        type: MessageType.MsgPreVoteResp,
                        reject: true,
                    })
                );
            } else {
                // ignore other cases
                this.logger.infof(
                    "%s [term: %d] ignored a %d message with lower term from %s [term: %d]",
                    this.id,
                    this.term,
                    m.type,
                    m.from,
                    m.term,
                );
            }
            return null;
        }

        switch (m.type) {
            case MessageType.MsgHup:
                if (this.preVote) {
                    this.hup(CampaignType.campaignPreElection);
                } else {
                    this.hup(CampaignType.campaignElection);
                }
                break;
            case MessageType.MsgVote: // Fallthrough
            case MessageType.MsgPreVote:
                // We can vote if this is a repeat of a vote we've already
                // cast...
                const canVote =
                    this.vote === m.from ||
                    // ...we haven't voted and we don't think there's a leader
                    // yet in this term...
                    (this.lead === ridnone && this.vote === ridnone) ||
                    // ...or this is a PreVote for a future term...
                    (m.type === MessageType.MsgPreVote && m.term > this.term);
                // ...and we believe the candidate is up to date.
                if (canVote && this.raftLog.isUpToDate(m.index, m.logTerm)) {
                    // Note: it turns out that that learners must be allowed to
                    // cast votes. This seems counter- intuitive but is
                    // necessary in the situation in which a learner has been
                    // promoted (i.e. is now a voter) but has not learned about
                    // this yet. For example, consider a group in which id=1 is
                    // a learner and id=2 and id=3 are voters. A configuration
                    // change promoting 1 can be committed on the quorum `{2,3}`
                    // without the config change being appended to the learner's
                    // log. If the leader (say 2) fails, there are de facto two
                    // voters remaining. Only 3 can win an election (due to its
                    // log containing all committed entries), but to do so it
                    // will need 1 to vote. But 1 considers itself a learner and
                    // will continue to do so until 3 has stepped up as leader,
                    // replicates the conf change to 1, and 1 applies it.
                    // Ultimately, by receiving a request to vote, the learner
                    // realizes that the candidate believes it to be a voter,
                    // and that it should act accordingly. The candidate's
                    // config may be stale, too; but in that case it won't win
                    // the election, at least in the absence of the bug
                    // discussed in:
                    // https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
                    this.logger.infof(
                        "%s [logterm: %d, index: %d, vote: %s] cast %d for %s [logterm: %d, index: %d] at term %d",
                        this.id,
                        this.raftLog.lastTerm(),
                        this.raftLog.lastIndex(),
                        this.vote,
                        m.type,
                        m.from,
                        m.logTerm,
                        m.index,
                        this.term,
                    );
                    // When responding to Msg{Pre,}Vote messages we include the
                    // term from the message, not the local term. To see why,
                    // consider the case where a single node was previously
                    // partitioned away and it's local term is now out of date.
                    // If we include the local term (recall that for pre-votes
                    // we don't update the local term), the (pre-)campaigning
                    // node on the other end will proceed to ignore the message
                    // (it ignores all out of date messages). The term in the
                    // original message and current local term are the same in
                    // the case of regular votes, but different for pre-votes.
                    this.send(
                        new Message({
                            to: m.from,
                            term: m.term,
                            type: voteRespMsgType(m.type),
                        })
                    );
                    if (m.type === MessageType.MsgVote) {
                        // Only record real votes.
                        this.electionElapsed = 0;
                        this.vote = m.from;
                    }
                } else {
                    this.logger.infof(
                        "%s [logterm: %d, index: %d, vote: %s] rejected %d from %s [logterm: %d, index: %d] at term %d",
                        this.id,
                        this.raftLog.lastTerm(),
                        this.raftLog.lastIndex(),
                        this.vote,
                        m.type,
                        m.from,
                        m.logTerm,
                        m.index,
                        this.term,
                    );
                    this.send(
                        new Message({
                            to: m.from,
                            term: this.term,
                            type: voteRespMsgType(m.type),
                            reject: true,
                        })
                    );
                }
                break;
            default:
                const err = this.step!(this, m);
                if (err !== null) {
                    return err;
                }
        }
        return null;
    }

    handleAppendEntries(m: Message) {
        if (m.index < this.raftLog.committed) {
            this.send(
                new Message({
                    to: m.from,
                    type: MessageType.MsgAppResp,
                    index: this.raftLog.committed,
                })
            );
            return;
        }

        const [mlastIndex, ok] = this.raftLog.maybeAppend(
            m.index,
            m.logTerm,
            m.commit,
            ...m.entries
        );
        if (ok) {
            // reject = false -> default values for rejectHint and logTerm
            const me = new Message({
                to: m.from,
                type: MessageType.MsgAppResp,
                index: mlastIndex,
            });
            this.send(me);
        } else {
            const f = this.raftLog.zeroTermOnErrCompacted(...this.raftLog.term(m.index));
            this.logger.debugf(
                "%s [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %s",
                this.id,
                f,
                m.index,
                m.logTerm,
                m.index,
                m.from,
            );

            // Return a hint to the leader about the maximum index and term that
            // the two logs could be divergent at. Do this by searching through
            // the follower's log for the maximum (index, term) pair with a term
            // <= the MsgApp's LogTerm and an index <= the MsgApp's Index. This
            // can help skip all indexes in the follower's uncommitted tail with
            // terms greater than the MsgApp's LogTerm.
            //
            // See the other caller for findConflictByTerm (in stepLeader) for a
            // much more detailed explanation of this mechanism.
            let hintIndex = Math.min(m.index, this.raftLog.lastIndex());
            hintIndex = this.raftLog.findConflictByTerm(hintIndex, m.logTerm);
            const [hintTerm, err] = this.raftLog.term(hintIndex);
            if (err !== null) {
                throw new Error(`term(${hintIndex}) must be valid, but got ${err}`);
            }
            this.send(
                new Message({
                    to: m.from,
                    type: MessageType.MsgAppResp,
                    index: m.index,
                    reject: true,
                    rejectHint: hintIndex,
                    logTerm: hintTerm,
                })
            );
        }
    }

    handleHeartbeat(m: Message) {
        this.raftLog.commitTo(m.commit);
        const msg = new Message({
            to: m.from,
            type: MessageType.MsgHeartbeatResp,
            context: m.context,
        });
        this.send(msg);
    }

    handleSnapshot(m: Message) {
        const sindex = m.snapshot.metadata.index;
        const sterm = m.snapshot.metadata.term;
        if (this.restore(m.snapshot)) {
            this.logger.infof(
                "%s [commit: %d] restored snapshot [index: %d, term: %d]",
                this.id,
                this.raftLog.committed,
                sindex,
                sterm,
            );
            this.send(
                new Message({
                    to: m.from,
                    type: MessageType.MsgAppResp,
                    index: this.raftLog.lastIndex(),
                })
            );
        } else {
            this.logger.infof(
                "%s [commit: %d] ignored snapshot [index: %d, term: %d]",
                this.id,
                this.raftLog.committed,
                sindex,
                sterm,
            );
            this.send(
                new Message({
                    to: m.from,
                    type: MessageType.MsgAppResp,
                    index: this.raftLog.committed,
                })
            );
        }
    }

    /**
     * restore recovers the state machine from a snapshot. It restores the log
     * and the configuration of state machine. If this method returns false, the
     * snapshot was ignored, either because it was obsolete or because of an
     * error.
     */
    restore(s: Snapshot): boolean {
        if (s.metadata.index <= this.raftLog.committed) {
            return false;
        }
        if (this.state !== StateType.StateFollower) {
            // This is defense-in-depth: if the leader somehow ended up applying
            // a snapshot, it could move into a new term without moving into a
            // follower state. This should never fire, but if it did, we'd have
            // prevented damage by returning early, so log only a loud warning.
            //
            // At the time of writing, the instance is guaranteed to be in
            // follower state when this method is called.
            this.logger.warningf(
                "%s attempted to restore snapshot as leader; should never happen",
                this.id
            );
            this.becomeFollower(this.term + 1, ridnone);
            return false;
        }

        // More defense-in-depth: throw away snapshot if recipient is not in the
        // config. This shouldn't ever happen (at the time of writing) but lots
        // of code here and there assumes that r.id is in the progress tracker.
        let found = false;
        const cs = s.metadata.confState;

        for (const values of [cs.voters, cs.learners, cs.votersOutgoing]) {
            for (const id of values ?? []) {
                if (id === this.id) {
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
        }

        if (!found) {
            this.logger.warningf(
                "%s attempted to restore snapshot but it is not in the ConfState %o; should never happen",
                this.id,
                cs,
            );
            return false;
        }

        // Now go ahead and actually restore.

        if (this.raftLog.matchTerm(s.metadata.index, s.metadata.term)) {
            this.logger.infof(
                "%s [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
                this.id,
                this.raftLog.committed,
                this.raftLog.lastIndex(),
                this.raftLog.lastTerm(),
                s.metadata.index,
                s.metadata.term,
            );
            this.raftLog.commitTo(s.metadata.index);
            return false;
        }

        this.raftLog.restore(s);

        // Reset the configuration and add the (potentially updated) peers in
        // anew.
        this.prs = MakeProgressTracker(this.prs.maxInflight);
        const b = Restore(new Changer(this.prs, this.raftLog.lastIndex()), cs); // cfg, prs, err :

        if (b[2] !== null) {
            // This should never happen. Either there's a bug in our config
            // change handling or the client corrupted the conf change.
            throw new Error(`unable to restore config ${cs}: ${b[2]}`);
        }

        assertConfStatesEquivalent(this.logger, cs, this.switchToConfig(b[0], b[1]));

        const pr = this.prs.progress.get(this.id);
        if (pr === undefined) {
            // NOTE: Go implementation doesn't check for this case => We assume
            // that this should never happen
            throw new Error();
        }
        pr.MaybeUpdate(pr.next - 1); // RaftGo-TODO(tbg): this is untested and likely unneeded

        this.logger.infof(
            "%s [commit: %d, lastIndex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
            this.id,
            this.raftLog.committed,
            this.raftLog.lastIndex(),
            this.raftLog.lastTerm(),
            s.metadata.index,
            s.metadata.term,
        );
        return true;
    }

    /**
     * promotable indicates whether state machine can be promoted to leader,
     * which is true when its own id is in progress list.
     */
    promotable(): boolean {
        const pr = this.prs.progress.get(this.id);
        return pr !== undefined && !pr.isLearner && !this.raftLog.hasPendingSnapshot();
    }

    applyConfChange(cc: ConfChangeV2): [ConfState, NullableError] {
        const changer: Changer = new Changer(this.prs, this.raftLog.lastIndex());
        let res: [Config, ProgressMap, NullableError];
        if (cc.leaveJoint()) {
            res = changer.leaveJoint();
        } else if (cc.enterJoint()[1]) {
            res = changer.enterJoint(cc.enterJoint()[0], ...cc.changes);
        } else {
            res = changer.simple(...cc.changes);
        }

        // NOTE: Resolved todo from go library in ts version
        return [this.switchToConfig(res[0], res[1]), res[2]];
    }

    /**
     * switchToConfig reconfigures this node to use the provided configuration.
     * It updates the in-memory state and, when necessary, carries out
     * additional actions such as reacting to the removal of nodes or changed
     * quorum requirements.
     *
     * The inputs usually result from restoring a ConfState or applying a
     * ConfChange.
     */
    switchToConfig(cfg: Config, prs: ProgressMap): ConfState {
        this.prs.config = cfg;
        this.prs.progress = prs;

        this.logger.infof("%s switched to configuration %o", this.id, this.prs.config);
        const cs = this.prs.confState();
        const pr = this.prs.progress.get(this.id);
        const ok = pr !== undefined;

        // Update whether the node itself is a learner, resetting to false when
        // the node is removed.
        this.isLearner = pr !== undefined && pr.isLearner;

        if ((!ok || this.isLearner) && this.state === StateType.StateLeader) {
            // This node is leader and was removed or demoted. We prevent
            // demotions at the time writing but hypothetically we handle them
            // the same way as removing the leader: stepping down into the next
            // Term.
            //
            // TO DO(tbg): step down (for sanity) and ask follower with largest
            // Match to TimeoutNow (to avoid interruption). This might still
            // drop some proposals but it's better than nothing.
            //
            // TO DO(tbg): test this branch. It is untested at the time of
            // writing.
            return cs;
        }

        // The remaining steps only make sense if this node is the leader and
        // there are other nodes.
        if (this.state !== StateType.StateLeader || cs.voters.length === 0) {
            return cs;
        }

        if (this.maybeCommit()) {
            // If the configuration change means that more entries are committed
            // now, broadcast/append to everyone in the updated config.
            this.bcastAppend();
        } else {
            // Otherwise, still probe the newly added replicas; there's no
            // reason to let them wait out a heartbeat interval (or the next
            // incoming proposal).
            this.prs.visit((id: rid, p: Progress) => {
                this.maybeSendAppend(id, false /* sendIfEmpty */);
            });
        }
        // If the the leadTransferee was removed or demoted, abort the
        // leadership transfer.
        const tOK = this.prs.config.voters.ids().has(this.leadTransferee);
        if (!tOK && this.leadTransferee !== ridnone) {
            this.abortLeaderTransfer();
        }
        return cs;
    }

    loadState(state: HardState) {
        if (state.commit < this.raftLog.committed || state.commit > this.raftLog.lastIndex()) {
            this.logger.panicf(
                "%s state.commit %d is out of range [%d], %d",
                this.id,
                state.commit,
                this.raftLog.committed,
                this.raftLog.lastIndex(),
            );
        }
        this.raftLog.committed = state.commit;
        this.term = state.term;
        this.vote = state.vote;
    }

    /**
     * pastElectionTimeout returns true iff r.electionElapsed is greater than or
     * equal to the randomized election timeout in [electionTimeout, 2 *
     * electionTimeout - 1].
     */
    pastElectionTimeout(): boolean {
        return this.electionElapsed >= this.randomizedElectionTimeout;
    }

    resetRandomizedElectionTimeout() {
        this.randomizedElectionTimeout =
            this.electionTimeout + Math.floor(Math.random() * this.electionTimeout);
    }

    sendTimeoutNow(to: rid) {
        this.send(new Message({ to: to, type: MessageType.MsgTimeoutNow }));
    }

    abortLeaderTransfer() {
        this.leadTransferee = ridnone;
    }

    /**
     * committedEntryInCurrentTerm return true if the peer has committed an
     * entry in its term.
     */
    committedEntryInCurrentTerm(): boolean {
        const a = this.raftLog.term(this.raftLog.committed);
        return this.raftLog.zeroTermOnErrCompacted(a[0], a[1]) === this.term;
    }

    /**
     * responseToReadIndexReq constructs a response for `req`. If `req` comes
     * from the peer itself, a blank value will be returned.
     */
    responseToReadIndexReq(req: Message, readIndex: number): Message {
        if (req.from === ridnone || req.from === this.id) {
            this.readStates.push(
                new ReadState({
                    index: readIndex,
                    requestCtx: req.entries[0].data,
                })
            );
            return new Message();
        }
        return new Message({
            type: MessageType.MsgReadIndexResp,
            to: req.from,
            index: readIndex,
            entries: req.entries,
        });
    }

    /**
     * increaseUncommittedSize computes the size of the proposed entries and
     * determines whether they would push leader over its maxUncommittedSize
     * limit. If the new entries would exceed the limit, the method returns
     * false. If not, the increase in uncommitted entry size is recorded and the
     * method returns true.
     *
     * Empty payloads are never refused. This is used both for appending an
     * empty entry at a new leader's term, as well as leaving a joint
     * configuration.
     */
    increaseUncommittedSize(ents: Entry[]): boolean {
        let s: number = 0;
        ents.forEach((value, _) => (s += payloadSize(value)));

        if (
            this.uncommittedSize > 0 &&
            s > 0 &&
            this.uncommittedSize + s > this.maxUncommittedSize
        ) {
            // If the uncommitted tail of the Raft log is empty, allow any size
            // proposal. Otherwise, limit the size of the uncommitted tail of
            // the log and drop any proposal that would push the size over the
            // limit. Note the added requirement s>0 which is used to make sure
            // that appending single empty entries to the log always succeeds,
            // used both for replicating a new leader's initial empty entry, and
            // for auto-leaving joint configurations.
            return false;
        }
        this.uncommittedSize += s;
        return true;
    }

    /**
     * reduceUncommittedSize accounts for the newly committed entries by
     * decreasing the uncommitted entry size limit.
     */
    reduceUncommittedSize(ents: Entry[]) {
        if (this.uncommittedSize === 0) {
            // Fast-path for followers, who do not track or enforce the limit.
            return;
        }

        let s: number = 0;
        ents.forEach((value, _) => (s += payloadSize(value)));

        if (s > this.uncommittedSize) {
            // uncommittedSize may underestimate the size of the uncommitted
            // Raft log tail but will never overestimate it. Saturate at 0
            // instead of allowing overflow.
            this.uncommittedSize = 0;
        } else {
            this.uncommittedSize -= s;
        }
    }
}

type StepFunc = (r: Raft, m: Message) => NullableError;

export function stepLeader(r: Raft, m: Message): NullableError {
    // These message types do not require any progress for m.From.
    switch (m.type) {
        case MessageType.MsgBeat:
            r.bcastHeartbeat();
            return null;
        case MessageType.MsgCheckQuorum:
            // The leader should always see itself as active. As a precaution,
            // handle the case in which the leader isn't in the configuration
            // any more (for example if it just removed itself).
            //
            // RaftGo-TODO(tbg): I added a TODO in removeNode, it doesn't seem
            // that the leader steps down when removing itself. I might be
            // missing something.
            const prd = r.prs.progress.get(r.id);
            if (prd !== undefined) {
                prd.recentActive = true;
            }
            if (!r.prs.quorumActive()) {
                r.logger.warningf("%s stepped down to follower since quorum is not active", r.id);
                r.becomeFollower(r.term, ridnone);
            }
            // Mark everyone (but ourselves) as inactive in preparation for the
            // next CheckQuorum.
            r.prs.visit((id: rid, p: Progress) => {
                if (id !== r.id) {
                    p.recentActive = false;
                }
            });
            return null;
        case MessageType.MsgProp:
            if (m.entries.length === 0) {
                r.logger.panicf("%s stepped empty MsgProp", r.id);
            }

            if (r.prs.progress.get(r.id) === undefined) {
                // If we are not currently a member of the range (i.e. this node
                // was removed from the configuration while serving as leader),
                // drop any new proposals.
                return errProposalDropped;
            }

            if (r.leadTransferee !== ridnone) {
                r.logger.debugf(
                    "%s [term %d] transfer leadership to %s is in progress; dropping proposal",
                    r.id,
                    r.term,
                    r.leadTransferee,
                );
                return errProposalDropped;
            }
            for (let i = 0; i < m.entries.length; i++) {
                let cc: ConfChangeI | null = null;
                if (m.entries[i].type === EntryType.EntryConfChange) {
                    cc = new ConfChange(m.entries[i].data);
                } else if (m.entries[i].type === EntryType.EntryConfChangeV2) {
                    cc = new ConfChangeV2(m.entries[i].data);
                }

                if (cc !== null) {
                    const alreadyPending = r.pendingConfIndex > r.raftLog.applied;
                    const alreadyJoint = r.prs?.config?.voters?.config[1]?.length() > 0;
                    const wantsLeaveJoint = cc.asV2().changes.length === 0;

                    let refused: string = "";
                    if (alreadyPending) {
                        refused = `possible unapplied conf change at index ${r.pendingConfIndex} (applied to ${r.raftLog.applied})`;
                    } else if (alreadyJoint && !wantsLeaveJoint) {
                        refused = `must transition out of joint config first`;
                    } else if (!alreadyJoint && wantsLeaveJoint) {
                        refused = `not in joint state; refusing empty conf change`;
                    }

                    if (refused !== "") {
                        r.logger.infof(
                            "%s ignoring conf change %o at config %o: %s", r.id, cc, r.prs.config, refused
                        );
                        m.entries[i] = new Entry({
                            type: EntryType.EntryNormal,
                        });
                    } else {
                        r.pendingConfIndex = r.raftLog.lastIndex() + i + 1;
                    }
                }
            }

            if (!r.appendEntry(...m.entries)) {
                return errProposalDropped;
            }

            r.bcastAppend();
            return null;
        case MessageType.MsgReadIndex:
            // only one voting member (the leader) in the cluster
            if (r.prs.isSingleton()) {
                const resp = r.responseToReadIndexReq(m, r.raftLog.committed);
                if (resp.to !== ridnone) {
                    r.send(resp);
                }
                return null;
            }

            // Postpone read only request when this leader has not committed any
            // log entry at its term.
            if (!r.committedEntryInCurrentTerm()) {
                r.pendingReadIndexMessages.push(m);
                return null;
            }

            sendMsgReadIndexResponse(r, m);

            return null;
    }

    // All other message types require a progress for m.From (pr).
    const pr = r.prs.progress.get(m.from);
    if (pr === undefined) {
        r.logger.debugf("%d no progress available", r.id);
        return null;
    }
    switch (m.type) {
        case MessageType.MsgAppResp:
            pr.recentActive = true;

            if (m.reject) {
                // RejectHint is the suggested next base entry for appending
                // (i.e. we try to append entry RejectHint+1 next), and LogTerm
                // is the term that the follower has at index RejectHint. Older
                // versions of this library did not populate LogTerm for
                // rejections and it is zero for followers with an empty log.
                //
                // Under normal circumstances, the leader's log is longer than
                // the follower's and the follower's log is a prefix of the
                // leader's (i.e. there is no divergent uncommitted suffix of
                // the log on the follower). In that case, the first probe
                // reveals where the follower's log ends (RejectHint=follower's
                // last index) and the subsequent probe succeeds.
                //
                // However, when networks are partitioned or systems overloaded,
                // large divergent log tails can occur. The naive attempt,
                // probing entry by entry in decreasing order, will be the
                // product of the length of the diverging tails and the network
                // round-trip latency, which can easily result in hours of time
                // spent probing and can even cause outright outages. The probes
                // are thus optimized as described below.
                r.logger.debugf(
                    "%s received MsgAppResp(rejected, hint: (index %d, term %d)) from %s for index %d",
                    r.id,
                    m.rejectHint,
                    m.logTerm,
                    m.from,
                    m.index,
                );
                let nextProbeIdx = m.rejectHint ?? 0;
                if (m.logTerm > 0) {
                    // If the follower has an uncommitted log tail, we would end
                    // up probing one by one until we hit the common prefix.
                    //
                    // For example, if the leader has:
                    //
                    //   idx        1 2 3 4 5 6 7 8 9
                    //              -----------------
                    //   term (L)   1 3 3 3 5 5 5 5 5 term (F)   1 1 1 1 2 2
                    //
                    // Then, after sending an append anchored at (idx=9,term=5)
                    // we would receive a RejectHint of 6 and LogTerm of 2.
                    // Without the code below, we would try an append at index
                    // 6, which would fail again.
                    //
                    // However, looking only at what the leader knows about its
                    // own log and the rejection hint, it is clear that a probe
                    // at index 6, 5, 4, 3, and 2 must fail as well:
                    //
                    // For all of these indexes, the leader's log term is larger
                    // than the rejection's log term. If a probe at one of these
                    // indexes succeeded, its log term at that index would match
                    // the leader's, i.e. 3 or 5 in this example. But the
                    // follower already told the leader that it is still at term
                    // 2 at index 9, and since the log term only ever goes up
                    // (within a log), this is a contradiction.
                    //
                    // At index 1, however, the leader can draw no such
                    // conclusion, as its term 1 is not larger than the term 2
                    // from the follower's rejection. We thus probe at 1, which
                    // will succeed in this example. In general, with this
                    // approach we probe at most once per term found in the
                    // leader's log.
                    //
                    // There is a similar mechanism on the follower (implemented
                    // in handleAppendEntries via a call to findConflictByTerm)
                    // that is useful if the follower has a large divergent
                    // uncommitted log tail[1], as in this example:
                    //
                    //   idx        1 2 3 4 5 6 7 8 9
                    //              -----------------
                    //   term (L)   1 3 3 3 3 3 3 3 7 term (F)   1 3 3 4 4 5 5 5
                    //   6
                    //
                    // Naively, the leader would probe at idx=9, receive a
                    // rejection revealing the log term of 6 at the follower.
                    // Since the leader's term at the previous index is already
                    // smaller than 6, the leader- side optimization discussed
                    // above is ineffective. The leader thus probes at index 8
                    // and, naively, receives a rejection for the same index and
                    // log term 5. Again, the leader optimization does not
                    // improve over linear probing as term 5 is above the
                    // leader's term 3 for that and many preceding indexes; the
                    // leader would have to probe linearly until it would
                    // finally hit index 3, where the probe would succeed.
                    //
                    // Instead, we apply a similar optimization on the follower.
                    // When the follower receives the probe at index 8 (log term
                    // 3), it concludes that all of the leader's log preceding
                    // that index has log terms of 3 or below. The largest index
                    // in the follower's log with a log term of 3 or below is
                    // index 3. The follower will thus return a rejection for
                    // index=3, log term=3 instead. The leader's next probe will
                    // then succeed at that index.
                    //
                    // [1]: more precisely, if the log terms in the large
                    // uncommitted tail on the follower are larger than the
                    // leader's. At first, it may seem unintuitive that a
                    // follower could even have such a large tail, but it can
                    // happen:
                    //
                    // 1. Leader appends (but does not commit) entries 2 and 3,
                    //    crashes. idx        1 2 3 4 5 6 7 8 9
                    //              -----------------
                    //   term (L)   1 2 2     [crashes] term (F)   1 term (F) 1
                    //
                    // 2. a follower becomes leader and appends entries at term
                    //    3.
                    //              -----------------
                    //   term (x)   1 2 2     [down] term (F)   1 3 3 3 3 term
                    //   (F)   1
                    //
                    // 3. term 3 leader goes down, term 2 leader returns as term
                    //    4 leader. It commits the log & entries at term 4.
                    //
                    //              -----------------
                    //   term (L)   1 2 2 2 term (x)   1 3 3 3 3 [down] term (F)
                    //   1
                    //              -----------------
                    //   term (L)   1 2 2 2 4 4 4 term (F)   1 3 3 3 3 [gets
                    //   probed] term (F)   1 2 2 2 4 4 4
                    //
                    // 4. the leader will now probe the returning follower at
                    //    index 7, the rejection points it at the end of the
                    //    follower's log which is at a higher log term than the
                    //    actually committed log.
                    nextProbeIdx = r.raftLog.findConflictByTerm(m.rejectHint, m.logTerm);
                }

                if (pr.MaybeDecrTo(m.index, nextProbeIdx)) {
                    r.logger.debugf(
                        "%s decreased progress of %s to [%o]",
                        r.id,
                        m.from,
                        pr,
                    );
                    if (pr.state === ProgressStateType.StateReplicate) {
                        pr.BecomeProbe();
                    }

                    r.sendAppend(m.from);
                }
            } else {
                const oldPaused = pr.IsPaused();
                if (pr.MaybeUpdate(m.index)) {
                    if (pr.state === ProgressStateType.StateProbe) {
                        pr.BecomeReplicate();
                    } else if (
                        pr.state === ProgressStateType.StateSnapshot &&
                        pr.match >= pr.pendingSnapshot
                    ) {
                        // TO DO(tbg): we should also enter this branch if a
                        // snapshot is received that is below pr.PendingSnapshot
                        // but which makes it possible to use the log again.
                        r.logger.debugf(
                            "%s recovered from needing snapshot, resumed sending replication messages to %s [%o]",
                            r.id,
                            m.from,
                            pr,
                        );
                        // Transition back to replicating state via probing
                        // state (which takes the snapshot into account). If we
                        // didn't move to replicating state, that would only
                        // happen with the next round of appends (but there may
                        // not be a next round for a while, exposing an
                        // inconsistent RaftStatus).
                        pr.BecomeProbe();
                        pr.BecomeReplicate();
                    } else if (pr.state === ProgressStateType.StateReplicate) {
                        pr.inflights!.freeLE(m.index);
                    }

                    if (r.maybeCommit()) {
                        // committed index has progressed for the term, so it is
                        // safe to respond to pending read index requests
                        releasePendingReadIndexMessages(r);
                        r.bcastAppend();
                    } else if (oldPaused) {
                        // If we were paused before, this node may be missing
                        // the latest commit index, so send it.
                        r.sendAppend(m.from);
                    }
                    // We've updated flow control information above, which may
                    // allow us to send multiple (size-limited) in-flight
                    // messages at once (such as when transitioning from probe
                    // to replicate, or when freeTo() covers multiple messages).
                    // If we have more entries to send, send as many messages as
                    // we can (without sending empty messages for the commit
                    // index)
                    while (r.maybeSendAppend(m.from, false));
                    // Transfer leadership is in progress.
                    if (m.from === r.leadTransferee && pr.match === r.raftLog.lastIndex()) {
                        r.logger.infof(
                            "%s sent MsgTimeoutNow to %s after received MsgAppResp",
                            r.id,
                            m.from,
                        );
                        r.sendTimeoutNow(m.from);
                    }
                }
            }
            break;
        case MessageType.MsgHeartbeatResp:
            pr.recentActive = true;
            pr.probeSent = false;

            // free one slot for the full inflights window to allow progress.
            if (pr.state === ProgressStateType.StateReplicate && pr.inflights!.full()) {
                pr.inflights!.freeFirstOne();
            }
            if (pr.match < r.raftLog.lastIndex()) {
                r.sendAppend(m.from);
            }

            if (
                r.readOnly.option !== ReadOnlyOption.ReadOnlySafe ||
                (m.context ? m.context : emptyRaftData) === emptyRaftData
            ) {
                return null;
            }

            if (
                r.prs.config.voters.voteResult(r.readOnly.recvAck(m.from, m.context)) !==
                VoteResult.VoteWon
            ) {
                return null;
            }

            const rss = r.readOnly.advance(m);
            rss.forEach((value, _) => {
                const resp = r.responseToReadIndexReq(value.req, value.index);
                if (resp.to !== ridnone) {
                    r.send(resp);
                }
            });
            break;
        case MessageType.MsgSnapStatus:
            if (pr.state !== ProgressStateType.StateSnapshot) {
                return null;
            }
            // TO DO(tbg): this code is very similar to the snapshot handling in
            // MsgAppResp above. In fact, the code there is more correct than
            // the code here and should likely be updated to match (or even
            // better, the logic pulled into a newly created Progress state
            // machine handler).
            if (!m.reject) {
                pr.BecomeProbe();
                r.logger.debugf(
                    "%s snapshot succeeded, resumed sending replication messages to %s [%o]",
                    r.id,
                    m.from,
                    pr,
                );
            } else {
                // NB: the order here matters or we'll be probing erroneously
                // from the snapshot index, but the snapshot never applied.
                pr.pendingSnapshot = 0;
                pr.BecomeProbe();
                r.logger.debugf(
                    "%s snapshot failed, resumed sending replication messages to %s [%o]",
                    r.id,
                    m.from,
                    pr,
                );
            }
            // If snapshot finish, wait for the MsgAppResp from the remote node
            // before sending out the next MsgApp. If snapshot failure, wait for
            // a heartbeat interval before next try
            pr.probeSent = true;
            break;
        case MessageType.MsgUnreachable:
            // During optimistic replication, if the remote becomes unreachable,
            // there is huge probability that a MsgApp is lost.
            if (pr.state === ProgressStateType.StateReplicate) {
                pr.BecomeProbe();
            }
            r.logger.debugf(
                "%s failed to send message to %s because it is unreachable [%o]",
                r.id,
                m.from,
                pr,
            );
            break;
        case MessageType.MsgTransferLeader:
            if (pr.isLearner) {
                r.logger.debugf("%s is learner. Ignored transferring leadership", r.id);
                return null;
            }
            const leadTransferee = m.from;
            const lastLeadTransferee = r.leadTransferee;
            if (lastLeadTransferee !== ridnone) {
                if (lastLeadTransferee === leadTransferee) {
                    r.logger.infof(
                        "%s [term %d] transfer leadership to %s is in progress, ignores request to same node %s",
                        r.id,
                        r.term,
                        r.leadTransferee,
                        r.leadTransferee,
                    );
                    return null;
                }
                r.abortLeaderTransfer();
                r.logger.infof(
                    "%s [term %d] abort previous transferring leadership to %s",
                    r.id,
                    r.term,
                    lastLeadTransferee,
                );
            }
            if (leadTransferee === r.id) {
                r.logger.debugf(
                    "${r.id} is already leader. Ignored transferring leadership to self",
                    r.id
                );
                return null;
            }
            // Transfer leadership to third party.
            r.logger.infof(
                "%s [term %d] starts to transfer leadership to %s",
                r.id,
                r.term,
                leadTransferee,
            );
            // Transfer leadership should be finished in one electionTimeout, so
            // reset r.electionElapsed.
            r.electionElapsed = 0;
            r.leadTransferee = leadTransferee;
            if (pr.match === r.raftLog.lastIndex()) {
                r.sendTimeoutNow(leadTransferee);
                r.logger.infof(
                    "%s sends MsgTimeoutNow to %s immediately as it already has up-to-date log",
                    r.id,
                    leadTransferee,
                );
            } else {
                r.sendAppend(leadTransferee);
            }
            break;
    }
    return null;
}

/**
 * stepCandidate is shared by StateCandidate and StatePreCandidate; the
 * difference is whether they respond to MsgVoteResp or MsgPreVoteResp.
 */
export function stepCandidate(r: Raft, m: Message): NullableError {
    // Only handle vote responses corresponding to our candidacy (while in
    // StateCandidate, we may get stale MsgPreVoteResp messages in this term
    // from our pre-candidate state).
    let myVoteRespType: MessageType;
    if (r.state === StateType.StatePreCandidate) {
        myVoteRespType = MessageType.MsgPreVoteResp;
    } else {
        myVoteRespType = MessageType.MsgVoteResp;
    }
    switch (m.type) {
        case MessageType.MsgProp:
            r.logger.infof("%s no leader at term %d; dropping proposal", r.id, r.term);
            return errProposalDropped;
        case MessageType.MsgApp:
            r.becomeFollower(m.term, m.from); // always m.Term == r.Term
            r.handleAppendEntries(m);
            break;
        case MessageType.MsgHeartbeat:
            r.becomeFollower(m.term, m.from); // always m.Term == r.Term
            r.handleHeartbeat(m);
            break;
        case MessageType.MsgSnap:
            r.becomeFollower(m.term, m.from); // always m.Term == r.Term
            r.handleSnapshot(m);
            break;
        case myVoteRespType:
            const pollRes = r.poll(m.from, m.type, !m.reject); // gr, rj, res :
            r.logger.infof(
                "%s has received %d granted %d type votes and %d vote rejections",
                this.id,
                pollRes[0],
                m.type,
                pollRes[1],
            );
            switch (pollRes[2]) {
                case VoteResult.VoteWon:
                    if (r.state === StateType.StatePreCandidate) {
                        r.campaign(CampaignType.campaignElection);
                    } else {
                        r.becomeLeader();
                        r.bcastAppend();
                    }
                    break;
                case VoteResult.VoteLost:
                    // pb.MsgPreVoteResp contains future term of pre-candidate
                    // m.Term > r.Term; reuse r.Term
                    r.becomeFollower(r.term, ridnone);
                    break;
            }
            break;
        case MessageType.MsgTimeoutNow:
            r.logger.debugf(
                "%s [term %d state %d] ignored MsgTimeoutNow from %s",
                r.id,
                r.term,
                r.state,
                m.from,
            );
            break;
    }
    return null;
}

export function stepFollower(r: Raft, m: Message): NullableError {
    switch (m.type) {
        case MessageType.MsgProp:
            if (r.lead === ridnone) {
                r.logger.infof("%s no leader at term %d; dropping proposal", r.id, r.term);
                return errProposalDropped;
            } else if (r.disableProposalForwarding) {
                r.logger.infof(
                    "%s not forwarding to leader %s at term %d; dropping proposal",
                    r.id,
                    r.lead,
                    r.term,
                );
                return errProposalDropped;
            }
            m.to = r.lead;
            r.send(m);
            break;
        case MessageType.MsgApp:
            r.electionElapsed = 0;
            r.lead = m.from;
            r.handleAppendEntries(m);
            break;
        case MessageType.MsgHeartbeat:
            r.electionElapsed = 0;
            r.lead = m.from;
            r.handleHeartbeat(m);
            break;
        case MessageType.MsgSnap:
            r.electionElapsed = 0;
            r.lead = m.from;
            r.handleSnapshot(m);
            break;
        case MessageType.MsgTransferLeader:
            if (r.lead === ridnone) {
                r.logger.infof("%s no leader at term %d; dropping leader transfer msg", r.id, r.term);
                return null;
            }
            m.to = r.lead;
            r.send(m);
            break;
        case MessageType.MsgTimeoutNow:
            r.logger.infof(
                "%s [term %d] received MsgTimeoutNow from %s and starts an election to get leadership.",
                r.id,
                r.term,
                m.from,
            );
            // Leadership transfers never use pre-vote even if r.preVote is
            // true; we know we are not recovering from a partition so there is
            // no need for the extra round trip.
            r.hup(CampaignType.campaignTransfer);
            break;
        case MessageType.MsgReadIndex:
            if (r.lead === ridnone) {
                r.logger.infof("%s no leader at term %d; dropping index reading msg", r.id, r.term);
                return null;
            }
            m.to = r.lead;
            r.send(m);
            break;
        case MessageType.MsgReadIndexResp:
            if (m.entries.length !== 1) {
                r.logger.errorf(
                    "%s invalid format of MsgReadIndexResp from %s, entries count: %d",
                    r.id,
                    m.from,
                    m.entries.length,
                );
                return null;
            }
            r.readStates.push(...r.readStates);
            r.readStates.push({
                index: m.index,
                requestCtx: m.entries[0].data,
            });
            break;
    }
    return null;
}

function numOfPendingConf(entries: Entry[]): number {
    let n = 0;
    entries?.forEach((entry, _) => {
        if (
            entry.type === EntryType.EntryConfChange ||
            entry.type === EntryType.EntryConfChangeV2
        ) {
            n++;
        }
    });
    return n;
}

function releasePendingReadIndexMessages(r: Raft) {
    if (!r.committedEntryInCurrentTerm()) {
        r.logger.errorf(
            "pending MsgReadIndex should be released only after first commit in current term"
        );
        return;
    }

    const msgs = r.pendingReadIndexMessages;
    r.pendingReadIndexMessages = [];

    if (msgs) {
        msgs.forEach((m, _) => {
            sendMsgReadIndexResponse(r, m);
        });
    }
}

function sendMsgReadIndexResponse(r: Raft, m: Message) {
    // thinking: use an internally defined context instead of the user given
    // context. We can express this in terms of the term and index instead of a
    // user-supplied value. This would allow multiple reads to piggyback on the
    // same message.
    switch (r.readOnly.option) {
        // If more than the local vote is needed, go through a full broadcast.
        case ReadOnlyOption.ReadOnlySafe:
            r.readOnly.addRequest(r.raftLog.committed, m);
            // The local node automatically acks the request.
            r.readOnly.recvAck(r.id, m.entries[0].data);
            r.bcastHeartbeatWithCtx(m.entries[0].data);
            break;
        case ReadOnlyOption.ReadOnlyLeaseBased:
            const resp = r.responseToReadIndexReq(m, r.raftLog.committed);
            if (resp.to !== ridnone) {
                r.send(resp);
            }
            break;
    }

}

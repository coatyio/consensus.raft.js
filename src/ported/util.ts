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

import { confChangesToString } from "./confchange/confchange";
import { Logger } from "./logger";
import { Ready, SoftState } from "./node";
import { ReadState } from "./readonly";
import { emptyRaftData, RaftData } from "../non-ported/raft-controller";
import {
    Message,
    Entry,
    MessageType,
    HardState,
    Snapshot,
    ConfState,
    ConfChangeI,
    ConfChange,
    ConfChangeV2,
    EntryType,
    ridnone,
} from "./raftpb/raft.pb";

/**
 * In Go an error has a zero value of null. To mimic this behavior while having
 * the strictNullChecks enabled, we have decided to declare this type which can
 * either be Error or null.
 */
export type NullableError = Error | null;

export function isLocalMsg(msgt: MessageType): boolean {
    return (
        msgt === MessageType.MsgHup ||
        msgt === MessageType.MsgBeat ||
        msgt === MessageType.MsgUnreachable ||
        msgt === MessageType.MsgSnapStatus ||
        msgt === MessageType.MsgCheckQuorum
    );
}

export function isResponseMsg(msgt: MessageType): boolean {
    return (
        msgt === MessageType.MsgAppResp ||
        msgt === MessageType.MsgVoteResp ||
        msgt === MessageType.MsgHeartbeatResp ||
        msgt === MessageType.MsgUnreachable ||
        msgt === MessageType.MsgPreVoteResp
    );
}

/**
 * voteResponseType maps vote and prevote message types to their corresponding
 * responses.
 */
export function voteRespMsgType(msgt: MessageType): MessageType {
    switch (msgt) {
        case MessageType.MsgVote:
            return MessageType.MsgVoteResp;
        case MessageType.MsgPreVote:
            return MessageType.MsgPreVoteResp;
        default:
            throw new Error("not a vote message: " + msgt);
    }
}

export function describeHardState(hs: HardState): string {
    return "Term:" + hs.term + (hs.vote !== ridnone ? " Vote:" + hs.vote : "") + " Commit:" + hs.commit;
}

export function describeSoftState(ss: SoftState): string {
    return "Lead:" + ss.lead + " State:" + ss.raftState;
}

export function describeReadState(rs: ReadState): string {
    return "Index:" + rs.index + " RequestCtx:" + rs.requestCtx;
}

export function describeConfState(state: ConfState): string {
    return (
        "Voters:" +
        "[" +
        state.voters +
        "]" +
        " VotersOutgoing:" +
        "[" +
        state.votersOutgoing +
        "]" +
        " Learners:" +
        "[" +
        state.learners +
        "]" +
        " LearnersNext:" +
        "[" +
        state.learnersNext +
        "]" +
        " AutoLeave:" +
        state.autoLeave
    );
}

export function describeSnapshot(snap: Snapshot): string {
    const m = snap.metadata;
    return (
        "Index:" +
        m.index +
        " Term:" +
        m.term +
        " ConfState:(" +
        describeConfState(m.confState) +
        ")"
    );
}

export function describeReady(rd: Ready, f: EntryFormatter): string {
    const buffer =
        (rd.softState !== null ? "SoftState:" + describeSoftState(rd.softState) : "") +
        (!rd.hardState.isEmptyHardState() ? "\nHardState:" + describeHardState(rd.hardState) : "") +
        (rd.readStates.length > 0
            ? "\nReadStates:[" + rd.readStates.map(describeReadState) + "]"
            : "") +
        (rd.entries.length > 0 ? "\nEntries:[\n" + describeEntries(rd.entries, f) + "]" : "") +
        (!rd.snapshot.isEmptySnap() ? "\nSnapshot:" + describeSnapshot(rd.snapshot) : "") +
        (rd.committedEntries.length > 0
            ? "\nCommittedEntries:[\n" + describeEntries(rd.committedEntries, f) + "]"
            : "") +
        (rd.messages.length > 0 ? "\nMessages:[\n" + describeMessages(rd.messages, f) + "]" : "");
    if (buffer.length > 0) {
        return "Ready MustSync=" + rd.mustSync + ":\n" + buffer;
    } else {
        return "<empty Ready>";
    }
}

/**
 * EntryFormatter can be implemented by the application to provide
 * human-readable formatting of entry data. Nil is a valid EntryFormatter and
 * will use a default format.
 */
export type EntryFormatter = (x: RaftData) => string;

/**
 * DescribeMessage returns a concise human-readable description of a Message for
 * debugging.
 */
export function describeMessage(m: Message, f: EntryFormatter): string {
    return (
        m.from +
        "->" +
        m.to +
        " " +
        MessageType[m.type] +
        " Term:" +
        m.term +
        " Log:" +
        m.logTerm +
        "/" +
        m.index +
        (m.reject ? " Rejected (Hint: " + m.rejectHint + ")" : "") +
        (m.commit !== 0 ? " Commit:" + m.commit : "") +
        (m.entries.length > 0
            ? " Entries:[" + m.entries.map((e) => describeEntry(e, f)) + "]"
            : "") +
        (!m.snapshot.isEmptySnap() ? " Snapshot:" + describeSnapshot(m.snapshot) : "")
    );
}

export function describeMessages(ms: Message[], f: EntryFormatter): string {
    return ms.map((m) => describeMessage(m, f)).join("\n");
}

/**
 * PayloadSize is the size of the payload of this Entry. Notably, it does not
 * depend on its Index or Term.
 */
export function payloadSize(e: Entry): number {
    return e.data === emptyRaftData ? 0 : JSON.stringify(e.data).length;
}

/**
 * DescribeEntry returns a concise human-readable description of an Entry for
 * debugging.
 */
export function describeEntry(e: Entry, f: EntryFormatter | null): string {
    if (!f) {
        f = (data: RaftData) => {
            if (typeof data === "object") {
                return JSON.stringify(data);
            } else {
                return String(data);
            }
        };
    }

    const formatConfChange = (cc: ConfChangeI) => {
        // RaftGo-TODO(tbg): give the EntryFormatter a type argument so that it
        // gets a chance to expose the Context.
        return confChangesToString(cc.asV2().changes);
    };

    let formatted = "";
    switch (e.type) {
        case EntryType.EntryNormal:
            formatted = f(e.data);
            break;
        case EntryType.EntryConfChange:
            formatted = formatConfChange(new ConfChange(e.data));
            break;
        case EntryType.EntryConfChangeV2:
            formatted = formatConfChange(new ConfChangeV2(e.data));
            break;
    }

    if (formatted !== "") {
        formatted = " " + formatted;
    }

    return e.term + "/" + e.index + " " + EntryType[e.type] + formatted;
}

/**
 * DescribeEntries calls DescribeEntry for each Entry, adding a newline to each.
 */
export function describeEntries(ents: Entry[], f: EntryFormatter): string {
    return ents.map((e) => describeEntry(e, f)).join("\n");
}

export function limitSize(ents: Entry[], maxSize: number): Entry[] {
    if (ents.length === 0) {
        return ents;
    }
    let size = JSON.stringify(ents[0]).length;
    let limit = 1;
    for (; limit < ents.length; limit++) {
        size += JSON.stringify(ents[limit]).length;

        if (size > maxSize) {
            break;
        }
    }
    return ents.slice(0, limit);
}

export function assertConfStatesEquivalent(l: Logger, cs1: ConfState, cs2: ConfState): void {
    const potentialError = cs1.equivalent(cs2);
    if (potentialError !== null) {
        l.panicf(potentialError.message);
    }
}

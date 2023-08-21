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

import { SoftState } from "./node";
import { Raft, StateType } from "./raft";
import { HardState, rid, ridnone } from "./raftpb/raft.pb";
import { Progress } from "./tracker/progress";
import { Config } from "./tracker/tracker";

/**
 * Status contains information about this Raft peer and its view of the system.
 * The Progress is only populated on the leader.
 */
export class Status {
    basicStatus: BasicStatus;
    config: Config;
    progress: Map<rid, Progress>;

    constructor(
        param: {
            basicStatus?: BasicStatus;
            config?: Config;
            progress?: Map<rid, Progress>;
        } = {}
    ) {
        this.basicStatus = param.basicStatus ?? new BasicStatus();
        this.config = param.config ?? new Config();
        // only populated on leader
        this.progress = param.progress ?? new Map<rid, Progress>();
    }

    public static getStatus(r: Raft): Status {
        let progress;
        if (r.state === StateType.StateLeader) {
            progress = this.getProgressCopy(r);
        }
        const s: Status = new Status({
            basicStatus: BasicStatus.getBasicStatus(r),
            config: r.prs.config.clone(),
            progress: progress,
        });
        return s;
    }

    /**
     * getStatus gets a copy of the current raft status.
     */
    static getProgressCopy(r: Raft): Map<rid, Progress> {
        const m = new Map<rid, Progress>();
        r.prs.visit((id: rid, pr: Progress) => {
            m.set(id, new Progress(pr));
        });
        return m;
    }
}

/**
 * BasicStatus contains basic information about the Raft peer. It does not
 * allocate.
 */
export class BasicStatus {
    id: rid;

    // Realized as embedded structs
    hardState: HardState;
    softState: SoftState;

    applied: number;
    leadTransferee: rid;

    constructor(
        param: {
            id?: rid;
            hardState?: HardState;
            softState?: SoftState;
            applied?: number;
            leadTransferee?: rid;
        } = {}
    ) {
        this.id = param?.id ?? ridnone;
        this.hardState = param?.hardState ?? new HardState();
        this.softState = param?.softState ?? new SoftState();
        this.applied = param?.applied ?? 0;
        this.leadTransferee = param?.leadTransferee ?? ridnone;
    }

    public static getBasicStatus(r: Raft): BasicStatus {
        const id = r.id;
        const leadTransferee = r.leadTransferee;
        const hardState = r.hardState();
        const softState = r.softState();
        const applied = r.raftLog.applied;

        return new BasicStatus({
            id: id,
            hardState: hardState,
            softState: softState,
            applied: applied,
            leadTransferee: leadTransferee,
        });
    }
}

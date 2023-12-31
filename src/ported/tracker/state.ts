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

/**
 * ProgressStateType is the state of a tracked follower.
 */
export enum ProgressStateType {
    /**
     * StateProbe indicates a follower whose last index isn't known. Such a
     * follower is "probed" (i.e. an append sent periodically) to narrow down
     * its last index. In the ideal (and common) case, only one round of probing
     * is necessary as the follower will react with a hint. Followers that are
     * probed over extended periods of time are often offline.
     */
    StateProbe,

    /**
     * StateReplicate is the state steady in which a follower eagerly receives
     * log entries to append to its log.
     */
    StateReplicate,

    /**
     * StateSnapshot indicates a follower that needs log entries not available
     * from the leader's Raft log. Such a follower needs a full snapshot to
     * return to StateReplicate.
     */
    StateSnapshot,
}

const strmap = ["StateProbe", "StateReplicate", "StateSnapshot"];

export function StateTypeToString(st: ProgressStateType): string {
    return strmap[st];
}

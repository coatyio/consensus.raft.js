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

import { ConfState, rid } from "./raft.pb";
import { describeConfState, NullableError } from "../util";

/**
 * confStateEquivalent returns null if the inputs describe the same
 * configuration. On mismatch, returns a descriptive error showing the
 * differences.
 */
export function confStateEquivalent(cs1: ConfState, cs2: ConfState): NullableError {
    function returnCopyWithSortedFields(cs: ConfState): ConfState {
        const s = (a: rid, b: rid) => ('' + a).localeCompare(b);

        const sortedVoters = [...cs.voters].sort(s);
        const sortedLearners = [...cs.learners].sort(s);
        const sortedVotersOutgoing = [...cs.votersOutgoing].sort(s);
        const sortedLearnersNext = [...cs.learnersNext].sort(s);
        return new ConfState({
            voters: sortedVoters,
            learners: sortedLearners,
            votersOutgoing: sortedVotersOutgoing,
            learnersNext: sortedLearnersNext,
            autoLeave: cs.autoLeave,
        });
    }

    /**
     * NOTE: a and b are not allowed to be undefined!
     */
    function arrEqual(a: rid[], b: rid[]): boolean {
        if (a.length !== b.length) {
            return false;
        }
        for (let i = 0; i < a.length; i++) {
            if (a[i] !== b[i]) {
                return false;
            }
        }
        return true;
    }

    const sort1 = returnCopyWithSortedFields(cs1);
    const sort2 = returnCopyWithSortedFields(cs2);

    if (
        !arrEqual(sort1.voters, sort2.voters) ||
        !arrEqual(sort1.learners, sort2.learners) ||
        !arrEqual(sort1.votersOutgoing, sort2.votersOutgoing) ||
        !arrEqual(sort1.learnersNext, sort2.learnersNext) ||
        sort1.autoLeave !== sort2.autoLeave
    ) {
        return new Error(
            "ConfStates not equivalent after sorting:\n" +
            describeConfState(sort1) +
            "\n" +
            describeConfState(sort2) +
            "\n" +
            "Inputs were:\n" +
            describeConfState(cs1) +
            "\n" +
            describeConfState(cs2)
        );
    }
    return null;
}

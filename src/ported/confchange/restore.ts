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

import { Changer } from "./confchange";
import { ConfChangeSingle, ConfChangeType, ConfState, rid } from "../raftpb/raft.pb";
import { Progress, ProgressMap } from "../tracker/progress";
import { Config } from "../tracker/tracker";
import { NullableError } from "../util";

/**
 * toConfChangeSingle translates a conf state into 1) a slice of operations
 * creating first the config that will become the outgoing one, and then the
 * incoming one, and b) another slice that, when applied to the config resulted
 * from 1), represents the ConfState.
 */
export function toConfChangeSingle(cs: ConfState): [ConfChangeSingle[], ConfChangeSingle[]] {
    // Example to follow along this code: voters=(1 2 3) learners=(5)
    // outgoing=(1 2 4 6) learners_next=(4)
    //
    // This means that before entering the joint config, the configuration had
    // voters (1 2 4 6) and perhaps some learners that are already gone. The new
    // set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6) are no
    // longer voters; however 4 is poised to become a learner upon leaving the
    // joint state. We can't tell whether 5 was a learner before entering the
    // joint config, but it doesn't matter (we'll pretend that it wasn't).
    //
    // The code below will construct outgoing = add 1; add 2; add 4; add 6
    // incoming = remove 1; remove 2; remove 4; remove 6 add 1;    add 2;    add
    // 3; add-learner 5; add-learner 4;
    //
    // So, when starting with an empty config, after applying 'outgoing' we have
    //
    //   quorum=(1 2 4 6)
    //
    // From which we enter a joint state via 'incoming'
    //
    //   quorum=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)
    //
    // as desired.
    const outChange: ConfChangeSingle[] = [];
    const inChange: ConfChangeSingle[] = [];

    for (const id of cs.votersOutgoing ? cs.votersOutgoing : []) {
        // If there are outgoing voters, first add them one by one so that the
        // (non-joint) config has them all.
        outChange.push({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: id,
        });
    }

    // We're done constructing the outgoing slice, now on to the incoming one
    // (which will apply on top of the config created by the outgoing slice).

    // First, we'll remove all of the outgoing voters.
    for (const id of cs.votersOutgoing ? cs.votersOutgoing : []) {
        inChange.push({
            type: ConfChangeType.ConfChangeRemoveNode,
            nodeId: id,
        });
    }

    // Then we'll add the incoming voters and learners.
    for (const id of cs.voters ? cs.voters : []) {
        inChange.push({
            type: ConfChangeType.ConfChangeAddNode,
            nodeId: id,
        });
    }
    for (const id of cs.learners ? cs.learners : []) {
        inChange.push({
            type: ConfChangeType.ConfChangeAddLearnerNode,
            nodeId: id,
        });
    }

    // Same for LearnersNext; these are nodes we want to be learners but which
    // are currently voters in the outgoing config.
    for (const id of cs.learnersNext ? cs.learnersNext : []) {
        inChange.push({
            type: ConfChangeType.ConfChangeAddLearnerNode,
            nodeId: id,
        });
    }

    return [outChange, inChange];
}

function chain(
    chg: Changer,
    ...ops: ((c: Changer) => [Config, ProgressMap, NullableError])[]
): [Config, ProgressMap, NullableError] {
    for (const op of ops) {
        const [cfg, prs, err] = op(chg);
        if (err !== null) {
            return [new Config(), new Map<rid, Progress>(), err];
        }
        chg.tracker.config = cfg;
        chg.tracker.progress = prs;
    }
    return [chg.tracker.config, chg.tracker.progress, null];
}

/**
 * Restore takes a Changer (which must represent an empty configuration), and
 * runs a sequence of changes enacting the configuration described in the
 * ConfState. TO DO(tbg) it's silly that this takes a Changer. Unravel this by
 * making sure the Changer only needs a ProgressMap (not a whole Tracker) at
 * which point this can just take LastIndex and MaxInflight directly instead and
 * cook up the results from that alone.
 */
export function Restore(chg: Changer, cs: ConfState): [Config, ProgressMap, NullableError] {
    const [outgoing, incoming] = toConfChangeSingle(cs);

    const ops: ((localChg: Changer) => [Config, ProgressMap, NullableError])[] = [];

    if (outgoing.length === 0) {
        // No outgoing config, so just apply the incoming changes one by one.
        for (const cc of incoming) {
            const copyCC: ConfChangeSingle = {
                type: cc.type,
                nodeId: cc.nodeId,
            }; // loop-local copy

            ops.push((localChg: Changer): [Config, ProgressMap, NullableError] => {
                return localChg.simple(copyCC);
            });
        }
    } else {
        // The ConfState describes a joint configuration.
        //
        // First, apply all of the changes of the outgoing config one by one, so
        // that it temporarily becomes the incoming active config. For example,
        // if the config is (1 2 3)&(2 3 4), this will establish (2 3 4)&().
        for (const cc of outgoing) {
            const copyCC: ConfChangeSingle = {
                type: cc.type,
                nodeId: cc.nodeId,
            };

            ops.push((localChg: Changer): [Config, ProgressMap, NullableError] => {
                return localChg.simple(copyCC);
            });
        }

        // Now enter the joint state, which rotates the above additions into the
        // outgoing config, and adds the incoming config in. Continuing the
        // example above, we'd get (1 2 3)&(2 3 4), i.e. the incoming operations
        // would be removing 2,3,4 and then adding in 1,2,3 while transitioning
        // into a joint state.
        ops.push((localChg: Changer): [Config, ProgressMap, NullableError] => {
            return localChg.enterJoint(cs.autoLeave, ...incoming);
        });
    }

    return chain(chg, ...ops);
}

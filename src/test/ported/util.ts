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

import { RaftLog } from "../../ported/log";
import { EntryType } from "../../ported/raftpb/raft.pb";

// NOTE: In the original, this creates 2 temporary files and compares them using
// "diff -u"
export function diffu(a: string, b: string): string {
    if (a === b) {
        return "";
    } else {
        return `Not equal.\na: \n${a} \nb: \n${b}`;
    }
}

export function ltoa(l: RaftLog): string {
    let s = `committed: ${l.committed}\n`;
    s += `applied: ${l.applied}\n`;

    for (let i = 0; i < l.allEntries().length; i++) {
        const e = l.allEntries()[i];

        s += `#${i}: index: ${e.index}, term: ${e.term}, data: ${e.data}, type: ${EntryType[e.type]
            }\n`;
    }
    return s;
}

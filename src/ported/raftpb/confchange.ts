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

import { ConfChangeI, EntryType } from "./raft.pb";
import { RaftData } from "../../non-ported/raft-controller";

/**
 * MarshalConfChange calls Marshal on the underlying ConfChange or ConfChangeV2
 * and returns the result along with the corresponding EntryType.
 */
export function marshalConfChange(c: ConfChangeI): [EntryType, RaftData] {
    let typ: EntryType;
    let ccdata: RaftData;
    const a = c.asV1();
    if (a[1]) {
        typ = EntryType.EntryConfChange;
        ccdata = a[0];
    } else {
        const ccv2 = c.asV2();
        typ = EntryType.EntryConfChangeV2;
        ccdata = ccv2;
    }

    return [typ, ccdata];
}

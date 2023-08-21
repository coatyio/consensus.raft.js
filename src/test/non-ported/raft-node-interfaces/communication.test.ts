/*! Copyright 2023 Siemens AG

   Licensed under the Apache License, Version 2.0 (the "License"); you may not
   use this file except in compliance with the License. You may obtain a copy of
   the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   License for the specific language governing permissions and limitations under
   the License.
*/

import { CommunicationState, Components, Configuration, Container } from "@coaty/core";
import { filter, take, timeout } from "rxjs/operators";
import tap from "tap";

import { RaftData } from "../../../non-ported/raft-controller";
import { RaftCommunicationController } from "../../../non-ported/raft-node-interfaces/communication";
import { RaftInputProposal } from "../../../non-ported/raft-node-interfaces/state-machine-wrapper";
import { Entry, Message } from "../../../ported/raftpb/raft.pb";

tap.test("raft-communication RaftCommunicationController", async (t) => {
    const comController = startController();
    t.teardown(() => comController.container.shutdown());

    // Test connection to broker
    await t.resolves(
        new Promise<void>((resolve, reject) => {
            comController.communicationManager
                .observeCommunicationState()
                .pipe(
                    filter((state) => state === CommunicationState.Online),
                    take(1),
                    timeout(3000)
                )
                .subscribe(
                    (_) => resolve(),
                    (_) => reject()
                );
        })
    );

    // 4 Messages get sent to the same node
    await t.resolves(
        new Promise<void>((resolve, reject) => {
            const dataToBeSend = ["this", "is", "a", "test"];
            const msgCount = dataToBeSend.length;
            let counter = 0;
            comController
                .startReceivingRaftMessages("1", "")
                .pipe(timeout(5000))
                .forEach((msg) => {
                    const dataIndex = dataToBeSend.findIndex(
                        (data) => data === msg.entries[0].data
                    );
                    if (dataIndex < 0) {
                        reject();
                    } else {
                        dataToBeSend.splice(dataIndex, 1);
                    }
                    if (++counter === msgCount) {
                        resolve();
                    }
                })
                .catch((err) => reject());
            dataToBeSend.forEach((data) => {
                const msg = new Message({
                    to: "1",
                    entries: [new Entry({ data: data })],
                });
                comController.sendRaftMessage(msg, msg.to, "");
            });
        })
    );

    // Messages sent to node 2 shouldn't be received by node 1
    await t.resolves(
        new Promise<void>((resolve, reject) => {
            const dataToBeSend = ["this", "is", "a", "test"];
            const msgCount = dataToBeSend.length / 2;
            let counter = 0;
            comController
                .startReceivingRaftMessages("1", "")
                .pipe(timeout(5000))
                .forEach((msg) => {
                    const dataIndex = dataToBeSend.findIndex(
                        (data) => data === msg.entries[0].data
                    );
                    if (dataIndex < 0) {
                        reject();
                    } else {
                        dataToBeSend.splice(dataIndex, 1);
                    }
                    if (++counter === msgCount) {
                        resolve();
                    }
                })
                .catch((err) => reject());
            dataToBeSend.forEach((data, index) => {
                const msg = new Message({
                    to: index % 2 === 0 ? "42" : "1",
                    entries: [new Entry({ data: data })],
                });
                comController.sendRaftMessage(msg, msg.to, "");
            });
        })
    );

    // Test if RaftData is serialized+deserialized correctly by Coaty
    await t.resolves(
        new Promise<void>((resolve, reject) => {
            const data: RaftData = {
                x: "hello",
                y: 42,
                z: { z: [4, 2] },
            };
            const inputProposal: RaftInputProposal = {
                inputId: "1-2-3-4",
                originatorId: "1",
                data: data,
                isNoop: false,
            };
            const msgToBeSend = new Message({
                to: "1",
                entries: [new Entry({ data: inputProposal })],
            });
            comController
                .startReceivingRaftMessages("1", "")
                .pipe(take(1), timeout(5000))
                .forEach((msg) => {
                    t.strictSame(msg.entries[0].data, inputProposal);
                    resolve();
                })
                .catch((err) => reject());
            comController.sendRaftMessage(msgToBeSend, msgToBeSend.to, "");
        })
    );

    t.end();
});

function startController(): RaftCommunicationController {
    const components: Components = {
        controllers: {
            RaftCommunicationController,
        },
    };
    const configuration: Configuration = {
        common: {
            agentIdentity: { name: "test" },
        },
        communication: {
            brokerUrl: "mqtt://localhost:1888",
            shouldAutoStart: true,
        },
    };
    const container = Container.resolve(components, configuration);
    const communicationController = container.getController<RaftCommunicationController>("RaftCommunicationController");

    return communicationController;
}

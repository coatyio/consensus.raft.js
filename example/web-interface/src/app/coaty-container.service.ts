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

import { Injectable } from "@angular/core";
import { Components, Configuration, Container } from "@coaty/core";

import { RaftAgentController } from "./raft-agent-controller";
import { AGENT_INTERFACE_CONNECTION_NAMESPACE_PREFIX } from "../shared";

/**
 * An app-wide service that can instantiate a Coaty container with a
 * `RaftAgentController` to communicate with the corresponding
 * `WebInterfaceController` of the respective agent.
 */
@Injectable({
    providedIn: "root",
})
export class CoatyContainerService {
    instantiateContainer(id: string): Container {
        const components: Components = {
            controllers: {
                RaftAgentController,
            },
        };
        const configuration: Configuration = {
            common: {
                agentIdentity: { name: `Raft-Web-Interface-${id}` },
            },
            communication: {
                brokerUrl: "mqtt://localhost:9883",
                namespace: `${AGENT_INTERFACE_CONNECTION_NAMESPACE_PREFIX}.${id}`,
                shouldAutoStart: true,
            },
        };
        return Container.resolve(components, configuration);
    }
}

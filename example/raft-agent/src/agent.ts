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

import { RaftController, RaftControllerOptions } from "@coaty/consensus.raft";
import { Components, Configuration, Container } from "@coaty/core";
import Debug from "debug";

import { getDBFilePath } from "./db";
import { KVRaftStateMachine } from "./kv-state-machine";
import { AGENT_INTERFACE_CONNECTION_NAMESPACE_PREFIX } from "./shared";
import { WebInterfaceController } from "./web-interface-controller";

/**
 * Used for logging inside this Raft agent.
 */
let logger = Debug("raft-agent");

/**
 * Tries to parse the command line arguments. Prints usage and calls
 * `process.exit(1)` in case of invalid arguments.
 */
function parseCommandLineArgs(): [boolean, string] {
    if (process.argv.length === 4 && process.argv[2] === "-c") {
        // start agent and create cluster
        logger = logger.extend(process.argv[3]);
        logger("Starting agent");
        logger("Will create a new Raft cluster on connect");
        return [true, process.argv[3]];
    } else if (process.argv.length === 3 && process.argv[2] !== "-c") {
        // (re)start agent and join existing cluster
        logger = logger.extend(process.argv[2]);
        logger("(Re)starting agent");
        logger("Will try to join an existing Raft cluster on connect");
        return [false, process.argv[2]];
    } else {
        // Print usage and exit
        logger = logger.extend("help");
        logger(
            `Usage: %s %s [-c] id\n` +
            "   -c: Create a new cluster, must be specified for the first agent and must be omitted for all other agents in the cluster\n" +
            "   id: The unique id of the agent to be started\n\n" +
            "Run via npm:\n" +
            "   npm run create id\n" +
            "     id: The unique id of the first agent that creates the cluster\n" +
            "   npm run join id\n" +
            "     id: The unique id of all other agents that join the cluster",
            process.argv[0],
            process.argv[1]
        );
        process.exit(1);
    }
}

/*
 * All code that is needed to start up a `RaftController`.
 */
function startRaftController(shouldCreateCluster: boolean, id: string): RaftController {
    const components: Components = {
        controllers: {
            RaftController,
        },
    };
    const raftControllerOptions: RaftControllerOptions = {
        shouldCreateCluster: shouldCreateCluster,
        id: id,
        stateMachine: new KVRaftStateMachine(),
    };
    const configuration: Configuration = {
        common: {
            agentIdentity: { name: `Raft-Agent-${id}` },
        },
        communication: {
            brokerUrl: "mqtt://localhost:1883",
            namespace: `coaty.examples.raft.cluster`,
            shouldAutoStart: true,
        },
        controllers: {
            RaftController: raftControllerOptions,
        },
        databases: {
            raftdb: {
                adapter: "SqLiteNodeAdapter",
                connectionString: getDBFilePath(id),
            },
        },
    };
    const container = Container.resolve(components, configuration);
    return container.getController<RaftController>("RaftController");
}

/**
 * Starts a `WebInterfaceController` in it's own Coaty container.
 * `WebInterfaceController` and `RaftController` don't share the same container
 * since they both use different communication namespaces.
 */
function startWebInterfaceController(id: string): WebInterfaceController {
    const components: Components = {
        controllers: {
            WebInterfaceController,
        },
    };
    const configuration: Configuration = {
        common: {
            agentIdentity: { name: `Raft-Agent-${id}` },
        },
        communication: {
            brokerUrl: "mqtt://localhost:1883",
            namespace: `${AGENT_INTERFACE_CONNECTION_NAMESPACE_PREFIX}.${id}`,
            shouldAutoStart: true,
        },
    };
    const container = Container.resolve(components, configuration);
    return container.getController<WebInterfaceController>("WebInterfaceController");
}

/**
 * Parse command line arguments, start up `RaftController` and
 * `WebInterfaceController` and give reference to `RaftController` to
 * `WebInterfaceController`.
 */
function start() {
    const [shouldCreateCluster, id] = parseCommandLineArgs();
    const raftController = startRaftController(shouldCreateCluster, id);
    const webInterfaceController = startWebInterfaceController(id);
    webInterfaceController.assignRaftController(raftController, id, logger);
    logger("Control via web interface:");
    logger(`http://localhost:4200/${id}`);
}

start();

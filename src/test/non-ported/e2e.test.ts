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

import tap from "tap";

import {
    startController,
    testConnectionToBroker,
    NumberInputStateMachine,
    KVRaftStateMachine,
    SetInput,
    DeleteInput,
    cleanupTestDatabase,
} from "./util";
import { RaftController } from "../../non-ported/raft-controller";

tap.test("e2e test number state machine", async (t) => {
    // Initialize RaftControllers
    console.log("Initializing RaftControllers.");
    const agent1: RaftController = startController("1", new NumberInputStateMachine(), true);
    const agent2: RaftController = startController("2", new NumberInputStateMachine());
    const agent3: RaftController = startController("3", new NumberInputStateMachine());
    const agent4: RaftController = startController("4", new NumberInputStateMachine());

    // Test connection to broker
    console.log("Testing connection to broker.");
    const connected =
        (await testConnectionToBroker(agent1)) &&
        (await testConnectionToBroker(agent2)) &&
        (await testConnectionToBroker(agent3)) &&
        (await testConnectionToBroker(agent4));
    t.strictSame(connected, true, "Connection to broker successful");

    // Create + connect to RAFT cluster
    console.log("Creating RAFT cluster and connecting to it.");
    await Promise.all([
        agent1.connect().then(() => console.log("RaftController 1 joined the cluster.")),
        agent2.connect().then(() => console.log("RaftController 2 joined the cluster.")),
        agent3.connect().then(() => console.log("RaftController 3 joined the cluster.")),
        agent4.connect().then(() => console.log("RaftController 4 joined the cluster.")),
    ]);

    // Log all shared state changes
    agent1.observeState().subscribe((state) => {
        console.log("State: [" + state + "]");
    });

    // Propose [0...10]
    console.log("Proposing [0...10] from agent 3 and 2.");
    await Promise.all([
        agent3.propose(0),
        agent2.propose(1),
        agent3.propose(2),
        agent2.propose(3),
        agent3.propose(4),
        agent2.propose(5),
        agent3.propose(6),
        agent2.propose(7),
        agent3.propose(8),
        agent2.propose(9),
        agent3.propose(10),
    ]);

    // Propose [11...19]
    console.log("Proposing [11...19] from agent 1, 2 and 4.");
    await Promise.all([
        agent1.propose(11),
        agent1.propose(12),
        agent2.propose(13),
        agent4.propose(14),
        agent1.propose(15),
        agent2.propose(16),
        agent4.propose(17),
        agent4.propose(18),
        agent4.propose(19),
    ]);

    // Propose 20 and check end result
    console.log("Proposing 20 from agent 1.");
    const result = (await agent1.propose(20)) as number[];
    t.strictSame(
        result.sort((a, b) => a - b),
        [...Array(21).keys()],
        "Correct end result"
    );

    // Check consistency between agents' state-machines
    console.log("Checking consistency between logs.");
    const [result1, result2, result3, result4] = await Promise.all([
        agent1.getState(),
        agent2.getState(),
        agent3.getState(),
        agent4.getState(),
    ]);
    t.strictSame(result2, result1, "Agent 2 state consistent to agent 1.");
    t.strictSame(result3, result1, "Agent 3 state consistent to agent 1.");
    t.strictSame(result4, result1, "Agent 4 state consistent to agent 1.");

    // Disconnect from RAFT cluster
    console.log("Disconnecting from RAFT cluster.");
    await Promise.all([
        agent2.disconnect().then(() => console.log("RaftController 2 left the cluster.")),
        agent3.disconnect().then(() => console.log("RaftController 3 left the cluster.")),
        agent4.disconnect().then(() => console.log("RaftController 4 left the cluster.")),
    ]);
    // Leader should disconnect last to avoid long waiting times in this test
    // run. But also has to work the other way!
    await agent1.disconnect().then(() => console.log("RaftController 1 left the cluster."));

    // Shutdown Coaty containers after all tests are finished
    t.teardown(async () => {
        console.log("Shutting down Coaty Containers.");
        agent1.container.shutdown();
        agent2.container.shutdown();
        agent3.container.shutdown();
        agent4.container.shutdown();
        await new Promise(res => setTimeout(res, 1000));
    });
});

tap.test("e2e test kv store", async (t) => {
    // Initialize RaftControllers
    console.log("Initializing RaftControllers.");
    const agent1: RaftController = startController("1", new KVRaftStateMachine(), true);
    const agent2: RaftController = startController("2", new KVRaftStateMachine());

    // Test connection to broker
    console.log("Testing connection to broker.");
    const connected =
        (await testConnectionToBroker(agent1)) && (await testConnectionToBroker(agent2));
    t.strictSame(connected, true, "Connection to broker successful");

    // Create + connect to RAFT cluster
    console.log("Creating RAFT cluster and connecting to it.");
    await Promise.all([
        agent1.connect().then(() => console.log("RaftController 1 joined the cluster.")),
        agent2.connect().then(() => console.log("RaftController 2 joined the cluster.")),
    ]);

    // Log all shared state changes
    agent1.observeState().subscribe((state) => {
        console.log("State: " + JSON.stringify(state));
    });

    // Propose 2 entries
    console.log("Proposing 2 new kv-pairs.");
    await Promise.all([
        agent1.propose(SetInput("agent1", "1")),
        agent2.propose(SetInput("agent2", "2")),
    ]);

    // Delete kv-pair with key agent1
    console.log("Deleting kv-pair with key agent1.");
    await agent1.propose(DeleteInput("agent1"));

    // Check consistency between states
    console.log("Checking consistency between logs.");
    const [result1, result2] = await Promise.all([agent1.getState(), agent2.getState()]);
    t.strictSame(result2, result1, "Agent 2 state consistent to agent 1.");

    // Disconnect from RAFT cluster
    console.log("Disconnecting from RAFT cluster.");
    await agent2.disconnect().then(() => console.log("Controller 2 left the cluster."));
    // Leader should disconnect last to avoid long waiting times in this test
    // run But also has to work the other way!!
    await agent1.disconnect().then(() => console.log("Controller 1 left the cluster."));

    // Shutdown Coaty containers after all tests are finished
    t.teardown(async () => {
        console.log("Shutting down Coaty Containers.");
        agent1.container.shutdown();
        agent2.container.shutdown();
        await new Promise(res => setTimeout(res, 1000));
    });
});

tap.test("e2e test connect and disconnect (fast multiple times)", async (t) => {
    // Initialize RaftController
    console.log("Initializing RaftControllers.");
    const agent1: RaftController = startController("1", new NumberInputStateMachine(), true);

    // Test connection to broker
    console.log("Testing connection to broker.");
    const connected = await testConnectionToBroker(agent1);
    t.strictSame(connected, true, "Connection to broker successful");

    // Connect + propose + disconnect 5 times
    console.log("Trying to connect + disconnect 5 times.");
    let x = 0;
    for (let i = 0; i < 5; i++) {
        await agent1.connect();
        console.log("Connected");
        await agent1.disconnect();
        console.log("Disconnected");
        x++;
    }
    t.strictSame(x, 5, "successfully connected + disconnected 10 times");

    // Shutdown Coaty container after all tests are finished
    t.teardown(async () => {
        console.log("Shutting down Coaty Container.");
        agent1.container.shutdown();
        await new Promise(res => setTimeout(res, 1000));
    });
});

tap.test("e2e test connect, propose and disconnect (fast multiple times)", async (t) => {
    // Initialize RaftController
    console.log("Initializing RaftControllers.");
    const agent1: RaftController = startController("1", new NumberInputStateMachine(), true);

    // Test connection to broker
    console.log("Testing connection to broker.");
    const connected = await testConnectionToBroker(agent1);
    t.strictSame(connected, true, "Connection to broker successful");

    // Connect + propose + disconnect 5 times
    console.log("Trying to connect + propose + disconnect 5 times.");
    let x = 0;
    for (let i = 0; i < 5; i++) {
        await agent1.connect();
        console.log("Connected");
        const result = await agent1.propose(42);
        t.strictSame(result[0], 42, "Propose successful");
        console.log("Proposed");
        await agent1.disconnect();
        console.log("Disconnected");
        x++;
    }
    t.strictSame(x, 5, "successfully connected + proposed + disconnected 5 times");

    // Shutdown Coaty container after all tests are finished
    t.teardown(async () => {
        console.log("Shutting down Coaty Container.");
        agent1.container.shutdown();
        await new Promise(res => setTimeout(res, 1000));
    });
});

tap.test("e2e test start 1 2 3 propose kill 1 propose restart 1", async (t) => {
    // Initialize RaftControllers
    console.log("Initializing RaftControllers.");
    const agent1: RaftController = startController("1", new KVRaftStateMachine(), true);
    const agent2: RaftController = startController("2", new KVRaftStateMachine());
    const agent3: RaftController = startController("3", new KVRaftStateMachine());

    // Test connection to broker
    console.log("Testing connection to broker.");
    const connected =
        (await testConnectionToBroker(agent1)) &&
        (await testConnectionToBroker(agent2)) &&
        (await testConnectionToBroker(agent3));
    t.strictSame(connected, true, "Connection to broker successful");

    // Create + connect to RAFT cluster
    console.log("Creating RAFT cluster and connecting to it.");
    await Promise.all([
        agent1.connect().then(() => console.log("RaftController 1 joined the cluster.")),
        agent2.connect().then(() => console.log("RaftController 2 joined the cluster.")),
        agent3.connect().then(() => console.log("RaftController 3 joined the cluster.")),
    ]);

    // Propose 43 with 3
    console.log("Proposing 43 with agent 3.");
    await agent3.propose(SetInput("foo", "43"));

    // Kill 1
    console.log("Killing/stopping agent 1.");
    await agent1.stop();

    // Propose 42 with 2
    console.log(
        "Proposing 42 with agent 2."
    );
    await agent2.propose(SetInput("foo", "42"));

    // Restart 1
    console.log("Restarting agent 1.");
    await agent1.connect();

    // Check end state and consistency between agents' state-machines
    console.log("Checking end state and consistency between logs.");
    const [result1, result2, result3] = await Promise.all([
        agent1.getState(),
        agent2.getState(),
        agent3.getState(),
    ]);
    t.strictSame(result2, result1, "Agent 2 state consistent to agent 1.");
    t.strictSame(result3, result1, "Agent 3 state consistent to agent 1.");
    t.strictSame(result1, [["foo", "42"]], "Distributed state is correct.");

    // Disconnect from RAFT cluster
    console.log("Disconnecting from RAFT cluster.");
    // Not the leader so disconnect first to avoid longer waiting time. A Also
    // has to work if all disconnect at the same time!!
    await agent1.disconnect().then(() => console.log("RaftController 1 left the cluster."));
    await Promise.all([
        agent2.disconnect().then(() => console.log("RaftController 2 left the cluster.")),
        agent3.disconnect().then(() => console.log("RaftController 3 left the cluster.")),
    ]);

    // Shutdown Coaty containers after all tests are finished
    t.teardown(async () => {
        console.log("Shutting down Coaty Containers.");
        agent1.container.shutdown();
        agent2.container.shutdown();
        agent3.container.shutdown();
        await new Promise(res => setTimeout(res, 1000));
    });
});

tap.test("e2e test with 2 clusters", async (t) => {
    // Initialize RaftController
    console.log("Initializing RaftControllers.");
    const agent11: RaftController = startController("1", new NumberInputStateMachine(), true, "cluster 1");
    const agent12: RaftController = startController("2", new NumberInputStateMachine(), false, "cluster 1");
    const agent21: RaftController = startController("1", new NumberInputStateMachine(), true, "cluster 2");
    const agent22: RaftController = startController("2", new NumberInputStateMachine(), false, "cluster 2");

    // Test connection to broker
    console.log("Testing connection to broker.");
    const connected =
        (await testConnectionToBroker(agent11)) &&
        (await testConnectionToBroker(agent12)) &&
        (await testConnectionToBroker(agent21)) &&
        (await testConnectionToBroker(agent22));
    t.strictSame(connected, true, "Connection to broker successful");

    // Create + connect to RAFT clusters
    console.log("Creating RAFT clusters and connecting to them.");
    await Promise.all([
        agent11.connect().then(() => console.log("RaftController 11 joined cluster 1.")),
        agent12.connect().then(() => console.log("RaftController 12 joined cluster 1.")),
        agent21.connect().then(() => console.log("RaftController 21 joined cluster 2.")),
        agent22.connect().then(() => console.log("RaftController 22 joined cluster 2.")),
    ]);

    // Cluster 1
    console.log("Proposing in cluster 1");
    await agent11.propose(1);
    await agent12.stop(); // Test Persistency by reconnecting
    await agent12.connect();
    const result1 = await agent12.getState();
    t.strictSame(result1, [1], "successfully proposed in cluster 1");

    // Cluster 2
    console.log("Proposing in cluster 2");
    await agent21.propose(2);
    await agent22.stop(); // Test Persistency by reconnecting
    await agent22.connect();
    const result2 = await agent22.getState();
    t.strictSame(result2, [2], "successfully proposed in cluster 2");

    // Disconnect from RAFT cluster
    console.log("Disconnecting from RAFT cluster.");
    await Promise.all([
        agent11.disconnect().then(() => console.log("RaftController 11 left the cluster.")),
        agent12.disconnect().then(() => console.log("RaftController 12 left the cluster.")),
        agent21.disconnect().then(() => console.log("RaftController 21 left the cluster.")),
        agent22.disconnect().then(() => console.log("RaftController 22 left the cluster.")),
    ]);

    // Shutdown Coaty container after all tests are finished
    t.teardown(async () => {
        console.log("Shutting down Coaty Container.");
        agent11.container.shutdown();
        agent12.container.shutdown();
        agent21.container.shutdown();
        agent22.container.shutdown();
        await new Promise(res => setTimeout(res, 1000));
    });
});

tap.test("e2e test final cleanup of databases", (t) => {
    cleanupTestDatabase("1");
    cleanupTestDatabase("2");
    cleanupTestDatabase("3");
    cleanupTestDatabase("4");

    t.end();
});

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

const { fork } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");
const mqtt = require("mqtt");

const testContextFile = path.join(os.tmpdir(), "consensus-raft-test-context.json");

function startBroker() {
    return new Promise(resolve => {
        const brokerPort = 1888;
        const brokerUrls = [
            `mqtt://127.0.0.1:${brokerPort}`,
            `ws://127.0.0.1:${brokerPort + 8000}`,
        ];
        const child = fork("./node_modules/@coaty/core/scripts/coaty-scripts.js", ["broker",
            "--port", `${brokerPort}`,
            "--nobonjour",
            // "--verbose",
        ], {
            cwd: process.cwd(),
            detached: true,
            windowsHide: true,
            stdio: "ignore",
        });

        fs.writeFileSync(testContextFile, JSON.stringify({
            brokerPid: child.pid,
            shouldTerminateBroker: true,
            canStopAndRestartBrokerWhileTesting: true,
            // @todo update as soon as aedes broker supports MQTT 5.0
            supportsMqtt5: false,
            brokerUrls,
        }));

        child.connected && child.disconnect();
        child.unref();

        awaitBrokerStarted(brokerUrls, resolve);
    });
}

function awaitBrokerStarted(brokerUrls, resolve) {
    // Await broker up and accepting connections.
    const client = mqtt.connect(brokerUrls[0]);
    client.once("connect", () => {
        client.end(true, () => {
            // Defer removal of event listeners to ensure proper clean up.
            setTimeout(() => client.removeAllListeners(), 0);
            resolve(brokerUrls);
        });
    });
}

async function stopBroker() {
    let options;
    try {
        options = JSON.parse(fs.readFileSync(testContextFile).toString());
        if (options.shouldTerminateBroker) {
            process.kill(options.brokerPid);
        }
    } catch { }
    try {
        fs.unlinkSync(testContextFile);
    } catch { }
    return options === undefined ? false : options.shouldTerminateBroker;
}

module.exports = {
    startBroker,
    stopBroker,
};

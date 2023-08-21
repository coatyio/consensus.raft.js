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

import Channel from "../../@nodeguy/channel/lib";
import tap from "tap";

tap.test("Simple send and receive", async (t) => {
    const channel = new Channel<number>();
    channel.push(42);
    const value = await channel.shift();
    t.equal(value, 42);
    t.end();
});

tap.test("send with receiver in select", async (t) => {
    const channel = new Channel<number>();
    channel.push(42);
    switch (await Channel.select([channel.shift()])) {
        case channel:
            console.log(`Channel received value: ${channel.value()}.`);
            t.end();
            break;
    }
});

tap.test("send with receiver and default in select", async (t) => {
    const channel = new Channel<number>();

    // Send the value after 200 ms timeout
    setTimeout(() => {
        channel.push(42);
    }, 200);

    // In order to implement the default behavior, we need to create an additional channel and close it before
    const closed = Channel();
    closed.close();

    switch (await Channel.select([channel.shift(), closed.shift()])) {
        case channel:
            console.log(`${t.name}: Channel received value: ${channel.value()}.`);
            break;
        default:
            console.log(`${t.name}: Default case`);
            t.end();
            break;
    }
});

tap.test("send with default not executed", async (t) => {
    const channel = new Channel<number>();
    channel.push(42);

    // In order to implement the default behavior, we need to create an additional channel and close it before
    const closed = Channel();
    closed.close();

    setTimeout(async () => {
        switch (await Channel.select([channel.shift(), closed.shift()])) {
            case channel:
                console.log(`${t.name}: Channel received value: ${channel.value()}.`);
                break;
            default:
                console.log(`${t.name}: Default case`);
                t.end();
                break;
        }
    }, 2000);
});

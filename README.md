# Raft Consensus Algorithm over Coaty in TypeScript/JavaScript

[![Powered by Coaty 2](https://img.shields.io/badge/Powered%20by-Coaty%202-FF8C00.svg)](https://coaty.io)
[![TypeScript](https://img.shields.io/badge/Source%20code-TypeScript-007ACC.svg)](http://www.typescriptlang.org/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202-blue.svg)](https://opensource.org/licenses/apache-2.0)
[![release](https://img.shields.io/badge/release-Conventional%20Commits-yellow.svg)](https://conventionalcommits.org/)
[![npm version](https://badge.fury.io/js/@coaty%2Fconsensus.raft.js.svg)](https://www.npmjs.com/package/@coaty/consensus.raft)

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Getting started](#getting-started)
  - [Defining a custom replicated state machine](#defining-a-custom-replicated-state-machine)
  - [Bootstrapping a new node](#bootstrapping-a-new-node)
  - [Accessing and modifying the replicated state](#accessing-and-modifying-the-replicated-state)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)
- [Credits](#credits)

## Overview

This project contains a TypeScript implementation of the [Raft Consensus
Algorithm](https://raft.github.io/). Raft is a protocol with which a cluster of
nodes can maintain a replicated state machine. The state machine is kept in sync
through the use of a replicated log. It was originally proposed in ["In Search
of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf) by
Diego Ongaro and John Ousterhout.

Many different implementations of this library are in existence, though none of
them that were originally developed for TypeScript/Javascript are being actively
maintained. The most popular implementation of the Raft algorithm can be found
inside the [etcd project](https://github.com/etcd-io/etcd/tree/main/raft), which
is written entirely in Go. The authors of this library have decided to use the
etcd Raft implementation to create a custom port from Go to TypeScript.

This library includes the ported code together with an additional layer on top
to provide better programmability to the user. The additional layer handles
aspects like persistency, communication between nodes, cluster configuration as
well as client interaction with reproposed inputs.

The additional programmability layer uses the [Coaty
Framework](https://coaty.io) for communication between nodes as well as some
aspects of persistency and cluster configuration which requires
[Node.js](https://nodejs.org/) JavaScript runtime (version 14 LTS or higher).

This project comes with complete documentation of its public
[API](https://coatyio.github.io/consensus.raft.js/api/index.html) including all
public and protected type and member definitions.

This project includes a complete
[example](https://github.com/coatyio/consensus.raft.js/tree/master/example)
that demonstrates best practices and typical usage patterns of the library.

## Installation

You can install the latest version of this library in your application as
follows:

```sh
npm install @coaty/consensus.raft
```

This npm package uses ECMAScript version `es2019` and module format `commonjs`.

## Getting started

A `RaftStateMachine` stores some form of state and defines inputs that can be
used to modify this state. Our library replicates an implementation of the
`RaftStateMachine` interface on multiple nodes in a cluster using Raft. This
means that each node has their own `RaftStateMachine` instance and applies the
same inputs in the same order to replicate the same shared state. We therefore
end up with one consistent state that can be accessed by all nodes in the
cluster.

### Defining a custom replicated state machine

The first step of getting started is to implement your own `RaftStateMachine`.
The implementation will depend on your use case. In this example we will create
a simple key value store that stores string key value pairs. Objects of type
`KVSet` and `KVDelete` will later be used as state machine inputs. `getState()`
and `setState()` should serialize and deserialize the stored state. For further
info have a look at the [API
documentation](https://coatyio.github.io/consensus.raft.js/api/interfaces/RaftStateMachine.html)
of the `RaftStateMachine` interface. The implementation of the string key value
store looks like this:

```typescript
class KVStateMachine implements RaftStateMachine {
    private _state = new Map<string, string>();

    processInput(input: RaftData) {
        if (input.type === "set") {
            // Add or update key value pair
            this._state.set(input.key, input.value);
        } else if (input.type === "delete") {
            // Delete key value pair
            this._state.delete(input.key);
        }
    }

    getState(): RaftData {
        // Convert from Map to JSON compatible object
        return Array.from(this._state);
    }

    setState(state: RaftData): void {
        // Convert from JSON compatible object to Map
        this._state = new Map(state);
    }
}

// Use an input object of this type to set or update a key value pair
type KVSet = { type: "set"; key: string; value: string; }

// Use an input object of this type to delete a key value pair
type KVDelete = { type: "delete"; key: string }
```

### Bootstrapping a new node

After you have implemented the `RaftStateMachine` interface you're ready to
bootstrap your first node. What you will end up with is a running
`RaftController` that can `connect()` to the Raft cluster and `propose()` inputs
of type `KVSet` and `KVDelete` as well as read the state with `getState()`.

You will need to specify a couple of options when bootstrapping a new
`RaftController`. Have a look at the [API
documentation](https://coatyio.github.io/consensus.raft.js/api/interfaces/RaftControllerOptions.html)
of the `RaftControllerOptions` interface for further info. The following code
will start a new controller, that will create a new Raft cluster on `connect()`:

```typescript
    // URL to the MQTT broker used by Coaty for the communication between nodes
    const brokerUrl = "mqtt://localhost:1883";

    // Path to the database file for persistent storage that will be created
    const databaseFilePath = "raft-agent-1.db"

    // Id that uniquely identifies the node in the cluster
    const id = "1";

    // Instance of your RaftStateMachine implementation
    const stateMachine = new KVStateMachine();

    // Create a new cluster on connect
    const shouldCreateCluster = true;

    // RaftControllerOptions
    const controllerOptions: RaftControllerOptions = { id, stateMachine, shouldCreateCluster };

    const components: Components = {
        controllers: {
            RaftController
        }
    };
    const configuration: Configuration = {
        communication: {
            brokerUrl: brokerUrl,
            shouldAutoStart: true,
        },
        controllers: {
            RaftController: controllerOptions
        },
        databases: {
            // Database key to persist Raft data must equal the one specified in
            // RaftControllerOptions.databaseKey (if not specified, defaults to "raftdb").
            // Database adapter must support local database operations, e.g. "SqLiteNodeAdapter".
            // Ensure path to database file exists and is accessible.
            raftdb: {
                adapter: "SqLiteNodeAdapter",
                connectionString: databaseFilePath,
            },
        },
    };
    const container = Container.resolve(components, configuration);

    // Represents the new node and can be used to access and modify the replicated state
    const raftController = container.getController<RaftController>("RaftController");
```

**Note**: The code above doesn't define the `RaftControllerOptions.cluster`
property. It therefore defaults to the empty string. Define this property if you
want to start multiple different clusters.

### Accessing and modifying the replicated state

Once you have bootstrapped the `RaftController` you can use it as follows:

```typescript
    // Connect the new node to the Raft cluster
    await raftController.connect();

    // Read the current replicated state
    const currentState = await raftController.getState();
    console.log("Current state: %s", JSON.stringify(currentState));
    // > Current state: []

    // Read the current cluster configuration
    const currentConfiguration = await raftController.getClusterConfiguration();
    console.log("Current configuration: %s", JSON.stringify(currentConfiguration));
    // > Current configuration: ["1"]

    // Subscribe to replicated state updates
    const observable1 = raftController.observeState();
    observable1.subscribe((state) => console.log("New state: %s", JSON.stringify(state)));
    // > New state: [["meaning of life","42"]]
    // > New state: [["meaning of life","42"],["coaty","io"]]
    // > New state: [["coaty","io"]]

    // To modify the replicated state use KVSet and KVDelete inputs
    const setInput1: KVSet = { type: "set", key: "meaning of life", value: "42" };
    const setInput2: KVSet = { type: "set", key: "coaty", value: "io" };
    const deleteInput: KVDelete = { type: "delete", key: "meaning of life" };

    const resultingState1 = await raftController.propose(setInput1);
    console.log("Resulting state: %s", JSON.stringify(resultingState1))
    // > Resulting state: [["meaning of life","42"]]

    const resultingState2 = await raftController.propose(setInput2);
    console.log("Resulting state: %s", JSON.stringify(resultingState2))
    // > Resulting state: [["meaning of life","42"],["coaty","io"]]
    
    const resultingState3 = await raftController.propose(deleteInput);
    console.log("Resulting state: %s", JSON.stringify(resultingState3))
    // > Resulting state: [["coaty","io"]]

    // Gracefully stop the node without disconnecting from the Raft cluster
    await raftController.stop();

    // Reconnect
    await raftController.connect();

    // Disconnect the node from the Raft cluster before shutting down
    await raftController.disconnect();
```

## Logging

This package supports logging by use of the npm
[debug](https://www.npmjs.com/package/debug) package. Logging output can be
filtered by specifying the `DEBUG` environment variable on startup, like this:

```sh
# Logs all supported log levels: INFO, WARNING, ERROR, DEBUG
DEBUG="consensus.raft:*"

# Logs ERROR logs only
DEBUG="consensus.raft:ERROR"

# Logs all supported log levels except INFO
DEBUG="consensus.raft:*,-consensus.raft:INFO"
```

## Contributing

If you like this package, please consider &#x2605; starring [the project on
GitHub](https://github.com/coatyio/consensus.raft.js). Contributions are welcome
and appreciated. If you wish to contribute please follow the Coaty developer
guidelines described
[here](https://github.com/coatyio/coaty-js/blob/master/CONTRIBUTING.md).

## License

Non-ported code and documentation copyright 2023 Siemens AG. Ported code and
documentation copyright 2016 The etcd Authors. @nodeguy/channel code and
documentation copyright 2017 David Braun.

Non-ported code is licensed under the [Apache
2.0](http://www.apache.org/licenses/LICENSE-2.0) license.

Non-ported documentation is licensed under a [Creative Commons
Attribution-ShareAlike 4.0 International
License](http://creativecommons.org/licenses/by-sa/4.0/).

@nodeguy/channel code and documentation is licensed under the [Apache
2.0](http://www.apache.org/licenses/LICENSE-2.0) license.

## Credits

Last but certainly not least, a big *Thank You!* to the folks who designed,
implemented and contributed to this library:

- Łukasz Zalewski [@lukasz-zet](https://github.com/lukasz-zet)
- Leonard Giglhuber [@LeonardGiglhuber](https://github.com/LeonardGiglhuber)
- Andreas Dachsberger
- Finn Capelle
- Felix Elfering

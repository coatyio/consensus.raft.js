# Example consensus.raft.js

This example demonstrates how the `RaftController` provided by package
`@coaty/consensus.raft` can be used to maintain a replicated state machine (RSM)
using the Raft protocol. The RSM implements a key value store that accepts
set/update and delete requests.

## Overview

The example consists of two components:

* Raft agent: A lightweight Coaty agent that uses the `RaftController` to
  implement a key value store that is shared and held consistent between all
  Raft agent instances.
* Web interface: An Angular web app that can be used to control each Raft agent
  instance and display their key value store.

## Installation and build

To begin with, make sure that a `Node.js` JavaScript runtime (version 14 LTS or
higher) is globally installed on your target machine. Download and installation
details can be found [here](http://nodejs.org/). Then install and build Raft
agent and web interface.

### Raft agent installation and build

```sh
cd raft-agent
npm install
npm run build
```

### Web interface installation

```sh
cd web-interface
npm install
```

## Run example

To run the example, execute the following npm run commands in separate
terminals:

```sh
# Start broker
cd raft-agent
npm run broker

# Start Raft agent with ID 1 that creates the Raft cluster
cd raft-agent
npm run create 1

# Start web interface
cd web-interface
npm run start
```

Your browser should automatically open and navigate to `http://localhost:4200/1`
after the web interface has started. This page can be used to control the
started Raft agent. You should see a connect button that lets you tell the Raft
agent to connect which implicitly creates a new Raft cluster.

## Starting additional Raft agents

Each agent needs it's own unique ID to be identifiable in the Raft cluster.
Starting two agents with the same ID leads to undefined behaviour. To start an
additional agent with id "hello" that will join the existing cluster created by
agent 1 run `npm run join hello` in a new terminal window inside the
`raft-agent` directory. You can exchange "hello" by any other ID that is unique
in the cluster to start more agents.

To control the new Raft agent navigate to `http://localhost:4200/hello`.

**Note**: An ID has to be unique in the cluster for all time and cannot be
reused. That means that a given ID **must** be used only once even if the old
Raft agent has disconnected from the cluster.

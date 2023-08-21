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

import Debug from "debug";

/**
 * Defines all logging functions that are used in this library. This interface
 * is a subset of the interface defined in the etcd raft library used as
 * template for this library.
 */
export interface Logger {
    infof(format: string, ...v: any[]): void;

    warningf(format: string, ...v: any[]): void;

    errorf(format: string, ...v: any[]): void;

    debugf(format: string, ...v: any[]): void;

    fatalf(format: string, ...v: any[]): void;

    panicf(format: string, ...v: any[]): void;
}

/**
 * DefaultLogger is an implementation of the Logger interface using the npm
 * debug package. The npm debug package provides a tiny JavaScript debugging
 * utility modelled after Node.js core's debugging technique. It works in
 * Node.js and web browsers.
 *
 * The format strings in the provided log functions support the following
 * directives: `%O` (object multi-line), `%o` (object single-line), `%s`
 * (string), `%d` (number), `%j` (JSON), `%%` (escape).
 *
 * To turn on debug output for this library, set the `DEBUG` environment
 * variable to `consensus.raft:*`.
 *
 * See https://github.com/visionmedia/debug for more info.
 */
class DefaultLogger implements Logger {
    infof = Debug("consensus.raft").extend("INFO");

    warningf = Debug("consensus.raft").extend("WARNING");

    errorf = Debug("consensus.raft").extend("ERROR");

    debugf = Debug("consensus.raft").extend("DEBUG");

    constructor() {
        this.errorf.color = "1";
        this.infof.color = "2";
        this.warningf.color = "3";
        this.debugf.color = "5";
    }

    fatalf(format: string, ...v: any[]) {
        this.errorf(format, ...v);
        process.exit(1);
    }

    panicf(format: string, ...v: any[]) {
        this.errorf(format, ...v);
        throw new Error();
    }

    enableDebug(): void {
        this.infof.enabled = true;
        this.warningf.enabled = true;
        this.errorf.enabled = true;
        this.debugf.enabled = true;
    }
}

/**
 * DiscardLogger is an implementation of the Logger interface that discards all
 * logs.
 */
class DiscardLogger implements Logger {
    infof = () => {
        // Discard
    };
    warningf = () => {
        // Discard
    };
    errorf = () => {
        // Discard
    };
    debugf = () => {
        // Discard
    };
    fatalf = () => {
        process.exit(1);
    };
    panicf = (format: string, ...v: any[]) => {
        throw new Error("format-string: " + format + ", " + "format-string-args: [" + v + "]");
    };
}

export const defaultLogger = new DefaultLogger() as Logger;
export const discardLogger = new DiscardLogger() as Logger;
let raftLogger = defaultLogger;

/**
 * Use `defaultLogger` or `discardLogger` from `logger.ts` as argument.
 */
export function setLogger(logger: Logger) {
    raftLogger = logger;
}

export function getLogger() {
    return raftLogger;
}

export function resetLogger() {
    raftLogger = defaultLogger;
}

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

/**
 * Inflights limits the number of MsgApp (represented by the largest index
 * contained within) sent to followers but not yet acknowledged by them. Callers
 * use Full() to check whether more messages can be sent, call Add() whenever
 * they are sending a new append, and release "quota" via FreeLE() whenever an
 * ack is received.
 */
export class Inflights {

    /**
     * the starting index in the buffer
     */
    start: number;

    /**
     * number of inflights in the buffer
     */
    _count: number; // _ to resolve conflict with count() function

    /**
     * the size of the buffer
     */
    size: number;

    /**
     * buffer contains the index of the last entry inside one message.
     */
    buffer: number[];

    constructor(param: { size?: number; start?: number; _count?: number; buffer?: number[] }) {
        this.size = param.size ? param.size : 0;
        this.start = param.start ? param.start : 0;
        this._count = param._count ? param._count : 0;
        this.buffer = param.buffer ? param.buffer : [];
    }

    /**
     * Clone returns an *Inflights that is identical to but shares no memory
     * with the receiver.
     */
    public clone(): Inflights {
        return new Inflights({
            size: this.size,
            start: this.start,
            _count: this._count,
            buffer: this.buffer.slice(),
        });
    }

    /**
     * Add notifies the Inflights that a new message with the given index is
     * being dispatched. Full() must be called prior to Add() to verify that
     * there is room for one more message, and consecutive calls to add Add()
     * must provide a monotonic sequence of indexes.
     */
    public add(inflight: number) {
        if (this.full()) {
            throw new Error("cannot add into a Full inflights");
        }

        let next = this.start + this._count;
        const size = this.size;
        if (next >= size) {
            next -= size;
        }
        if (next >= this.buffer.length) {
            this.grow();
        }
        this.buffer[next] = inflight;
        this._count++;

        if (this._count > this.size) {
            throw new Error("Number of inflights exceed capacity");
        }
    }

    /**
     * FreeLE frees the inflights smaller or equal to the given `to` flight.
     */
    public freeLE(to: number): void {
        if (this._count === 0 || to < this.buffer[this.start]) {
            // out of the left side of the window
            return;
        }

        let idx = this.start;
        let i: number = 0;
        for (; i < this._count; i++) {
            if (to < this.buffer[idx]) {
                // found the first large inflight
                break;
            }

            // increase index and maybe rotate
            const size = this.size;
            idx++;
            if (idx >= size) {
                idx -= size;
            }
        }

        // free i inflights and set new start index
        this._count -= i;
        this.start = idx;
        if (this._count === 0) {
            // inflights is empty, reset the start index so that we don't grow
            // the buffer unnecessarily
            this.start = 0;
        }
    }

    /**
     * FreeFirstOne releases the first inflight. This is a no-op if nothing is
     * inflight.
     */
    public freeFirstOne() {
        this.freeLE(this.buffer[this.start]);
    }

    /**
     * Full returns true if no more messages can be sent at the moment.
     */
    public full(): boolean {
        return this._count === this.size;
    }

    /**
     * Count returns the number of inflight messages.
     */
    public count(): number {
        return this._count;
    }

    /**
     * reset frees all inflights.
     */
    public reset(): void {
        this._count = 0;
        this.start = 0;
    }

    public toString(): string {
        const result =
            "start: " +
            this.start +
            ", " +
            "count: " +
            this._count +
            ", " +
            "size: " +
            this.size +
            ", " +
            "buffer: " +
            this.buffer;
        return result;
    }

    /**
     * Grow the inflight buffer by doubling up to inflights.size. We grow on
     * demand instead of preallocating to inflights.size to handle systems which
     * have thousands of Raft groups per process.
     */
    private grow(): void {
        let newSize = this.buffer.length * 2;
        if (newSize === 0) {
            newSize = 1;
        } else if (newSize > this.size) {
            newSize = this.size;
        }

        const newBuffer = new Array(newSize).fill(0);
        newBuffer.splice(0, this.buffer.length, ...this.buffer);
        this.buffer = newBuffer;
    }
}

/**
 * NewInflights sets up an Inflights that allows up to 'size' inflight messages.
 */
export function NewInflights(size: number): Inflights {
    return new Inflights({ size: size });
}

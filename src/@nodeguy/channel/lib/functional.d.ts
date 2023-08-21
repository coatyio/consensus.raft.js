// Copyright 2017 David Braun
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

// tslint:disable:array-type variable-name

type Combinations<A extends Array<any>, Acc extends Array<any> = []> =
    A extends [a: infer A, ...rest: infer Rest]
    ? [[...Acc, A], ...Combinations<Rest, [...Acc, A]>]
    : []

type Curried<T extends (...args: any) => any, LeftArgs extends Array<any> = [Parameters<T>[0]]> =
    T extends (...args: LeftArgs) => any
    ? T
    : T extends (...args: [...LeftArgs, ...infer V]) => infer R
    ? (...args: LeftArgs) => Curried<(...args: V) => R, LeftArgs>
    : T extends (a: infer A) => infer R
    ? (arg_last: A) => R
    : T extends (a: infer A, ...rest: infer V) => infer R
    ? ((arg_inner: A) => Curried<(...args: V) => R, LeftArgs>)
    : never

type CurriedVariants<
    T extends (...args: any) => any,
    C extends any[][] = Combinations<Parameters<T>>
> = T extends () => any
    ? [T]
    : { [Cn in keyof C]: Curried<T, Extract<C[Cn], C[number]>> };

type Named<QueryKey extends string, Value> = {
    [key in QueryKey]: Value;
}

type NamedCurriedVariants<Name extends string, T extends (...args: any) => any, V = CurriedVariants<T>> =
    { [C in keyof V]: C extends `${number}` ? Named<Name, V[C]> : never };

type UnionToIntersection<U> =
    (U extends any ? (k: U) => void : never) extends ((k: infer I) => void) ? I : never

type InterfaceValues<T> = T extends Record<any, infer Inner> ? Inner : never;

type ToFunctional<Func, ExtraArg> = Func extends (...a: infer U) => infer R ? (...a: [...U, ExtraArg]) => R : never;

type Functions = {
    [x: string]: (...args: any) => any
};

export type Typify<T> = { [K in keyof T]: T[K] };

export type Functionalify<T extends Typify<Functions>, ExtraArg> =
    UnionToIntersection<InterfaceValues<{
        [K in keyof T]: UnionToIntersection<NamedCurriedVariants<Extract<K, string>, ToFunctional<T[K], ExtraArg>>[number]>
    }>>

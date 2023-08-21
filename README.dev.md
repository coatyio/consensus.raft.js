# Developer Notes

## Testing

```sh
npm install
npm run build

# Run test suite without any Logger messages.
npm run test

# Run test suite with Logger messages except for log level INFO.
npm run test:detail
```

## Release

To release a new version of this package, run `npm run release`. This includes
automatic version bumping, generation of a conventional changelog based on git
commit history, git tagging and pushing the release, and publishing the package
on npm registry. For a dry test run, invoke `npm run release:dry`.

## Implementation Rationale

This implementation is based on the "release 3.5" of the [etcd Raft Go
library](https://github.com/etcd-io/etcd/tree/main/raft). Even though the
authors have decided to port the library to TypeScript in a very strict manner,
at some places different decisions were made that are not always consistent with
the original code. These decisions are documented at the respective places in
the code. The following general decisions were made:

1. Both options `strictNullChecks` and `strictPropertyInitialization` are set to
   true, to avoid mistakes that are later hard to reproduce and debug.
2. Constructor methods of classes and interfaces accept an object with the
   properties (JSON object). Then, the omitted properties are initialized to
   their zero-value, to match the behavior of Go. Zero values are:

    ```typescript
    // number
    0

    // boolean
    false

    // string
    ""

    // class/interface
    new ClassOrInterfaceName()

    // Array
    []

    // Map
    new Map<K, V>()

    // pointer
    const pointer: Typename | null = null;

    // enum
    // first case of enum
    ```

3. Byte slices are handled by using the `any` type.

## TODOs

- Potentially realize TODO inside state-machine-wrapper
- Abandon ConfChange and use ConfChangeV2 instead

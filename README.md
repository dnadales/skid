#  SImple Key-value store, Impefect, but Distributed 

An exercise on implementing a distributed key-value store in Haskell.

This project can be built using [`stack`](https://docs.haskellstack.org/en/stable/README/).

To run a node without installing the binaries you can use `stack`:

```haskell
stack exec skid -- -i -p 3023
```

The command above will spawn a node on the local-host, on port `3023`, and
start a REPL to interact with the key-value store. The following commands are
supported in this simple REPL:

- `peers`: list the known peers.
- `get k`: get the value (if any) associated with the key `k`.
- `put k v`: add the pair `(k, v)` to the map.
- `q` or `quit`: quit the REPL. Note that this won't stop the node. Just use
  Ctrl+C for that purpose.

To run a node in non-interactive mode omit the flag `-i`.

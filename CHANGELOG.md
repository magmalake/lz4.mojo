# Changelog

## Unreleased

## 0.1.4 - 2026-11-03

- The shim is found without `CONDA_PREFIX`: after `$CONDA_PREFIX/lib`,
  lz4 looks next to the executable, in its `../lib`, and on the binary's
  rpath (the environment `mojo build` ran in), so a binary built with
  `pixi run mojo build` runs from a plain shell. The search lives in
  `src/lz4/_dlopen.mojo`, shared with the other magmalake tins that load
  a library.
- A shim that can't be found is an Error raised by the lz4 call, naming
  the library and every place searched and saying to set `CONDA_PREFIX`.
  It used to abort.
- The fallback to `build/liblz4mojo.*` relative to the working directory
  is gone: it named a file only lz4.mojo's own checkout had, and loading a
  library from the working directory isn't safe.

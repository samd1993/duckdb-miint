# The SRA reader

`read_sra` and `read_ncbi_sra_sequences` (`src/read_sra.cpp`) share one decoder for NCBI's SRA
archives, written from the format: no NCBI code is linked or vendored (`THIRD_PARTY_LICENSES.md`).
Its contract is `fasterq-dump`'s output, byte for byte, and the tests hold it to committed oracles
that are never edited (`data/sra/README.md`).

## Layers

Each layer is free of DuckDB and has a Catch2 test file of its own (`test/cpp/test_Sra*.cpp`), so
the format can be tested against fixtures without a database.

| File | What it does |
|---|---|
| `sra_source` | `SraByteSource`: positional reads from a file, memory, or (in `read_sra.cpp`) a DuckDB `FileHandle`. `SraPrefetchingSource` wraps a remote one: fixed blocks fetched by a pool of threads, read-ahead, and an LRU cache, so one reader keeps many range requests in flight. |
| `sra_archive` | The KAR container (header and table of contents), a KDB database or bare table inside it, the `md/cur` metadata tree, and static columns, whose single value is kept in metadata. |
| `sra_column` | A physical column: its index (`idx`, the `idx0` journal, `idx1`/`idx2` block types), blob location, checksums (CRC32 or MD5, always verified), and the VDB blob framing: headers and page maps that give each row's length. |
| `sra_codecs` | Column encodings: `zip` (deflate), `izip`/`irzip` (integer planes with delta and two-series modes), and the 2na and 4na base packings. |
| `sra_name` | Spot names: the `skey` text index (a persisted trie with a projection from rows to keys) and `format_spot_name`. |
| `sra_run` | Classification (which runs are read, and the message for each kind refused), `SpotReader` (the logical columns as the run's schema defines them, spot by spot, with `fasterq-dump`'s checks), and routing (`RouteSpot`, `RowsForSpot`: which reads `fasterq-dump` writes and where). |

`src/read_sra.cpp` is the DuckDB layer: binding, the work-unit queue (`GlobalState::Claim`), the
run-failure policy shared with `read_ena_sequences`, and filling the output vectors.

## Things that are easy to get wrong

- **Classification goes by the tables and columns present, never by platform.** PacBio and Nanopore
  runs loaded from FASTQ are ordinary sequence tables and are read; their consensus databases are
  refused. Anything outside the table types and encodings the survey of public runs found is
  refused with `SraUnsupportedError` rather than guessed at. Refusals are `InvalidInputException`;
  damage (`SraFormatError`) is an `IOException`.
- **A logical column is chosen as the schema chooses it**: the first alternative whose inputs exist
  (`ORIGINAL_QUALITY` before `QUALITY`, `READ` as 2na bases ORed with `ALTREAD`, a column from its
  directory or else from a static value). Spot names follow the same rule; the comment on
  `SpotReader::Impl::ResolveNaming` lists the alternatives.
- **Order.** Output is in run and spot order with parallel decoding: each work unit (a range of
  spots, `miint_sra_spots_per_unit`, default 131072) gets a batch index when claimed, and
  `get_partition_data` hands it to DuckDB. Claims happen in order, so batch indexes do too.
- **Remote reads.** Throughput comes from concurrent range requests, not from bytes per request
  (`SraPrefetchingSource`): 2 MiB blocks, 32 fetch threads, 16 blocks of read-ahead per reader, and
  a cache sized from DuckDB's thread count. A claimed unit's blobs, and the next unit's, are hinted
  to the source as soon as it is claimed. Destroying the source drops its queue and waits only for
  requests in flight, which is what stops a `LIMIT`.

## Tests

- C++ (`[sra]`): each layer against the fixtures, with expected values read independently, and
  every fixture's spots, routing and names against the `fasterq-dump` oracles.
- `read_sra.test`: `read_fastx` over the oracles equals `read_sra`, in both modes.
- `read_sra_fastq_render.test`: the rows rendered as FASTQ match the md5 of every oracle file.
- `read_sra_real_data.test` (`SRA_REAL_DATA`): two whole real runs against `fasterq-dump`'s md5s.
- `read_ncbi_sra_sequences_mock.test`: the remote path against the test server, equal to `read_sra`.
- `read_ncbi_sra_sequences_live.test` (`SRA_AWS_AVAILABLE`, nightly): NCBI's bucket itself.

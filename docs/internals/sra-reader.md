# The SRA reader

`read_sra` and `read_ncbi_sra_sequences` (`src/read_sra.cpp`) share one decoder for NCBI's SRA
archives, written with NCBI's public-domain sources read as its reference. Most of it is written
from the format; the parts that are ports of NCBI code or follow it closely are marked in the code
and listed in `THIRD_PARTY_LICENSES.md`. Nothing of NCBI's is linked or vendored.
Its contract is `fasterq-dump`'s output, byte for byte, and the tests hold it to committed oracles
that are never edited (`data/sra/README.md`).

## Layers

Each layer is free of DuckDB and has a Catch2 test file of its own (`test/cpp/test_Sra*.cpp`), so
the format can be tested against fixtures without a database.

| File | What it does |
|---|---|
| `sra_source` | `SraByteSource`: positional reads from a file, memory, or (in `read_sra.cpp`) a DuckDB `FileHandle`. `SraFetchPool` fetches remote files in fixed blocks with a pool of threads, read-ahead and one LRU cache, shared by every file of a scan; each file is read through an `SraPrefetchingSource` view. |
| `sra_archive` | The KAR container (header and table of contents), a KDB database or bare table inside it, the `md/cur` metadata tree, and static columns, whose single value is kept in metadata. |
| `sra_column` | A physical column: its index (`idx`, the `idx0` journal, `idx1`/`idx2` block types), blob location, CRC32 blob checksums (always verified; MD5 blob checksums, which no public run in the survey used, are refused), and the VDB blob framing: headers and page maps that give each row's length. |
| `sra_codecs` | Column encodings: `zip` (deflate) and `izip`/`irzip` (integer planes with delta and two-series modes). |
| `sra_name` | Spot names: the `skey` text index (a persisted trie with a projection from rows to keys) and `format_spot_name`. |
| `sra_run` | Classification (which runs are read, and the message for each kind refused), `SpotReader` (the logical columns as the run's schema defines them, spot by spot, with `fasterq-dump`'s checks; this is where 2na bases are unpacked and ORed with 4na `ALTREAD`), and routing (`RouteSpot`, `RowsForSpot`: which reads `fasterq-dump` writes and where; `RowsOfSpot`: how many rows, from the layout alone). |

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
- **Row numbers.** `sequence_index` counts a run's rows from 1 with no gaps, but how many rows a
  spot makes is only known once it is routed, and units decode in parallel. So `Claim` counts each
  unit's rows as it hands it out, from `READ_LEN` and `READ_TYPE` alone (`SpotReader::ReadLayout`,
  `RowsOfSpot`, which share `RouteSpot`'s routing), and the unit's rows start where the last unit's
  ended. The count runs under the lock and reads only those two columns, which are small and often
  static; its cost on a remote run has not been measured. A one-read spot is routed on its `READ`
  length but counted on `READ_LEN`; `Execute` checks each unit makes the rows it was counted for
  and raises an `IOException` if not, so a one-read spot with `READ_LEN` 0 and bases is refused.
- **Remote reads.** Throughput comes from concurrent range requests, not from bytes per request:
  one stream from the bucket gave about 4 MB/s and 16-32 concurrent ranges 35-47 MB/s. A scan
  has one `SraFetchPool`, shared by all its remote runs, each read through an
  `SraPrefetchingSource`: 2 MiB blocks, 16 blocks of read-ahead per reader, one LRU cache. Its
  fetch threads are DuckDB's `threads`, at most 32 (`MAX_FETCH_THREADS`; 32 matched a 32-way plain
  download, more was not measured), so neither threads nor memory grow with the number of runs; a
  lower `threads` means fewer requests in flight, and slower reads. A claimed unit's
  blobs, and the next unit's, are hinted to the pool as soon as it is claimed. Closing a run drops
  its blocks; destroying the pool drops its queue and waits only for requests in flight, which is
  what stops a `LIMIT`. A WASM build without threads reads ranges directly instead.
- **Interrupts.** A reader waiting for a block checks DuckDB's interrupt every 100 ms
  (`INTERRUPT_POLL_INTERVAL`), so Ctrl-C and `max_execution_time` end its wait. A request already
  in flight inside httpfs cannot be cancelled: the query returns once httpfs gives up on it, after
  `http_timeout` and `http_retries` (about 2 minutes on a stalled transfer at the defaults; 6 s
  with `http_timeout = 5` and no retries).
- **The name index is shared.** Each decoding thread has its own `SpotReader`, but a run's `skey`
  index (tens of bytes per spot when a loader stores whole names) is read once and shared through
  `share_from`; `SkeyIndex` is immutable after construction for that reason.

## Measurements

On a campus network, SRR7761321 (1.35 GB `.sra`, 20.25 million spots), with
`CREATE TABLE ... AS SELECT * FROM read_ncbi_sra_sequences('SRR7761321')`:

- 2026-11-09, one fetch pool sized by `threads`, interleaved with a bare 32-way parallel download
  of the object (26-28 s): 30-32 s at `threads = 32`, 42-44 s at 16; 86 s at 8 in an earlier run.
- 2026-11-04/05, before the pool followed `threads` (32 fetch threads per run): 28-37 s at 8
  threads; about 161 s for the download plus `fasterq-dump`, and about 27 minutes for ENA's FASTQ
  that day. `read_sra` on the local file took 6-8 s. `LIMIT 10` returned in about 6 s, having
  received about 110 MB.

## Tests

- C++ (`[sra]`): each layer against the fixtures, with expected values read independently, and
  every fixture's spots, routing and names against the `fasterq-dump` oracles.
- `read_sra.test`: `read_fastx` over the oracles equals `read_sra`, in both modes.
- `read_sra_fastq_render.test`: the rows rendered as FASTQ match the md5 of every oracle file.
- `read_sra_real_data.test` (`SRA_REAL_DATA`): two whole real runs against `fasterq-dump`'s md5s.
- `read_ncbi_sra_sequences_mock.test`: the remote path against the test server, equal to `read_sra`.
- `read_ncbi_sra_sequences_live.test` (`SRA_AWS_AVAILABLE`, nightly): NCBI's bucket itself.

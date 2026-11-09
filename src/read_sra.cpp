#include "read_sra.hpp"

#include "ensure_httpfs.hpp"
#include "http_client_common.hpp"
#include "miint_log.hpp"
#include "miint_named_parameter.hpp"
#include "read_ena_sequences_policy.hpp"
#include "remote_file_helper.hpp"
#include "table_function_common.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/execution/partition_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <regex>

namespace duckdb {

using namespace miint::sra;

namespace {

// Consecutive spots decoded by one thread and emitted under one batch index. Large enough that
// an order-keeping sink holds few batches (docs/internals/duckdb-engine-notes.md, "Output order
// costs threads and memory"), small enough to spread a run over the threads. The setting
// miint_sra_spots_per_unit overrides it.
constexpr uint64_t DEFAULT_SPOTS_PER_UNIT = 128 * 1024;

// The most threads one scan decodes on (GlobalState::MaxThreads).
constexpr idx_t MAX_SCAN_THREADS = 16;
// The most range requests one scan keeps in flight (OpenTable).
constexpr idx_t MAX_FETCH_THREADS = 32;

// Random access to a .sra through DuckDB's file system, so local paths and (with httpfs) remote
// URLs are read the same way. FileHandle::Read at a position is safe from several threads when
// the handle is opened for parallel access.
class DuckDBSraSource : public SraByteSource {
public:
	DuckDBSraSource(FileSystem &fs, const std::string &path, bool direct_io)
	    : handle_(fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_PARALLEL_ACCESS |
	                                    (direct_io ? FileFlags::FILE_FLAGS_DIRECT_IO : FileOpenFlags()))),
	      size_(handle_->GetFileSize()) {
	}
	uint64_t Size() const override {
		return size_;
	}
	void ReadAt(uint64_t pos, uint8_t *buf, size_t n) const override {
		if (pos > size_ || n > size_ - pos) {
			throw SraFormatError("read past the end of the file");
		}
		handle_->Read(buf, n, pos);
	}

private:
	unique_ptr<FileHandle> handle_;
	uint64_t size_;
};

// A run's errors name the function and the run: a refusal (what the run is) is invalid input,
// damage is an I/O error.
[[noreturn]] void ThrowForRun(const SraScanData &bind, const std::string &accession, const std::exception &e) {
	std::string msg = bind.function + ": " + accession + ": " + e.what();
	if (dynamic_cast<const SraUnsupportedError *>(&e)) {
		throw InvalidInputException(msg);
	}
	throw IOException(msg);
}

// A run that could not be fetched: the I/O and HTTP errors DuckDB's file systems raise (a 404 is
// HTTP, a refused connection IO). Anything else (an interrupt, a deadline, an exhausted resource,
// a refused permission) is the query's failure, never a run's, and is not reworded as one.
bool IsFetchFailure(const std::exception &e) {
	auto type = ErrorData(e).Type();
	return type == ExceptionType::IO || type == ExceptionType::HTTP;
}

// Runs fn, rewording what it throws as the run's error: damage and refusals as ThrowForRun does,
// and a fetch that failed as an I/O error naming the run. Anything else is the query's failure and
// passes as is. Opening a run (Claim) has its own handling, since a run that cannot be opened may
// be skipped; once a run has been opened, a failure fails the query.
template <typename Fn>
void WithRunErrors(const SraScanData &bind, const std::string &accession, Fn &&fn) {
	try {
		fn();
	} catch (const SraFormatError &e) {
		ThrowForRun(bind, accession, e);
	} catch (const SraUnsupportedError &e) {
		ThrowForRun(bind, accession, e);
	} catch (const std::exception &e) {
		if (!IsFetchFailure(e)) {
			throw;
		}
		throw IOException(bind.function + ": " + accession + ": " + ErrorData(e).RawMessage());
	}
}

struct OpenRun {
	std::string location;
	std::string accession;
	std::shared_ptr<const SraTable> table;
	// Hints each claimed unit's blobs to the byte source, under the global lock. Execute also
	// passes it, outside the lock, as share_from to the readers of this run: that reads only what
	// its constructor wrote (the table and the name index), which PrefetchRows does not touch.
	mutable std::unique_ptr<SpotReader> prefetcher;
};

struct WorkUnit {
	std::shared_ptr<const OpenRun> run;
	int64_t first_row = 0;
	int64_t end_row = 0;
	idx_t batch_index = 0;
	// The unit's rows are numbered first_index, first_index + 1, ...; it makes `rows` of them.
	int64_t first_index = 1;
	uint64_t rows = 0;
};

// Opens a run. A remote one is read through the scan's fetch pool, created with the first: one
// pool for every run, sized from DuckDB's threads setting, so neither the threads nor the cache
// grow with the number of runs (SraFetchPool). Called under the scan's lock.
std::shared_ptr<const SraTable> OpenTable(ClientContext &context, const std::string &location,
                                          std::shared_ptr<SraFetchPool> &pool) {
	auto &fs = FileSystem::GetFileSystem(context);
	std::shared_ptr<const SraByteSource> source;
	if (miint::RemoteFileHelper::IsRemotePath(location)) {
#if defined(__EMSCRIPTEN__) && !defined(__EMSCRIPTEN_PTHREADS__)
		// A wasm build without -pthread cannot start the pool's threads (std::thread's constructor
		// aborts there rather than throwing): read the ranges directly, one at a time.
		source = std::make_shared<DuckDBSraSource>(fs, location, true);
#else
		if (!pool) {
			// As many fetches in flight as DuckDB may use threads. Each decoding thread walks two
			// columns (bases and qualities); the cache holds each walk's current and previous
			// block and its read-ahead: 72 MB per thread, at most 1 GiB in all.
			SraFetchPool::Options options;
			idx_t threads = std::max<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
			// 32 requests in flight matched a 32-way plain download of the object; more were not
			// measured (docs/internals/sra-reader.md, "Measurements"), so more threads open no more
			// connections.
			options.fetch_threads = std::min<idx_t>(threads, MAX_FETCH_THREADS);
			// Only the threads that decode need cache, at most MAX_SCAN_THREADS of them.
			size_t readers = std::min<size_t>(threads, MAX_SCAN_THREADS);
			size_t wanted = 2 * readers * (options.read_ahead + 2);
			options.max_cached_blocks =
			    std::min<size_t>(std::max<size_t>(wanted, 32), (size_t(1) << 30) / options.block_size);
			// A stalled transfer must not hold a reader past Ctrl-C or max_execution_time.
			options.interrupt_check = [&context] {
				context.BlockingInterruptCheck();
			};
			pool = std::make_shared<SraFetchPool>(std::move(options));
		}
		source = std::make_shared<SraPrefetchingSource>(pool, std::make_shared<DuckDBSraSource>(fs, location, true));
#endif
	} else {
		source = std::make_shared<DuckDBSraSource>(fs, location, false);
	}
	return std::make_shared<const SraTable>(std::make_shared<const KarArchive>(source));
}

struct GlobalState : public GlobalTableFunctionState {
	std::mutex lock;
	size_t next_run = 0;
	std::shared_ptr<const OpenRun> current;
	int64_t next_row = 0;
	int64_t end_row = 0;
	idx_t next_batch = 0;
	// Counts the rows of each unit as it is claimed, from the spots' read lengths and types alone,
	// so every unit knows where its rows start in the run's 1-based sequence_index. Used under the
	// lock only.
	std::unique_ptr<SpotReader> layout_reader;
	Spot layout;
	int64_t next_index = 1;
	uint64_t spots_per_unit = DEFAULT_SPOTS_PER_UNIT;
	std::vector<std::string> skipped;
	std::string first_skip_error;
	// Shared by every remote run of the scan; created with the first (OpenTable).
	std::shared_ptr<SraFetchPool> fetch_pool;

	idx_t MaxThreads() const override {
		// A run is split into many units, so even one run uses several threads, up to DuckDB's
		// threads setting. Each thread adds read-ahead cache (OpenTable), so this bounds that too.
		return MAX_SCAN_THREADS;
	}

	// Hands out the next unit, in run and spot order, opening the next run when the current one
	// is used up. The order of claims is the order of batch indexes, hence of the output.
	//
	// The next run is opened under the lock: its table of contents, metadata, column indexes and
	// name index are read before any of its units can be handed out, so a thread waiting here has
	// no other work to take. Threads still decoding the previous run's units carry on meanwhile.
	bool Claim(ClientContext &context, const SraScanData &bind, WorkUnit &unit) {
		std::lock_guard<std::mutex> guard(lock);
		while (!current || next_row >= end_row) {
			current.reset();
			if (next_run >= bind.runs.size()) {
				if (miint::EveryRunFailed(skipped.size(), bind.runs.size())) {
					throw IOException(miint::BuildEveryRunFailedMessage(bind.function, skipped, first_skip_error));
				}
				return false;
			}
			const SraScanData::Run &input = bind.runs[next_run++];
			auto run = std::make_shared<OpenRun>();
			run->location = input.location;
			run->accession = input.accession;
			try {
				run->table = OpenTable(context, input.location, fetch_pool);
				ClassifyRun(GatherRunFacts(*run->table));
				run->prefetcher = std::make_unique<SpotReader>(run->table);
				next_row = run->prefetcher->FirstRow();
				end_row = RowRangeEnd(next_row, run->prefetcher->RowCount(), "the READ column");
				layout_reader = std::make_unique<SpotReader>(run->table, run->prefetcher.get());
			} catch (const SraFormatError &e) {
				ThrowForRun(bind, run->accession, e);
			} catch (const SraUnsupportedError &e) {
				ThrowForRun(bind, run->accession, e);
			} catch (const std::exception &e) {
				if (!IsFetchFailure(e)) {
					throw;
				}
				// The run could not be fetched (missing from the bucket, the network). With other
				// runs asked for, skip it rather than lose theirs (read_ena_sequences' policy).
				std::string msg = ErrorData(e).RawMessage();
				if (!bind.remote_runs || !miint::ShouldSkipFailedRun(bind.runs.size())) {
					throw IOException(bind.function + ": " + run->accession + ": " + msg);
				}
				if (skipped.empty()) {
					first_skip_error = msg;
				}
				skipped.push_back(run->accession);
				miint::EmitWarning(context, bind.function + ": skipped run '" + run->accession + "': " + msg);
				continue;
			}
			next_index = 1;
			current = std::move(run);
		}
		unit.run = current;
		unit.first_row = next_row;
		// Bounded by the rows left first, so a unit near the largest row id cannot overflow.
		unit.end_row = next_row + int64_t(std::min<uint64_t>(spots_per_unit, uint64_t(end_row) - uint64_t(next_row)));
		unit.batch_index = next_batch++;
		// A thread's read-ahead follows its position, but each claim jumps it ahead of the other
		// threads: fetch the unit's blobs, and the next unit's, before a thread arrives at them
		// (and before the unit's rows are counted below, which reads the first of them).
		current->prefetcher->PrefetchRows(
		    unit.first_row,
		    unit.end_row + int64_t(std::min<uint64_t>(spots_per_unit, uint64_t(end_row) - uint64_t(unit.end_row))));
		unit.first_index = next_index;
		unit.rows = 0;
		WithRunErrors(bind, current->accession, [&] {
			for (int64_t row = next_row; row < unit.end_row; row++) {
				layout_reader->ReadLayout(row, layout);
				unit.rows += RowsOfSpot(layout, bind.split, bind.include_technical);
			}
		});
		next_index += int64_t(unit.rows);
		next_row = unit.end_row;
		return true;
	}
};

struct LocalState : public LocalTableFunctionState {
	bool has_unit = false;
	WorkUnit unit;
	idx_t batch_index = 0; // of the chunk last returned
	int64_t next_row = 0;
	int64_t next_index = 0; // the sequence_index of the unit's next row
	std::shared_ptr<const OpenRun> reader_run;
	std::unique_ptr<SpotReader> reader;
	Spot spot;
	std::vector<SraRead> reads;
	std::vector<SraRow> pending;
	size_t pending_pos = 0;
};

void SetQualityList(Vector &vec, const std::vector<const std::vector<uint8_t> *> &quals) {
	idx_t total = 0;
	for (auto *q : quals) {
		total += q ? q->size() : 0;
	}
	ListVector::Reserve(vec, total);
	ListVector::SetListSize(vec, total);
	auto &child = ListVector::GetChildMutable(vec);
	auto child_data = FlatVector::GetDataMutable<uint8_t>(child);
	auto entries = FlatVector::GetDataMutable<list_entry_t>(vec);
	// A NULL left in a mask buffer from an earlier chunk must not survive onto a valid row; a mask
	// without a buffer is already all-valid (as in SetResultVectorListUInt8).
	auto &validity = FlatVector::ValidityMutable(vec);
	if (validity.CanHaveNull()) {
		validity.SetAllValid(quals.size());
	}
	auto &child_validity = FlatVector::ValidityMutable(child);
	if (child_validity.CanHaveNull()) {
		child_validity.SetAllValid(total);
	}
	idx_t offset = 0;
	for (idx_t i = 0; i < quals.size(); i++) {
		const auto *q = quals[i];
		entries[i].offset = offset;
		entries[i].length = q ? q->size() : 0;
		if (!q) {
			validity.SetInvalid(i);
			continue;
		}
		if (!q->empty()) {
			std::memcpy(child_data + offset, q->data(), q->size());
		}
		offset += q->size();
	}
}

// The options both functions take, and the schema they imply.
void BindOptions(SraScanData &data, named_argument_map_t &params, vector<LogicalType> &return_types,
                 vector<Identifier> &names) {
	const std::string &fn = data.function;
	auto split = params.find("split");
	if (split != params.end() && !split->second.IsNull()) {
		std::string s = split->second.ToString();
		if (s == "split_3") {
			data.split = SplitMode::Split3;
		} else if (s == "spot") {
			data.split = SplitMode::Spot;
		} else {
			throw InvalidInputException(fn + ": split must be 'split_3' or 'spot', not '" + s + "'");
		}
	}
	auto tech = params.find("include_technical");
	if (tech != params.end() && !tech->second.IsNull()) {
		data.include_technical = tech->second.GetValue<bool>();
	}
	if (data.include_technical && data.split == SplitMode::Split3) {
		// fasterq-dump --split-3 --include-technical switches to split-files with a warning.
		throw InvalidInputException(fn + ": include_technical requires split => 'spot'");
	}
	data.include_filepath = ParseIncludeFilepathParameter(params);

	data.names = SequenceColumnNames();
	data.types = SequenceColumnTypes();
	// Where read_ena_sequences puts it.
	data.names.emplace_back("run_accession");
	data.types.emplace_back(LogicalType::VARCHAR);
	if (data.split == SplitMode::Spot) {
		data.names.emplace_back("read_number");
		data.types.emplace_back(LogicalType::UTINYINT);
	}
	if (data.include_filepath) {
		data.names.emplace_back("filepath");
		data.types.emplace_back(LogicalType::VARCHAR);
	}
	for (idx_t i = 0; i < data.names.size(); i++) {
		names.emplace_back(data.names[i]);
		return_types.push_back(data.types[i]);
	}
}

unique_ptr<FunctionData> ReadSraBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto data = make_uniq<SraScanData>();
	data->function = "read_sra";
	auto &fs = FileSystem::GetFileSystem(context);
	auto paths = ExpandPathArgument(fs, context, input.inputs[0], "read_sra").paths;
	for (const auto &path : paths) {
		if (IsStdinPath(path)) {
			throw InvalidInputException("read_sra: stdin is not supported (an .sra file is read at random positions)");
		}
	}
	RequireLocalFilesExist(fs, paths);
	for (const auto &path : paths) {
		data->runs.push_back({path, ReadSraTableFunction::AccessionFromPath(path)});
	}
	BindOptions(*data, input.named_parameters, return_types, names);
	return std::move(data);
}

unique_ptr<FunctionData> ReadNcbiBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto data = make_uniq<SraScanData>();
	data->function = "read_ncbi_sra_sequences";
	data->remote_runs = true;
	const std::string fn = data->function;
	const Value &arg = input.inputs[0];
	if (arg.IsNull()) {
		throw InvalidInputException(fn + ": the accession cannot be NULL");
	}
	std::vector<Value> values;
	if (arg.type().id() == LogicalTypeId::VARCHAR) {
		values.push_back(arg);
	} else if (arg.type().id() == LogicalTypeId::LIST) {
		values = ListValue::GetChildren(arg);
	} else {
		throw InvalidInputException(fn + ": the accession must be VARCHAR or VARCHAR[]");
	}
	if (values.empty()) {
		throw InvalidInputException(fn + ": at least one accession must be provided");
	}
	for (const auto &v : values) {
		if (v.IsNull()) {
			throw InvalidInputException(fn + ": the accession cannot be NULL");
		}
		std::string acc = v.ToString();
		auto start = acc.find_first_not_of(" \t\n\r");
		acc =
		    start == std::string::npos ? std::string() : acc.substr(start, acc.find_last_not_of(" \t\n\r") - start + 1);
		if (acc.empty()) {
			throw InvalidInputException(fn + ": the accession cannot be empty");
		}
		// A run named twice is read once.
		bool seen = false;
		for (const auto &r : data->runs) {
			seen |= r.accession == acc;
		}
		if (!seen) {
			data->runs.push_back({ReadNcbiSraSequencesTableFunction::RunUrl(context, acc), acc});
		}
	}
	BindOptions(*data, input.named_parameters, return_types, names);
	return std::move(data);
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<SraScanData>();
	if (bind.remote_runs) {
		miint::EnsureHttpfsLoaded(context);
	}
	auto state = make_uniq<GlobalState>();
	idx_t unit = GetNonNegativeOption(context, "miint_sra_spots_per_unit", 0, "0 uses the default");
	if (unit > 0) {
		// Capped far above any run's spot count, so row arithmetic cannot overflow.
		state->spots_per_unit = std::min<idx_t>(unit, idx_t(1) << 40);
	}
	return std::move(state);
}

unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                              GlobalTableFunctionState *global_state) {
	return make_uniq<LocalState>();
}

// Moves up to `capacity` rows of the thread's current unit into `rows`, decoding spots as needed.
// Clears has_unit when the unit is used up.
void FillRows(LocalState &ls, const SraScanData &bind, idx_t capacity, std::vector<SraRow> &rows) {
	while (rows.size() < capacity) {
		if (ls.pending_pos < ls.pending.size()) {
			rows.push_back(std::move(ls.pending[ls.pending_pos++]));
			continue;
		}
		if (ls.next_row >= ls.unit.end_row) {
			ls.has_unit = false;
			return;
		}
		ls.pending.clear();
		ls.pending_pos = 0;
		ls.reader->Read(ls.next_row++, ls.spot);
		ls.reads.clear();
		RouteSpot(ls.spot, bind.split, bind.include_technical, ls.reads);
		RowsForSpot(ls.reads, bind.split, ls.pending);
	}
}

// The rows decoded must be the rows Claim counted, or sequence_index would skip or repeat. They
// differ only when a one-read spot's READ_LEN disagrees with its READ (RowsOfSpot). Checked with
// each chunk of `n` more rows, and once the unit is done.
void CheckUnitRows(const SraScanData &bind, const LocalState &ls, idx_t n, bool unit_done) {
	const int64_t made = ls.next_index + int64_t(n) - ls.unit.first_index;
	if (made > int64_t(ls.unit.rows) || (unit_done && made != int64_t(ls.unit.rows))) {
		WithRunErrors(bind, ls.unit.run->accession, [&] {
			throw SraFormatError("spots " + std::to_string(ls.unit.first_row) + ".." +
			                     std::to_string(ls.unit.end_row - 1) + " make " + (unit_done ? "" : "at least ") +
			                     std::to_string(made) + " rows, but their READ_LEN and READ_TYPE give " +
			                     std::to_string(ls.unit.rows));
		});
	}
}

void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<SraScanData>();
	auto &gs = data_p.global_state->Cast<GlobalState>();
	auto &ls = data_p.local_state->Cast<LocalState>();

	// Checked on every call, not only between units: a unit is many chunks, and over a remote run
	// many range requests.
	context.BlockingInterruptCheck();
	std::vector<SraRow> rows;
	rows.reserve(STANDARD_VECTOR_SIZE);
	while (rows.empty()) {
		if (!ls.has_unit && ls.pending_pos >= ls.pending.size()) {
			if (ls.unit.run) {
				CheckUnitRows(bind, ls, 0, true);
			}
			if (!gs.Claim(context, bind, ls.unit)) {
				output.SetChildCardinality(0);
				return;
			}
			ls.has_unit = true;
			ls.batch_index = ls.unit.batch_index;
			ls.next_row = ls.unit.first_row;
			ls.next_index = ls.unit.first_index;
			if (ls.reader_run != ls.unit.run) {
				WithRunErrors(bind, ls.unit.run->accession, [&] {
					// The run's name index is shared with the reader Claim made, not read again.
					ls.reader = std::make_unique<SpotReader>(ls.unit.run->table, ls.unit.run->prefetcher.get());
				});
				ls.reader_run = ls.unit.run;
			}
		}
		// A run that fails partway fails the query: its rows so far are already out.
		WithRunErrors(bind, ls.unit.run->accession, [&] { FillRows(ls, bind, STANDARD_VECTOR_SIZE, rows); });
	}

	const idx_t n = rows.size();
	const std::string &acc = ls.unit.run->accession;
	CheckUnitRows(bind, ls, n, false);
	auto seq_index = FlatVector::GetDataMutable<int64_t>(output.data[0]);
	std::vector<std::string> read_ids(n), comments(n), seq1(n), seq2(n);
	std::vector<const std::vector<uint8_t> *> qual1(n), qual2(n);
	for (idx_t i = 0; i < n; i++) {
		const SraRow &r = rows[i];
		seq_index[i] = ls.next_index + int64_t(i);
		read_ids[i] = acc + "." + std::to_string(r.spot);
		// fasterq-dump's defline continues "$sn length=$rl"; read_fastx keeps read 1's.
		comments[i] = r.read1.has_name ? r.read1.name + " " : std::string();
		comments[i] += "length=" + std::to_string(r.read1.bases.size());
		seq1[i] = r.read1.bases;
		qual1[i] = &r.read1.quality;
		if (r.paired) {
			seq2[i] = r.read2.bases;
			qual2[i] = &r.read2.quality;
		}
	}
	idx_t col = 1;
	SetResultVectorString(output.data[col++], read_ids);
	SetResultVectorString(output.data[col++], comments);
	SetResultVectorString(output.data[col++], seq1);
	SetResultVectorStringNullable(output.data[col++], seq2);
	SetQualityList(output.data[col++], qual1);
	SetQualityList(output.data[col++], qual2);
	SetResultVectorFilepath(output.data[col++], acc); // one run per chunk, so a constant
	ls.next_index += int64_t(n);
	if (bind.split == SplitMode::Spot) {
		auto numbers = FlatVector::GetDataMutable<uint8_t>(output.data[col++]);
		for (idx_t i = 0; i < n; i++) {
			if (rows[i].read_number > 255) {
				throw InvalidInputException(bind.function + ": " + acc + ": spot " + std::to_string(rows[i].spot) +
				                            " has more than 255 reads");
			}
			numbers[i] = uint8_t(rows[i].read_number);
		}
	}
	if (bind.include_filepath) {
		SetResultVectorFilepath(output.data[col++], ls.unit.run->location);
	}
	output.SetChildCardinality(n);
}

OperatorPartitionData GetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("SRA scan: partition columns are not supported");
	}
	return OperatorPartitionData(input.local_state->Cast<LocalState>().batch_index);
}

// About 300 bytes of .sra per spot in the surveyed runs; a lower figure errs high, the safe side
// (EstimateFileScanCardinality). A remote run counts as large.
unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data) {
	std::vector<std::string> locations;
	for (const auto &r : bind_data->Cast<SraScanData>().runs) {
		locations.push_back(r.location);
	}
	return EstimateFileScanCardinality(locations, 100);
}

TableFunction MakeScan(const char *name, table_function_bind_t bind) {
	TableFunction tf(name, {LogicalType::ANY}, Execute, bind, InitGlobal, InitLocal);
	AddNamedParameter(tf, "split", LogicalType::VARCHAR);
	AddNamedParameter(tf, "include_technical", LogicalType::BOOLEAN);
	AddNamedParameter(tf, "include_filepath", LogicalType::BOOLEAN);
	tf.get_partition_data = GetPartitionData;
	tf.cardinality = Cardinality;
	return tf;
}

} // namespace

std::string ReadSraTableFunction::AccessionFromPath(const std::string &path) {
	auto slash = path.find_last_of("/\\");
	std::string name = slash == std::string::npos ? path : path.substr(slash + 1);
	for (const std::string ext : {".sralite", ".sra"}) {
		if (name.size() > ext.size() && name.compare(name.size() - ext.size(), ext.size(), ext) == 0) {
			return name.substr(0, name.size() - ext.size());
		}
	}
	return name;
}

TableFunction ReadSraTableFunction::GetFunction() {
	return MakeScan("read_sra", ReadSraBind);
}

void ReadSraTableFunction::Register(ExtensionLoader &loader) {
	loader.RegisterFunction(GetFunction());
}

std::string ReadNcbiSraSequencesTableFunction::RunUrl(ClientContext &context, const std::string &accession) {
	// Checked where the URL is built, so no caller can put another string into it.
	static const std::regex RUN("[SED]RR[0-9]+");
	if (!std::regex_match(accession, RUN)) {
		throw InvalidInputException("read_ncbi_sra_sequences: '" + accession +
		                            "' is not a run accession (SRR, ERR or DRR followed by digits); read_ena('" +
		                            accession + "') lists its runs");
	}
	std::string url = miint::UrlSetting(context, "miint_sra_run_url");
	if (url.find("{acc}") == std::string::npos) {
		// One template serves every run: without {acc} every run would read the same object.
		throw InvalidInputException("miint_sra_run_url must contain {acc}, which stands for the run accession");
	}
	for (size_t at; (at = url.find("{acc}")) != std::string::npos;) {
		url.replace(at, 5, accession);
	}
	return url;
}

TableFunction ReadNcbiSraSequencesTableFunction::GetFunction() {
	return MakeScan("read_ncbi_sra_sequences", ReadNcbiBind);
}

void ReadNcbiSraSequencesTableFunction::Register(ExtensionLoader &loader) {
	loader.RegisterFunction(GetFunction());
}

} // namespace duckdb

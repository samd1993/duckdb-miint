#pragma once

#include "sra_run.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// The scan behind read_sra and read_ncbi_sra_sequences: the reads of .sra runs as fasterq-dump
// writes them, in read_fastx's schema. A run is split into work units of consecutive spots,
// decoded by any thread, and emitted in spot order through their batch indexes. A remote run is
// read through many concurrent range requests (SraPrefetchingSource).
struct SraScanData : public TableFunctionData {
	struct Run {
		std::string location; // a path or URL
		std::string accession;
	};
	std::string function;
	std::vector<Run> runs;
	miint::sra::SplitMode split = miint::sra::SplitMode::Split3;
	bool include_technical = false;
	bool include_filepath = false;
	// read_ncbi_sra_sequences: a run_accession column, httpfs, and skipping a run that cannot be
	// opened when others were asked for (read_ena_sequences' policy).
	bool remote_runs = false;
	std::vector<std::string> names;
	std::vector<LogicalType> types;
};

// read_sra(path): local .sra files (or any path DuckDB's file system opens).
class ReadSraTableFunction {
public:
	static TableFunction GetFunction();
	static void Register(ExtensionLoader &loader);

	// The accession fasterq-dump prints for a file: its name without a .sra or .sralite extension.
	static std::string AccessionFromPath(const std::string &path);
};

// read_ncbi_sra_sequences(accession): runs from NCBI's public AWS bucket, by accession.
class ReadNcbiSraSequencesTableFunction {
public:
	static TableFunction GetFunction();
	static void Register(ExtensionLoader &loader);

	// The run object's URL. MIINT_SRA_RUN_URL_TEMPLATE overrides the bucket ({acc} is replaced),
	// which the tests point at a local range-serving server. Throws InvalidInputException unless
	// the accession is a run accession.
	static std::string RunUrl(const std::string &accession);
};

} // namespace duckdb

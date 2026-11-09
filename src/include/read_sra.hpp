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
// read through many concurrent range requests (SraFetchPool).
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
	// read_ncbi_sra_sequences: httpfs, and skipping a run that cannot be
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

	// NCBI's bucket, the default of the miint_sra_run_url setting.
	static constexpr const char *DEFAULT_RUN_URL = "https://sra-pub-run-odp.s3.amazonaws.com/sra/{acc}/{acc}";

	// The run object's URL: the miint_sra_run_url setting with {acc} replaced, so a mirror or a
	// test server can stand in for NCBI. Throws InvalidInputException unless the accession is a run
	// accession and the setting names {acc}.
	static std::string RunUrl(ClientContext &context, const std::string &accession);
};

} // namespace duckdb

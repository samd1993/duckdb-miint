#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "sra_source.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace miint::sra;

// The prefetching source is what makes reading a run from S3 fast (many range requests in flight):
// it must return exactly the backend's bytes, keep several ranges in flight ahead of a reader,
// bound its memory, and surface a failed fetch to the reader that needed it.

namespace {
// A backend that records the ranges asked of it, optionally slowly, optionally failing.
class RecordingSource : public SraByteSource {
public:
	explicit RecordingSource(size_t size, std::chrono::milliseconds delay = std::chrono::milliseconds(0))
	    : bytes_(size), delay_(delay) {
		for (size_t i = 0; i < size; i++) {
			bytes_[i] = uint8_t(i * 7 + (i >> 9));
		}
	}
	uint64_t Size() const override {
		return bytes_.size();
	}
	void ReadAt(uint64_t pos, uint8_t *buf, size_t n) const override {
		int now = ++in_flight_;
		int seen = max_in_flight_.load();
		while (now > seen && !max_in_flight_.compare_exchange_weak(seen, now)) {
		}
		{
			std::lock_guard<std::mutex> g(lock_);
			offsets_.insert(pos);
			fetches_[pos]++;
		}
		std::this_thread::sleep_for(delay_);
		--in_flight_;
		if (fail_at_ >= 0 && uint64_t(fail_at_) >= pos && uint64_t(fail_at_) < pos + n) {
			throw std::runtime_error("backend refused the range");
		}
		std::memcpy(buf, bytes_.data() + pos, n);
	}
	std::set<uint64_t> Offsets() const {
		std::lock_guard<std::mutex> g(lock_);
		return offsets_;
	}
	const std::vector<uint8_t> &Bytes() const {
		return bytes_;
	}
	int MaxInFlight() const {
		return max_in_flight_;
	}
	int FetchesOf(uint64_t pos) const {
		std::lock_guard<std::mutex> g(lock_);
		auto it = fetches_.find(pos);
		return it == fetches_.end() ? 0 : it->second;
	}
	int MostFetchesOfOneRange() const {
		std::lock_guard<std::mutex> g(lock_);
		int most = 0;
		for (auto &kv : fetches_) {
			most = std::max(most, kv.second);
		}
		return most;
	}
	std::atomic<int64_t> fail_at_ {-1};

private:
	std::vector<uint8_t> bytes_;
	std::chrono::milliseconds delay_;
	mutable std::mutex lock_;
	mutable std::set<uint64_t> offsets_;
	mutable std::map<uint64_t, int> fetches_;
	mutable std::atomic<int> in_flight_ {0};
	mutable std::atomic<int> max_in_flight_ {0};
};

SraPrefetchingSource::Options SmallBlocks(size_t max_cached = 64) {
	SraPrefetchingSource::Options o;
	o.block_size = 1000;
	o.max_cached_blocks = max_cached;
	o.fetch_threads = 8;
	o.read_ahead = 4;
	return o;
}
} // namespace

TEST_CASE("SraPrefetchingSource returns the backend's bytes for any range", "[sra]") {
	auto backend = std::make_shared<RecordingSource>(25'500);
	SraPrefetchingSource src(backend, SmallBlocks());
	CHECK(src.Size() == 25'500);
	// Ranges inside a block, across block boundaries, ending at the last partial block.
	for (auto [pos, n] : std::vector<std::pair<uint64_t, size_t>> {
	         {0, 10}, {995, 10}, {1999, 3002}, {25'000, 500}, {24'999, 1}, {7, 0}}) {
		std::vector<uint8_t> got(n);
		src.ReadAt(pos, got.data(), n);
		CHECK(std::equal(got.begin(), got.end(), backend->Bytes().begin() + pos));
	}
	std::vector<uint8_t> past(2);
	CHECK_THROWS_AS(src.ReadAt(25'499, past.data(), 2), SraFormatError);
}

TEST_CASE("SraPrefetchingSource is safe from many reading threads", "[sra]") {
	auto backend = std::make_shared<RecordingSource>(200'000);
	SraPrefetchingSource src(backend, SmallBlocks(16));
	std::atomic<int> wrong {0};
	std::vector<std::thread> readers;
	for (int t = 0; t < 8; t++) {
		readers.emplace_back([&, t] {
			std::vector<uint8_t> buf(1500);
			for (uint64_t pos = uint64_t(t) * 9000; pos + 1500 < 200'000; pos += 7300) {
				src.ReadAt(pos, buf.data(), buf.size());
				if (!std::equal(buf.begin(), buf.end(), backend->Bytes().begin() + pos)) {
					wrong++;
				}
			}
		});
	}
	for (auto &r : readers) {
		r.join();
	}
	CHECK(wrong == 0);
}

TEST_CASE("SraPrefetchingSource fetches ahead of a reader, several ranges at once", "[sra]") {
	// A slow backend, as S3 is per request: reading block 0 must queue the next blocks too, and
	// fetch them concurrently, so a reader walking forward finds them ready.
	auto backend = std::make_shared<RecordingSource>(20'000, std::chrono::milliseconds(50));
	SraPrefetchingSource src(backend, SmallBlocks());
	uint8_t b;
	src.ReadAt(0, &b, 1);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (backend->Offsets().size() < 5 && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	auto offsets = backend->Offsets();
	for (uint64_t block = 0; block <= 4; block++) {
		CHECK(offsets.count(block * 1000) == 1);
	}
	CHECK(backend->MaxInFlight() > 1);
}

TEST_CASE("SraPrefetchingSource keeps no more blocks than it may", "[sra]") {
	// Reading a whole 100-block file through a cache of 6 refetches nothing it still holds and
	// keeps at most 6 blocks: the bound is what keeps a multi-gigabyte run out of memory.
	auto backend = std::make_shared<RecordingSource>(100'000);
	auto o = SmallBlocks(6);
	o.read_ahead = 2;
	SraPrefetchingSource src(backend, o);
	std::vector<uint8_t> buf(1000);
	for (uint64_t pos = 0; pos < 100'000; pos += 1000) {
		src.ReadAt(pos, buf.data(), buf.size());
		REQUIRE(std::equal(buf.begin(), buf.end(), backend->Bytes().begin() + pos));
		REQUIRE(src.CachedBlocks() <= 6);
	}
	CHECK(backend->Offsets().size() == 100);
	CHECK(backend->MostFetchesOfOneRange() == 1);
}

TEST_CASE("SraPrefetchingSource keeps blocks fetched ahead until their readers arrive", "[sra]") {
	// Dropping a block fetched ahead before its reader arrives throws the transfer away and
	// fetches it again. With room for each reader's current and previous block and its read-ahead
	// (readers x (read_ahead + 2), how the scan sizes it), no block is fetched twice. Here 6 readers
	// walk separate regions, each with 4 blocks of read-ahead, through a cache of 48. The regions
	// are 80 blocks long and 100 apart, so no reader's read-ahead runs into another's region.
	auto backend = std::make_shared<RecordingSource>(600'000, std::chrono::milliseconds(2));
	auto o = SmallBlocks(48);
	o.read_ahead = 4;
	SraPrefetchingSource src(backend, o);
	std::vector<std::thread> readers;
	for (int t = 0; t < 6; t++) {
		readers.emplace_back([&, t] {
			std::vector<uint8_t> buf(500);
			for (uint64_t pos = uint64_t(t) * 100'000; pos < uint64_t(t) * 100'000 + 80'000; pos += 500) {
				src.ReadAt(pos, buf.data(), buf.size());
			}
		});
	}
	for (auto &r : readers) {
		r.join();
	}
	CHECK(backend->MostFetchesOfOneRange() == 1);
}

TEST_CASE("SraPrefetchingSource keeps reading ahead when blocks are never read to the end", "[sra]") {
	// Column files end in bytes no decoder reads (an MD5 trailer, padding, columns that are not
	// read). Blocks holding them must still age out, or they fill the cache, read-ahead stops,
	// and a reader waits for every block in turn: on S3 that cut throughput by more than half.
	// One reader takes half of each of 100 slow blocks through a cache of 8.
	auto backend = std::make_shared<RecordingSource>(100'000, std::chrono::milliseconds(20));
	auto o = SmallBlocks(8);
	o.read_ahead = 4;
	SraPrefetchingSource src(backend, o);
	std::vector<uint8_t> buf(500);
	auto start = std::chrono::steady_clock::now();
	for (uint64_t pos = 0; pos < 100'000; pos += 1000) {
		src.ReadAt(pos, buf.data(), buf.size());
	}
	// Fetched one at a time this takes 100 x 20 ms; with read-ahead about a quarter of that.
	CHECK(std::chrono::steady_clock::now() - start < std::chrono::milliseconds(1200));
	CHECK(backend->MostFetchesOfOneRange() == 1);
}

TEST_CASE("SraPrefetchingSource gives a failed fetch's error to the reader that needs the block", "[sra]") {
	auto backend = std::make_shared<RecordingSource>(10'000);
	backend->fail_at_ = 5500;
	SraPrefetchingSource src(backend, SmallBlocks());
	std::vector<uint8_t> buf(100);
	src.ReadAt(0, buf.data(), buf.size()); // block 0 fine; blocks 1-4 prefetched
	CHECK_THROWS_WITH(src.ReadAt(5400, buf.data(), buf.size()),
	                  Catch::Matchers::ContainsSubstring("backend refused the range"));
	// Other blocks are unaffected.
	src.ReadAt(7000, buf.data(), buf.size());
	CHECK(std::equal(buf.begin(), buf.end(), backend->Bytes().begin() + 7000));
}

TEST_CASE("SraPrefetchingSource fetches again a block whose read-ahead failed", "[sra]") {
	// A fetch made ahead of any reader can fail on a transient error long before the block is
	// needed; the reader that needs it must get a fresh fetch, not that old error.
	auto backend = std::make_shared<RecordingSource>(10'000);
	backend->fail_at_ = 3500;
	SraPrefetchingSource src(backend, SmallBlocks());
	std::vector<uint8_t> buf(100);
	src.ReadAt(0, buf.data(), buf.size()); // reads ahead into block 3, which fails
	for (int i = 0; i < 200 && !backend->Offsets().count(3000); i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	REQUIRE(backend->Offsets().count(3000));
	std::this_thread::sleep_for(std::chrono::milliseconds(50)); // let the failure be recorded
	backend->fail_at_ = -1;
	src.ReadAt(3400, buf.data(), buf.size());
	CHECK(std::equal(buf.begin(), buf.end(), backend->Bytes().begin() + 3400));
	CHECK(backend->FetchesOf(3000) == 2); // the failed read-ahead, then the reader's own fetch
}

TEST_CASE("SraPrefetchingSource can be destroyed with fetches in flight", "[sra]") {
	// A LIMIT ends a scan while blocks are still being fetched; the source must stop its workers
	// without waiting for the whole read-ahead queue.
	auto backend = std::make_shared<RecordingSource>(100'000, std::chrono::milliseconds(20));
	auto o = SmallBlocks();
	o.fetch_threads = 2;
	o.read_ahead = 50;
	auto start = std::chrono::steady_clock::now();
	{
		SraPrefetchingSource src(backend, o);
		uint8_t b;
		src.ReadAt(0, &b, 1);
	}
	CHECK(std::chrono::steady_clock::now() - start < std::chrono::milliseconds(500));
}

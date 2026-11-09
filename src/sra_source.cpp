#include "sra_source.hpp"

#include "interruptible_sleep.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <map>
#include <stdexcept>
#include <thread>

namespace miint {
namespace sra {

std::vector<uint8_t> SraByteSource::Read(uint64_t pos, size_t n) const {
	std::vector<uint8_t> out(n);
	if (n > 0) {
		ReadAt(pos, out.data(), n);
	}
	return out;
}

namespace {
void CheckRange(uint64_t pos, size_t n, uint64_t size) {
	if (pos > size || n > size - pos) {
		throw SraFormatError("read of " + std::to_string(n) + " bytes at offset " + std::to_string(pos) +
		                     " runs past the end of the file (" + std::to_string(size) + " bytes)");
	}
}
} // namespace

SraFileSource::SraFileSource(const std::string &path) : in_(path, std::ios::binary) {
	if (!in_) {
		throw SraFormatError("cannot open " + path);
	}
	in_.seekg(0, std::ios::end);
	size_ = uint64_t(in_.tellg());
}

uint64_t SraFileSource::Size() const {
	return size_;
}

void SraFileSource::ReadAt(uint64_t pos, uint8_t *buf, size_t n) const {
	CheckRange(pos, n, size_);
	std::lock_guard<std::mutex> lock(mutex_);
	in_.seekg(std::streamoff(pos));
	in_.read(reinterpret_cast<char *>(buf), std::streamsize(n));
	if (size_t(in_.gcount()) != n) {
		in_.clear();
		throw SraFormatError("short read at offset " + std::to_string(pos));
	}
}

SraMemorySource::SraMemorySource(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {
}

uint64_t SraMemorySource::Size() const {
	return bytes_.size();
}

void SraMemorySource::ReadAt(uint64_t pos, uint8_t *buf, size_t n) const {
	CheckRange(pos, n, bytes_.size());
	if (n > 0) {
		std::memcpy(buf, bytes_.data() + pos, n);
	}
}

struct SraFetchPool::State {
	struct Block {
		std::vector<uint8_t> data;
		bool started = false;
		bool done = false;
		std::exception_ptr error;
		uint64_t last_use = 0;
	};
	struct File {
		std::shared_ptr<const SraByteSource> backend;
		uint64_t size = 0;
		uint64_t block_count = 0;
	};
	using Key = std::pair<uint32_t, uint64_t>; // (file, block)

	Options options;
	std::mutex lock;
	std::condition_variable work_ready;
	std::condition_variable block_done;
	std::map<uint32_t, File> files;
	uint32_t next_file = 1;
	std::map<Key, std::shared_ptr<Block>> blocks;
	std::deque<Key> queue; // blocks to fetch, demanded ones first
	bool stopping = false;
	uint64_t clock = 0;

	// Holds the lock. Returns the block, queueing it if it is not held: a demanded block jumps the
	// queue and is always admitted; a block read ahead is admitted only if the cache has room, so
	// read-ahead never pushes out a block another reader is about to use. Returns nullptr for
	// read-ahead that did not fit.
	std::shared_ptr<Block> Ensure(Key k, bool demand) {
		auto it = blocks.find(k);
		if (it != blocks.end() && demand && it->second->done && it->second->error && it->second.use_count() == 1) {
			// A failed fetch nobody waited on (read ahead): the reader now asking for the block gets
			// a fresh fetch, not an error that may have been transient.
			blocks.erase(it);
			it = blocks.end();
		}
		if (it != blocks.end()) {
			if (demand && !it->second->started) {
				queue.erase(std::remove(queue.begin(), queue.end(), k), queue.end());
				queue.push_front(k);
				work_ready.notify_one();
			}
			return it->second;
		}
		while (blocks.size() >= options.max_cached_blocks && EvictOne()) {
		}
		if (!demand && blocks.size() >= options.max_cached_blocks) {
			return nullptr;
		}
		auto block = std::make_shared<Block>();
		blocks.emplace(k, block);
		if (demand) {
			queue.push_front(k);
		} else {
			queue.push_back(k);
		}
		work_ready.notify_one();
		return block;
	}

	// Drops one block nobody holds: the least recently used fetched block (a fetch counts as a
	// use, so a block read ahead and never read ages out like any other), or failing that the
	// farthest read-ahead still waiting in the queue.
	bool EvictOne() {
		auto victim = blocks.end();
		for (auto it = blocks.begin(); it != blocks.end(); ++it) {
			const auto &blk = it->second;
			if (blk.use_count() == 1 && blk->done &&
			    (victim == blocks.end() || blk->last_use < victim->second->last_use)) {
				victim = it;
			}
		}
		if (victim == blocks.end()) {
			for (auto rit = queue.rbegin(); rit != queue.rend(); ++rit) {
				auto it = blocks.find(*rit);
				if (it != blocks.end() && it->second.use_count() == 1 && !it->second->started) {
					victim = it;
					break;
				}
			}
			if (victim == blocks.end()) {
				return false;
			}
			queue.erase(std::remove(queue.begin(), queue.end(), victim->first), queue.end());
		}
		blocks.erase(victim);
		return true;
	}

	void Work() {
		std::unique_lock<std::mutex> guard(lock);
		while (true) {
			work_ready.wait(guard, [&] { return stopping || !queue.empty(); });
			if (stopping) {
				return;
			}
			Key k = queue.front();
			queue.pop_front();
			auto it = blocks.find(k);
			auto f = files.find(k.first);
			if (it == blocks.end() || f == files.end()) {
				continue;
			}
			std::shared_ptr<Block> block = it->second;
			// Held through the fetch: a file closed meanwhile keeps its backend alive until then.
			std::shared_ptr<const SraByteSource> backend = f->second.backend;
			block->started = true;
			uint64_t pos = k.second * options.block_size;
			size_t n = size_t(std::min<uint64_t>(options.block_size, f->second.size - pos));
			guard.unlock();
			std::vector<uint8_t> data;
			std::exception_ptr error;
			try {
				data.resize(n); // inside the try: a failed allocation is this block's error
				backend->ReadAt(pos, data.data(), n);
			} catch (...) {
				error = std::current_exception();
			}
			guard.lock();
			block->data = std::move(data);
			block->error = error;
			block->done = true;
			block->last_use = ++clock;
			block_done.notify_all();
		}
	}

	// Holds the lock. Waits for a block, checking for an interrupt between slices; the check runs
	// without the lock.
	void Wait(std::unique_lock<std::mutex> &guard, const std::shared_ptr<Block> &block) {
		while (!block->done) {
			if (!options.interrupt_check) {
				block_done.wait(guard, [&] { return block->done; });
				return;
			}
			if (block_done.wait_for(guard, INTERRUPT_POLL_INTERVAL, [&] { return block->done; })) {
				return;
			}
			guard.unlock();
			try {
				options.interrupt_check();
			} catch (...) {
				guard.lock();
				throw;
			}
			guard.lock();
		}
	}
};

SraFetchPool::SraFetchPool(Options options) : state_(std::make_shared<State>()) {
	if (options.block_size == 0 || options.fetch_threads == 0 || options.max_cached_blocks == 0) {
		throw std::invalid_argument("SraFetchPool: block size, threads and cache must be positive");
	}
	state_->options = std::move(options);
	State *st = state_.get();
	try {
		for (size_t i = 0; i < st->options.fetch_threads; i++) {
			workers_.emplace_back([st] { st->Work(); });
		}
	} catch (...) {
		// The destructor does not run for a constructor that throws, and a joinable thread's
		// destruction terminates the process: stop the threads already started.
		{
			std::lock_guard<std::mutex> guard(st->lock);
			st->stopping = true;
		}
		st->work_ready.notify_all();
		for (auto &w : workers_) {
			w.join();
		}
		throw;
	}
}

SraFetchPool::~SraFetchPool() {
	{
		std::lock_guard<std::mutex> guard(state_->lock);
		state_->stopping = true;
		state_->queue.clear();
	}
	state_->work_ready.notify_all();
	for (auto &w : workers_) {
		w.join();
	}
}

size_t SraFetchPool::CachedBlocks() const {
	std::lock_guard<std::mutex> guard(state_->lock);
	return state_->blocks.size();
}

SraPrefetchingSource::SraPrefetchingSource(std::shared_ptr<SraFetchPool> pool,
                                           std::shared_ptr<const SraByteSource> backend)
    : pool_(std::move(pool)) {
	if (!pool_ || !backend) {
		throw std::invalid_argument("SraPrefetchingSource: a pool and a backend are required");
	}
	size_ = backend->Size();
	auto &st = *pool_->state_;
	std::lock_guard<std::mutex> guard(st.lock);
	file_ = st.next_file++;
	st.files[file_] = {std::move(backend), size_, (size_ + st.options.block_size - 1) / st.options.block_size};
}

SraPrefetchingSource::SraPrefetchingSource(std::shared_ptr<const SraByteSource> backend, Options options)
    : SraPrefetchingSource(std::make_shared<SraFetchPool>(std::move(options)), std::move(backend)) {
}

SraPrefetchingSource::~SraPrefetchingSource() {
	auto &st = *pool_->state_;
	std::lock_guard<std::mutex> guard(st.lock);
	st.queue.erase(
	    std::remove_if(st.queue.begin(), st.queue.end(), [&](const State::Key &k) { return k.first == file_; }),
	    st.queue.end());
	for (auto it = st.blocks.begin(); it != st.blocks.end();) {
		it = it->first.first == file_ ? st.blocks.erase(it) : std::next(it);
	}
	st.files.erase(file_);
}

uint64_t SraPrefetchingSource::Size() const {
	return size_;
}

void SraPrefetchingSource::ReadAt(uint64_t pos, uint8_t *buf, size_t n) const {
	CheckRange(pos, n, size_);
	if (n == 0) {
		return;
	}
	auto &st = *pool_->state_;
	const uint64_t bs = st.options.block_size;
	const uint64_t first = pos / bs;
	const uint64_t last = (pos + n - 1) / bs;
	const uint64_t block_count = (size_ + bs - 1) / bs;
	std::unique_lock<std::mutex> guard(st.lock);
	std::vector<std::shared_ptr<State::Block>> needed;
	for (uint64_t b = first; b <= last; b++) {
		needed.push_back(st.Ensure({file_, b}, true));
	}
	for (uint64_t b = last + 1; b <= last + st.options.read_ahead && b < block_count; b++) {
		st.Ensure({file_, b}, false);
	}
	for (uint64_t i = 0; i < needed.size(); i++) {
		auto &block = needed[i];
		st.Wait(guard, block);
		block->last_use = ++st.clock;
		if (block->error) {
			// The failure is the reader's to handle; drop the block so a later read retries it.
			auto failed = block->error;
			auto it = st.blocks.find({file_, first + i});
			if (it != st.blocks.end() && it->second == block) {
				st.blocks.erase(it);
			}
			std::rethrow_exception(failed);
		}
		uint64_t block_start = (first + i) * bs;
		uint64_t from = std::max<uint64_t>(pos, block_start);
		uint64_t to = std::min<uint64_t>(pos + n, block_start + block->data.size());
		std::memcpy(buf + (from - pos), block->data.data() + (from - block_start), size_t(to - from));
	}
}

void SraPrefetchingSource::Prefetch(uint64_t pos, uint64_t n) const {
	auto &st = *pool_->state_;
	if (n == 0 || pos >= size_) {
		return;
	}
	uint64_t last = pos + std::min<uint64_t>(n, size_ - pos) - 1; // no pos + n: it can wrap
	std::lock_guard<std::mutex> guard(st.lock);
	for (uint64_t b = pos / st.options.block_size; b <= last / st.options.block_size; b++) {
		if (!st.Ensure({file_, b}, false)) {
			return; // the cache is full of blocks readers still need
		}
	}
}

size_t SraPrefetchingSource::CachedBlocks() const {
	return pool_->CachedBlocks();
}

namespace {
template <typename T>
T LoadLittleEndian(const uint8_t *p) {
	T v = 0;
	for (size_t i = 0; i < sizeof(T); i++) {
		v = T(v | (T(p[i]) << (BITS_PER_BYTE * i)));
	}
	return v;
}
} // namespace

uint16_t LoadU16(const uint8_t *p) {
	return LoadLittleEndian<uint16_t>(p);
}

uint32_t LoadU32(const uint8_t *p) {
	return LoadLittleEndian<uint32_t>(p);
}

uint64_t LoadU64(const uint8_t *p) {
	return LoadLittleEndian<uint64_t>(p);
}

} // namespace sra
} // namespace miint

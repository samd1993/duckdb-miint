#include "sra_source.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <deque>
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

struct SraPrefetchingSource::State {
	struct Block {
		std::vector<uint8_t> data;
		bool started = false;
		bool done = false;
		std::exception_ptr error;
		uint64_t last_use = 0;
	};

	std::shared_ptr<const SraByteSource> backend;
	Options options;
	uint64_t size = 0;
	uint64_t block_count = 0;

	std::mutex lock;
	std::condition_variable work_ready;
	std::condition_variable block_done;
	std::map<uint64_t, std::shared_ptr<Block>> blocks;
	std::deque<uint64_t> queue; // blocks to fetch, demanded ones first
	bool stopping = false;
	uint64_t clock = 0;
	std::vector<std::thread> workers;

	// Holds the lock. Returns the block, queueing it if it is not held: a demanded block jumps the
	// queue and is always admitted; a block read ahead is admitted only if the cache has room, so
	// read-ahead never pushes out a block another reader is about to use. Returns nullptr for
	// read-ahead that did not fit.
	std::shared_ptr<Block> Ensure(uint64_t b, bool demand) {
		auto it = blocks.find(b);
		if (it != blocks.end() && demand && it->second->done && it->second->error && it->second.use_count() == 1) {
			// A failed fetch nobody waited on (read ahead): the reader now asking for the block gets
			// a fresh fetch, not an error that may have been transient.
			blocks.erase(it);
			it = blocks.end();
		}
		if (it != blocks.end()) {
			if (demand && !it->second->started) {
				queue.erase(std::remove(queue.begin(), queue.end(), b), queue.end());
				queue.push_front(b);
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
		blocks.emplace(b, block);
		if (demand) {
			queue.push_front(b);
		} else {
			queue.push_back(b);
		}
		work_ready.notify_one();
		return block;
	}

	// Drops one block nobody holds: the least recently used fetched block (a fetch counts as a
	// use, so a block read ahead and never read ages out like any other), or failing that the
	// farthest read-ahead still waiting in the queue. Sizing the cache above the readers'
	// combined read-ahead is what keeps blocks fetched ahead until their readers arrive.
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
			uint64_t b = queue.front();
			queue.pop_front();
			auto it = blocks.find(b);
			if (it == blocks.end()) {
				continue;
			}
			std::shared_ptr<Block> block = it->second;
			block->started = true;
			guard.unlock();
			uint64_t pos = b * options.block_size;
			size_t n = size_t(std::min<uint64_t>(options.block_size, size - pos));
			std::vector<uint8_t> data(n);
			std::exception_ptr error;
			try {
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
};

SraPrefetchingSource::SraPrefetchingSource(std::shared_ptr<const SraByteSource> backend, Options options)
    : state_(std::make_unique<State>()) {
	State &st = *state_;
	if (options.block_size == 0 || options.fetch_threads == 0 || options.max_cached_blocks == 0) {
		throw std::invalid_argument("SraPrefetchingSource: block size, threads and cache must be positive");
	}
	st.backend = std::move(backend);
	st.options = options;
	st.size = st.backend->Size();
	st.block_count = (st.size + options.block_size - 1) / options.block_size;
	for (size_t i = 0; i < options.fetch_threads; i++) {
		st.workers.emplace_back([&st] { st.Work(); });
	}
}

SraPrefetchingSource::~SraPrefetchingSource() {
	{
		std::lock_guard<std::mutex> guard(state_->lock);
		state_->stopping = true;
		state_->queue.clear();
	}
	state_->work_ready.notify_all();
	for (auto &w : state_->workers) {
		w.join();
	}
}

uint64_t SraPrefetchingSource::Size() const {
	return state_->size;
}

void SraPrefetchingSource::ReadAt(uint64_t pos, uint8_t *buf, size_t n) const {
	CheckRange(pos, n, state_->size);
	if (n == 0) {
		return;
	}
	State &st = *state_;
	const uint64_t bs = st.options.block_size;
	const uint64_t first = pos / bs;
	const uint64_t last = (pos + n - 1) / bs;
	std::unique_lock<std::mutex> guard(st.lock);
	std::vector<std::shared_ptr<State::Block>> needed;
	for (uint64_t b = first; b <= last; b++) {
		needed.push_back(st.Ensure(b, true));
	}
	for (uint64_t b = last + 1; b <= last + st.options.read_ahead && b < st.block_count; b++) {
		st.Ensure(b, false);
	}
	for (uint64_t i = 0; i < needed.size(); i++) {
		auto &block = needed[i];
		st.block_done.wait(guard, [&] { return block->done; });
		block->last_use = ++st.clock;
		if (block->error) {
			// The failure is the reader's to handle; drop the block so a later read retries it.
			auto failed = block->error;
			auto it = st.blocks.find(first + i);
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
	State &st = *state_;
	if (n == 0 || pos >= st.size) {
		return;
	}
	uint64_t last = std::min<uint64_t>(pos + n, st.size) - 1;
	std::lock_guard<std::mutex> guard(st.lock);
	for (uint64_t b = pos / st.options.block_size; b <= last / st.options.block_size; b++) {
		if (!st.Ensure(b, false)) {
			return; // the cache is full of blocks readers still need
		}
	}
}

size_t SraPrefetchingSource::CachedBlocks() const {
	std::lock_guard<std::mutex> guard(state_->lock);
	return state_->blocks.size();
}

uint16_t LoadU16(const uint8_t *p) {
	return uint16_t(p[0] | (p[1] << 8));
}

uint32_t LoadU32(const uint8_t *p) {
	return uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
}

uint64_t LoadU64(const uint8_t *p) {
	return uint64_t(LoadU32(p)) | (uint64_t(LoadU32(p + 4)) << 32);
}

} // namespace sra
} // namespace miint

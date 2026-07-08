#include "keystone/block_cache.h"

namespace keystone {

BlockCache::BlockCache(size_t capacity_bytes) : capacity_(capacity_bytes) {}

BlockCache::Block BlockCache::get_or_load(int file_number,
                                          uint64_t block_offset,
                                          Loader loader) {
    CacheKey key{file_number, block_offset};

    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            lru_.splice(lru_.begin(), lru_, it->second);
            hits_.fetch_add(1, std::memory_order_relaxed);
            return it->second->block;
        }
    }

    auto raw = loader();
    auto block = std::make_shared<const std::vector<uint8_t>>(std::move(raw));
    size_t block_size = block->size();
    misses_.fetch_add(1, std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            lru_.splice(lru_.begin(), lru_, it->second);
            return it->second->block;
        }

        lru_.push_front({key, block, block_size});
        map_[key] = lru_.begin();
        current_bytes_ += block_size;

        while (current_bytes_ > capacity_ && !lru_.empty()) {
            auto& back = lru_.back();
            current_bytes_ -= back.size;
            map_.erase(back.key);
            lru_.pop_back();
        }
    }

    return block;
}

}  // namespace keystone

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace keystone {

class BlockCache {
public:
    explicit BlockCache(size_t capacity_bytes);

    using Block = std::shared_ptr<const std::vector<uint8_t>>;
    using Loader = std::function<std::vector<uint8_t>()>;

    Block get_or_load(int file_number, uint64_t block_offset, Loader loader);

    uint64_t hits() const { return hits_.load(std::memory_order_relaxed); }
    uint64_t misses() const { return misses_.load(std::memory_order_relaxed); }

private:
    struct CacheKey {
        int file_number;
        uint64_t block_offset;
        bool operator==(const CacheKey& o) const {
            return file_number == o.file_number &&
                   block_offset == o.block_offset;
        }
    };

    struct CacheKeyHash {
        size_t operator()(const CacheKey& k) const {
            size_t h = std::hash<int>{}(k.file_number);
            h ^= std::hash<uint64_t>{}(k.block_offset) +
                 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };

    struct CacheEntry {
        CacheKey key;
        Block block;
        size_t size;
    };

    using LRUIter = std::list<CacheEntry>::iterator;

    std::mutex mu_;
    size_t capacity_;
    size_t current_bytes_ = 0;
    std::list<CacheEntry> lru_;
    std::unordered_map<CacheKey, LRUIter, CacheKeyHash> map_;

    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
};

}  // namespace keystone

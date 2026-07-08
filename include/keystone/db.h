#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "keystone/block_cache.h"
#include "keystone/manifest.h"
#include "keystone/memtable.h"
#include "keystone/sstable.h"
#include "keystone/version.h"
#include "keystone/wal.h"

namespace keystone {

struct Options {
    SyncMode sync = SyncMode::EveryWrite;
    size_t flush_threshold_bytes = 4 * 1024 * 1024;
    size_t compaction_trigger = 4;
    size_t block_cache_bytes = 16 * 1024 * 1024;
    size_t target_file_size = 2 * 1024 * 1024;
    size_t l1_max_bytes = 8 * 1024 * 1024;
    size_t level_size_multiplier = 10;
};

struct Snapshot {
    uint64_t seq;
};

class DB {
public:
    static std::unique_ptr<DB> open(const std::string& dir, Options opts = {});
    ~DB();

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    void put(std::string_view key, std::string_view value);
    std::optional<std::string> get(std::string_view key);
    void del(std::string_view key);
    void flush();
    void compact();
    uint64_t wal_fsync_count() const;

    void scan(std::string_view start, std::string_view end,
              const std::function<void(std::string_view key,
                                       std::string_view value)>& emit) const;

    Snapshot get_snapshot();
    void release_snapshot(const Snapshot& snap);

    std::optional<std::string> get(std::string_view key, const Snapshot& snap);

    void scan(std::string_view start, std::string_view end,
              const std::function<void(std::string_view key,
                                       std::string_view value)>& emit,
              const Snapshot& snap) const;

    BlockCache* block_cache() const { return cache_.get(); }

private:
    DB() = default;

    struct WriteRequest {
        RecType type;
        std::string_view key;
        std::string_view value;
        bool done = false;
        uint64_t seq = 0;
    };

    struct CompactionJob {
        int output_level = 0;
        std::vector<FileMeta> inputs;
        bool is_bottommost = false;
    };

    void write(RecType type, std::string_view key, std::string_view value);
    void maybe_flush();
    void compaction_loop();
    CompactionJob pick_compaction(const std::shared_ptr<const Version>& ver);
    int alloc_sst_number();
    bool needs_compaction() const;
    int total_file_count() const;
    int max_populated_level(const Version& ver) const;
    size_t level_bytes(const Version& ver, int level) const;
    size_t max_level_bytes(int level) const;
    std::vector<ManifestEntry> version_to_manifest(const Version& ver) const;

    std::optional<std::string> get_impl(std::string_view key, uint64_t snap_seq);
    void scan_impl(std::string_view start, std::string_view end,
                   const std::function<void(std::string_view,
                                            std::string_view)>& emit,
                   uint64_t snap_seq) const;
    uint64_t oldest_live_snapshot() const;

    std::shared_ptr<BlockCache> cache_;
    std::unique_ptr<Memtable> memtable_ = std::make_unique<Memtable>();
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<Manifest> manifest_;
    std::shared_ptr<const Version> current_version_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::thread compaction_thread_;
    bool stop_ = false;
    bool compaction_pending_ = false;
    bool compaction_running_ = false;
    bool force_compact_ = false;

    std::mutex write_mu_;
    std::condition_variable write_cv_;
    std::deque<WriteRequest*> write_queue_;
    std::string dir_;
    Options opts_;
    int next_sst_number_ = 1;
    uint64_t next_seq_ = 1;

    std::multiset<uint64_t> live_snapshots_;
};

}  // namespace keystone

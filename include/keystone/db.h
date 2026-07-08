#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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

private:
    DB() = default;

    struct WriteRequest {
        RecType type;
        std::string_view key;
        std::string_view value;
        bool done = false;
    };

    void write(RecType type, std::string_view key, std::string_view value);
    void maybe_flush();
    void compaction_loop();

    std::unique_ptr<Memtable> memtable_ = std::make_unique<Memtable>();
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<Manifest> manifest_;
    std::shared_ptr<const Version> current_version_;
    std::vector<int> live_numbers_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::thread compaction_thread_;
    bool stop_ = false;
    bool compaction_pending_ = false;
    bool compaction_running_ = false;

    std::mutex write_mu_;
    std::condition_variable write_cv_;
    std::deque<WriteRequest*> write_queue_;
    std::string dir_;
    Options opts_;
    int next_sst_number_ = 1;
};

}  // namespace keystone

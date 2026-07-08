#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace keystone {

enum class RecType : uint8_t { Put = 1, Delete = 2 };
enum class SyncMode { EveryWrite, Batched };

class WAL {
public:
    static std::unique_ptr<WAL> open(const std::string& path, SyncMode mode);

    void append(RecType type, std::string_view key, std::string_view value);
    void sync();
    uint64_t fsync_count() const { return fsync_count_.load(std::memory_order_relaxed); }

    struct Record {
        RecType type;
        std::string key;
        std::string value;
    };

    void append_batch(const std::vector<Record>& records);
    static std::vector<Record> replay(const std::string& path);

    ~WAL();

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

private:
    WAL(int fd, SyncMode mode);
    int fd_;
    SyncMode mode_;
    std::atomic<uint64_t> fsync_count_{0};
};

}  // namespace keystone

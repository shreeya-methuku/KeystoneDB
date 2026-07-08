#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "keystone/bloom.h"

namespace keystone {

class BlockCache;

// ── SSTableWriter ────────────────────────────────────────────────────────────

class SSTableWriter {
public:
    explicit SSTableWriter(const std::string& final_path);
    ~SSTableWriter();

    SSTableWriter(const SSTableWriter&) = delete;
    SSTableWriter& operator=(const SSTableWriter&) = delete;

    void add(std::string_view key, std::string_view value, bool tombstone, uint64_t seq);
    void finish();
    const std::string& temp_path() const { return temp_path_; }
    const std::string& smallest_key() const { return smallest_key_; }
    const std::string& largest_key() const { return largest_key_; }

    static void install(const std::string& temp_path,
                        const std::string& final_path,
                        const std::string& dir_path);

private:
    struct IndexEntry {
        std::string first_key;
        uint64_t offset;
        uint64_t length;
    };

    void flush_block();

    std::string final_path_;
    std::string temp_path_;
    int fd_;
    std::vector<uint8_t> block_buf_;
    std::string block_first_key_;
    uint64_t current_offset_;
    std::vector<IndexEntry> index_;
    uint64_t entry_count_;
    uint64_t max_seq_ = 0;
    std::string smallest_key_;
    std::string largest_key_;
    std::vector<std::string> keys_;
};

// ── SSTable (reader) ─────────────────────────────────────────────────────────

class SSTable {
public:
    static std::shared_ptr<SSTable> open(
        const std::string& path,
        std::shared_ptr<BlockCache> cache = nullptr);
    ~SSTable();

    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;

    void set_unlink_on_destroy(bool v) { unlink_on_destroy_ = v; }

    struct Entry {
        std::string key;
        std::string value;
        bool tombstone;
        uint64_t seq = 0;
    };

    enum class LookupStatus { NotFound, Found, Deleted };
    struct LookupResult {
        LookupStatus status;
        std::string value;
    };

    LookupResult get(std::string_view key, uint64_t snapshot_seq = UINT64_MAX) const;
    bool may_contain(std::string_view key) const;
    const std::string& path() const { return path_; }
    int number() const { return number_; }

    uint64_t entry_count() const { return footer_.entry_count; }
    uint64_t max_seq() const { return footer_.max_seq; }
    size_t block_count() const { return index_.size(); }

private:
    struct IndexEntry {
        std::string first_key;
        uint64_t offset;
        uint64_t length;
    };

    struct Footer {
        uint64_t index_offset;
        uint64_t bloom_offset;
        uint64_t entry_count;
        uint64_t max_seq;
        uint32_t format_version;
        uint32_t magic;
    };

    SSTable(int fd, std::string path, int number, Footer footer,
            std::vector<IndexEntry> index, std::optional<Bloom> bloom);

    int fd_;
    std::string path_;
    int number_;
    Footer footer_;
    std::vector<IndexEntry> index_;
    std::optional<Bloom> bloom_;
    bool unlink_on_destroy_ = false;
    std::shared_ptr<BlockCache> cache_;

    ssize_t find_block_idx(std::string_view key) const;
    std::shared_ptr<const std::vector<uint8_t>> read_block(size_t idx) const;

public:
    class Iterator {
    public:
        const Entry& operator*() const { return current_; }
        Iterator& operator++();
        bool operator!=(const Iterator& o) const { return valid_ != o.valid_; }

    private:
        friend class SSTable;
        Iterator() : table_(nullptr), block_idx_(0), pos_(0), valid_(false) {}
        Iterator(const SSTable* t, size_t block_idx);
        Iterator(const SSTable* t, size_t block_idx,
                 std::string_view seek_target);

        void load_block();
        void parse_entry();

        const SSTable* table_;
        size_t block_idx_;
        std::shared_ptr<const std::vector<uint8_t>> block_;
        size_t pos_;
        bool valid_;
        Entry current_;
    };

    Iterator begin() const;
    Iterator end() const;
    Iterator seek(std::string_view target) const;
};

}  // namespace keystone

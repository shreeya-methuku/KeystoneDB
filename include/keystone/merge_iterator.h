#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "keystone/memtable.h"
#include "keystone/sstable.h"

namespace keystone {

struct MergeEntry {
    std::string key;
    std::string value;
    bool tombstone;
    uint64_t seq = 0;
};

class MergeSource {
public:
    virtual ~MergeSource() = default;
    virtual bool valid() const = 0;
    virtual const MergeEntry& entry() const = 0;
    virtual void next() = 0;
    int rank() const { return rank_; }

protected:
    explicit MergeSource(int rank) : rank_(rank) {}

private:
    int rank_;
};

// ── Memtable adapter (uses RawIterator for all versions) ────────────────────

class MemtableMergeSource : public MergeSource {
public:
    MemtableMergeSource(const Memtable& mt, int rank)
        : MergeSource(rank), it_(mt.raw_begin()), end_(mt.raw_end()),
          valid_(it_ != end_) {
        if (valid_) load();
    }

    bool valid() const override { return valid_; }
    const MergeEntry& entry() const override { return current_; }

    void next() override {
        ++it_;
        if (it_ != end_)
            load();
        else
            valid_ = false;
    }

private:
    void load() {
        auto e = *it_;
        current_.key.assign(e.key.data(), e.key.size());
        current_.value.assign(e.value.data(), e.value.size());
        current_.tombstone = e.tombstone;
        current_.seq = e.seq;
    }

    Memtable::RawIterator it_;
    Memtable::RawIterator end_;
    MergeEntry current_;
    bool valid_;
};

// ── SSTable adapter ──────────────────────────────────────────────────────────

class SSTableMergeSource : public MergeSource {
public:
    SSTableMergeSource(const SSTable& sst, int rank)
        : MergeSource(rank), it_(sst.begin()), end_(sst.end()),
          valid_(it_ != end_) {
        if (valid_) load();
    }

    SSTableMergeSource(const SSTable& sst, int rank,
                       std::string_view seek_target)
        : MergeSource(rank), it_(sst.seek(seek_target)), end_(sst.end()),
          valid_(it_ != end_) {
        if (valid_) load();
    }

    bool valid() const override { return valid_; }
    const MergeEntry& entry() const override { return current_; }

    void next() override {
        ++it_;
        if (it_ != end_)
            load();
        else
            valid_ = false;
    }

private:
    void load() {
        const auto& e = *it_;
        current_.key = e.key;
        current_.value = e.value;
        current_.tombstone = e.tombstone;
        current_.seq = e.seq;
    }

    SSTable::Iterator it_;
    SSTable::Iterator end_;
    MergeEntry current_;
    bool valid_;
};

// ── Snapshot (materialized vector) adapter ───────────────────────────────────

class SnapshotMergeSource : public MergeSource {
public:
    SnapshotMergeSource(std::vector<MergeEntry> entries, int rank)
        : MergeSource(rank), entries_(std::move(entries)), idx_(0) {}

    bool valid() const override { return idx_ < entries_.size(); }
    const MergeEntry& entry() const override { return entries_[idx_]; }
    void next() override { ++idx_; }

private:
    std::vector<MergeEntry> entries_;
    size_t idx_;
};

// ── k-way merging iterator ───────────────────────────────────────────────────

class MergeIterator {
public:
    explicit MergeIterator(std::vector<std::unique_ptr<MergeSource>> sources,
                           bool keep_tombstones = false,
                           uint64_t snapshot_seq = UINT64_MAX,
                           bool yield_all_versions = false);

    bool valid() const { return valid_; }
    std::string_view key() const { return current_key_; }
    std::string_view value() const { return current_value_; }
    bool tombstone() const { return current_tombstone_; }
    uint64_t seq() const { return current_seq_; }
    void next();

private:
    void advance();

    struct Cmp {
        bool operator()(MergeSource* a, MergeSource* b) const {
            int c = a->entry().key.compare(b->entry().key);
            if (c != 0) return c > 0;              // min-heap on key
            return a->entry().seq < b->entry().seq; // highest seq first for ties
        }
    };

    std::vector<std::unique_ptr<MergeSource>> sources_;
    std::priority_queue<MergeSource*, std::vector<MergeSource*>, Cmp> heap_;
    std::string current_key_;
    std::string current_value_;
    bool current_tombstone_ = false;
    uint64_t current_seq_ = 0;
    bool keep_tombstones_;
    uint64_t snapshot_seq_;
    bool yield_all_versions_;
    bool valid_ = false;
};

}  // namespace keystone

#pragma once

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

// ── Memtable adapter ─────────────────────────────────────────────────────────

class MemtableMergeSource : public MergeSource {
public:
    MemtableMergeSource(const Memtable& mt, int rank)
        : MergeSource(rank), it_(mt.begin()), end_(mt.end()),
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
    }

    Memtable::Iterator it_;
    Memtable::Iterator end_;
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
    }

    SSTable::Iterator it_;
    SSTable::Iterator end_;
    MergeEntry current_;
    bool valid_;
};

// ── k-way merging iterator ───────────────────────────────────────────────────

class MergeIterator {
public:
    explicit MergeIterator(std::vector<std::unique_ptr<MergeSource>> sources,
                           bool keep_tombstones = false);

    bool valid() const { return valid_; }
    std::string_view key() const { return current_key_; }
    std::string_view value() const { return current_value_; }
    bool tombstone() const { return current_tombstone_; }
    void next();

private:
    void advance();

    struct Cmp {
        bool operator()(MergeSource* a, MergeSource* b) const {
            int c = a->entry().key.compare(b->entry().key);
            if (c != 0) return c > 0;          // min-heap on key
            return a->rank() < b->rank();       // max-rank first for ties
        }
    };

    std::vector<std::unique_ptr<MergeSource>> sources_;
    std::priority_queue<MergeSource*, std::vector<MergeSource*>, Cmp> heap_;
    std::string current_key_;
    std::string current_value_;
    bool current_tombstone_ = false;
    bool keep_tombstones_;
    bool valid_ = false;
};

}  // namespace keystone

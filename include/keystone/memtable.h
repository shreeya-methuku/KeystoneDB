#pragma once

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace keystone {

class Memtable {
public:
    enum class Status { NotFound, Found, Deleted };

    struct Entry {
        std::string_view key;
        std::string_view value;
        bool tombstone;
        uint64_t seq;
    };

    explicit Memtable(int max_height = 12);
    ~Memtable();

    Memtable(const Memtable&) = delete;
    Memtable& operator=(const Memtable&) = delete;

    void put(std::string_view key, std::string_view value, uint64_t seq);
    void del(std::string_view key, uint64_t seq);
    Status lookup(std::string_view key, std::string* out_value,
                  uint64_t snapshot_seq = UINT64_MAX) const;
    size_t approx_bytes() const;

private:
    struct Node {
        std::string key;
        std::string value;
        bool tombstone;
        uint64_t seq;
        std::vector<Node*> forward;
        Node(std::string k, std::string v, bool tomb, uint64_t s, int height)
            : key(std::move(k)), value(std::move(v)), tombstone(tomb), seq(s),
              forward(static_cast<size_t>(height), nullptr) {}
    };

    static bool key_less(const Node* n, std::string_view k, uint64_t s) {
        int cmp = n->key.compare({k.data(), k.size()});
        if (cmp != 0) return cmp < 0;
        return n->seq > s;
    }

public:
    // Default iterator: yields newest version per key (dedup).
    class Iterator {
    public:
        Entry operator*() const {
            return {node_->key, node_->value, node_->tombstone, node_->seq};
        }
        Iterator& operator++() {
            std::string_view cur = node_->key;
            do { node_ = node_->forward[0]; }
            while (node_ && node_->key == cur);
            return *this;
        }
        bool operator!=(const Iterator& o) const { return node_ != o.node_; }
        bool operator==(const Iterator& o) const { return node_ == o.node_; }
    private:
        friend class Memtable;
        explicit Iterator(const Node* n) : node_(n) {}
        const Node* node_;
    };

    // Raw iterator: yields every version in (key ASC, seq DESC) order.
    class RawIterator {
    public:
        Entry operator*() const {
            return {node_->key, node_->value, node_->tombstone, node_->seq};
        }
        RawIterator& operator++() { node_ = node_->forward[0]; return *this; }
        bool operator!=(const RawIterator& o) const { return node_ != o.node_; }
        bool operator==(const RawIterator& o) const { return node_ == o.node_; }
    private:
        friend class Memtable;
        explicit RawIterator(const Node* n) : node_(n) {}
        const Node* node_;
    };

    Iterator begin() const;
    Iterator end() const;
    Iterator lower_bound(std::string_view key) const;

    RawIterator raw_begin() const;
    RawIterator raw_end() const;
    RawIterator raw_lower_bound(std::string_view key) const;

private:
    static constexpr size_t kNodeOverhead = 64;
    int random_level();

    Node* head_;
    int max_height_;
    int current_height_;
    size_t approx_bytes_;
    std::mt19937 rng_;
};

} // namespace keystone

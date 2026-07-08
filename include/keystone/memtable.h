#pragma once

#include <cstddef>
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
    };

    explicit Memtable(int max_height = 12);
    ~Memtable();

    Memtable(const Memtable&) = delete;
    Memtable& operator=(const Memtable&) = delete;

    void put(std::string_view key, std::string_view value);
    void del(std::string_view key);
    Status lookup(std::string_view key, std::string* out_value) const;
    size_t approx_bytes() const;

private:
    struct Node {
        std::string key;
        std::string value;
        bool tombstone;
        std::vector<Node*> forward;
        Node(std::string k, std::string v, bool tomb, int height)
            : key(std::move(k)), value(std::move(v)), tombstone(tomb),
              forward(static_cast<size_t>(height), nullptr) {}
    };

public:
    class Iterator {
    public:
        Entry operator*() const {
            return {node_->key, node_->value, node_->tombstone};
        }
        Iterator& operator++() { node_ = node_->forward[0]; return *this; }
        bool operator!=(const Iterator& o) const { return node_ != o.node_; }
        bool operator==(const Iterator& o) const { return node_ == o.node_; }
    private:
        friend class Memtable;
        explicit Iterator(const Node* n) : node_(n) {}
        const Node* node_;
    };

    Iterator begin() const;
    Iterator end() const;

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

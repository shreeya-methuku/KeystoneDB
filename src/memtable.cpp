#include "keystone/memtable.h"

namespace keystone {

Memtable::Memtable(int max_height)
    : head_(new Node({}, {}, false, 0, max_height)),
      max_height_(max_height),
      current_height_(1),
      approx_bytes_(0),
      rng_(std::random_device{}()) {}

Memtable::~Memtable() {
    Node* n = head_;
    while (n) {
        Node* next = n->forward[0];
        delete n;
        n = next;
    }
}

int Memtable::random_level() {
    int lvl = 1;
    std::bernoulli_distribution coin(0.5);
    while (lvl < max_height_ && coin(rng_))
        ++lvl;
    return lvl;
}

void Memtable::put(std::string_view key, std::string_view value, uint64_t seq) {
    std::vector<Node*> update(static_cast<size_t>(max_height_), head_);
    Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[i] && key_less(x->forward[i], key, seq))
            x = x->forward[i];
        update[static_cast<size_t>(i)] = x;
    }

    int lvl = random_level();
    if (lvl > current_height_) {
        current_height_ = lvl;
    }

    auto* node = new Node(std::string(key), std::string(value), false, seq, lvl);
    for (int i = 0; i < lvl; i++) {
        node->forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)];
        update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = node;
    }
    approx_bytes_ += key.size() + value.size() + kNodeOverhead;
}

void Memtable::del(std::string_view key, uint64_t seq) {
    std::vector<Node*> update(static_cast<size_t>(max_height_), head_);
    Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[i] && key_less(x->forward[i], key, seq))
            x = x->forward[i];
        update[static_cast<size_t>(i)] = x;
    }

    int lvl = random_level();
    if (lvl > current_height_) {
        current_height_ = lvl;
    }

    auto* node = new Node(std::string(key), {}, true, seq, lvl);
    for (int i = 0; i < lvl; i++) {
        node->forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)];
        update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = node;
    }
    approx_bytes_ += key.size() + kNodeOverhead;
}

Memtable::Status Memtable::lookup(std::string_view key,
                                   std::string* out_value,
                                   uint64_t snapshot_seq) const {
    const Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[static_cast<size_t>(i)] &&
               key_less(x->forward[static_cast<size_t>(i)], key, snapshot_seq))
            x = x->forward[static_cast<size_t>(i)];
    }
    x = x->forward[0];

    if (!x || x->key != key)
        return Status::NotFound;
    if (x->tombstone)
        return Status::Deleted;
    if (out_value)
        *out_value = x->value;
    return Status::Found;
}

size_t Memtable::approx_bytes() const { return approx_bytes_; }

Memtable::Iterator Memtable::begin() const {
    return Iterator(head_->forward[0]);
}

Memtable::Iterator Memtable::end() const {
    return Iterator(nullptr);
}

Memtable::Iterator Memtable::lower_bound(std::string_view key) const {
    const Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[static_cast<size_t>(i)] &&
               key_less(x->forward[static_cast<size_t>(i)], key, UINT64_MAX))
            x = x->forward[static_cast<size_t>(i)];
    }
    return Iterator(x->forward[0]);
}

Memtable::RawIterator Memtable::raw_begin() const {
    return RawIterator(head_->forward[0]);
}

Memtable::RawIterator Memtable::raw_end() const {
    return RawIterator(nullptr);
}

Memtable::RawIterator Memtable::raw_lower_bound(std::string_view key) const {
    const Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[static_cast<size_t>(i)] &&
               key_less(x->forward[static_cast<size_t>(i)], key, UINT64_MAX))
            x = x->forward[static_cast<size_t>(i)];
    }
    return RawIterator(x->forward[0]);
}

} // namespace keystone

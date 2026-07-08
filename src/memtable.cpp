#include "keystone/memtable.h"

namespace keystone {

Memtable::Memtable(int max_height)
    : head_(new Node({}, {}, false, max_height)),
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

void Memtable::put(std::string_view key, std::string_view value) {
    std::vector<Node*> update(static_cast<size_t>(max_height_), head_);
    Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->key < key)
            x = x->forward[i];
        update[static_cast<size_t>(i)] = x;
    }
    x = x->forward[0];

    if (x && x->key == key) {
        approx_bytes_ -= x->value.size();
        x->value.assign(value.data(), value.size());
        x->tombstone = false;
        approx_bytes_ += value.size();
        return;
    }

    int lvl = random_level();
    if (lvl > current_height_) {
        current_height_ = lvl;
    }

    auto* node = new Node(std::string(key), std::string(value), false, lvl);
    for (int i = 0; i < lvl; i++) {
        node->forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)];
        update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = node;
    }
    approx_bytes_ += key.size() + value.size() + kNodeOverhead;
}

void Memtable::del(std::string_view key) {
    std::vector<Node*> update(static_cast<size_t>(max_height_), head_);
    Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->key < key)
            x = x->forward[i];
        update[static_cast<size_t>(i)] = x;
    }
    x = x->forward[0];

    if (x && x->key == key) {
        approx_bytes_ -= x->value.size();
        x->value.clear();
        x->tombstone = true;
        return;
    }

    int lvl = random_level();
    if (lvl > current_height_) {
        current_height_ = lvl;
    }

    auto* node = new Node(std::string(key), {}, true, lvl);
    for (int i = 0; i < lvl; i++) {
        node->forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)];
        update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = node;
    }
    approx_bytes_ += key.size() + kNodeOverhead;
}

Memtable::Status Memtable::lookup(std::string_view key,
                                   std::string* out_value) const {
    const Node* x = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (x->forward[static_cast<size_t>(i)] &&
               x->forward[static_cast<size_t>(i)]->key < key)
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

} // namespace keystone

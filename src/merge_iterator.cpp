#include "keystone/merge_iterator.h"

namespace keystone {

MergeIterator::MergeIterator(
    std::vector<std::unique_ptr<MergeSource>> sources,
    bool keep_tombstones)
    : sources_(std::move(sources)), keep_tombstones_(keep_tombstones) {
    for (auto& src : sources_) {
        if (src->valid())
            heap_.push(src.get());
    }
    advance();
}

void MergeIterator::next() {
    advance();
}

void MergeIterator::advance() {
    while (!heap_.empty()) {
        MergeSource* winner = heap_.top();
        std::string winner_key = winner->entry().key;
        std::string winner_value = winner->entry().value;
        bool winner_tombstone = winner->entry().tombstone;

        // Pop and advance every source sitting on this key
        while (!heap_.empty() &&
               heap_.top()->entry().key == winner_key) {
            MergeSource* src = heap_.top();
            heap_.pop();
            src->next();
            if (src->valid())
                heap_.push(src);
        }

        if (keep_tombstones_ || !winner_tombstone) {
            current_key_ = std::move(winner_key);
            current_value_ = std::move(winner_value);
            current_tombstone_ = winner_tombstone;
            valid_ = true;
            return;
        }
    }
    valid_ = false;
}

}  // namespace keystone

#include "keystone/merge_iterator.h"

namespace keystone {

MergeIterator::MergeIterator(
    std::vector<std::unique_ptr<MergeSource>> sources,
    bool keep_tombstones,
    uint64_t snapshot_seq,
    bool yield_all_versions)
    : sources_(std::move(sources)), keep_tombstones_(keep_tombstones),
      snapshot_seq_(snapshot_seq), yield_all_versions_(yield_all_versions) {
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
    if (yield_all_versions_) {
        if (heap_.empty()) {
            valid_ = false;
            return;
        }
        MergeSource* src = heap_.top();
        current_key_ = src->entry().key;
        current_value_ = src->entry().value;
        current_tombstone_ = src->entry().tombstone;
        current_seq_ = src->entry().seq;
        heap_.pop();
        src->next();
        if (src->valid())
            heap_.push(src);
        valid_ = true;
        return;
    }

    while (!heap_.empty()) {
        std::string winner_key = heap_.top()->entry().key;

        std::string best_value;
        bool best_tombstone = false;
        uint64_t best_seq = 0;
        bool found_visible = false;

        while (!heap_.empty() &&
               heap_.top()->entry().key == winner_key) {
            MergeSource* src = heap_.top();
            const auto& e = src->entry();

            if (!found_visible && e.seq <= snapshot_seq_) {
                best_value = e.value;
                best_tombstone = e.tombstone;
                best_seq = e.seq;
                found_visible = true;
            }

            heap_.pop();
            src->next();
            if (src->valid())
                heap_.push(src);
        }

        if (found_visible && (keep_tombstones_ || !best_tombstone)) {
            current_key_ = std::move(winner_key);
            current_value_ = std::move(best_value);
            current_tombstone_ = best_tombstone;
            current_seq_ = best_seq;
            valid_ = true;
            return;
        }
    }
    valid_ = false;
}

}  // namespace keystone

#include "keystone/db.h"
#include "keystone/merge_iterator.h"
#include "coding.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <map>
#include <set>

#include <fcntl.h>
#include <unistd.h>

namespace keystone {

static std::string sst_path(const std::string& dir, int num) {
    char name[32];
    std::snprintf(name, sizeof(name), "%06d.sst", num);
    return dir + "/" + name;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

int DB::alloc_sst_number() {
    std::lock_guard<std::mutex> lock(mu_);
    return next_sst_number_++;
}

bool DB::needs_compaction() const {
    auto ver = current_version_;
    if (ver->files[0].size() >= opts_.compaction_trigger) return true;
    for (int level = 1; level < Version::kMaxLevels - 1; level++) {
        if (level_bytes(*ver, level) > max_level_bytes(level)) return true;
    }
    return false;
}

int DB::total_file_count() const {
    int total = 0;
    for (int l = 0; l < Version::kMaxLevels; l++)
        total += static_cast<int>(current_version_->files[l].size());
    return total;
}

int DB::max_populated_level(const Version& ver) const {
    for (int l = Version::kMaxLevels - 1; l >= 0; l--) {
        if (!ver.files[l].empty()) return l;
    }
    return 0;
}

size_t DB::level_bytes(const Version& ver, int level) const {
    size_t total = 0;
    for (const auto& fm : ver.files[level]) {
        if (fm.table)
            total += static_cast<size_t>(fm.table->entry_count()) * 64;
    }
    return total;
}

size_t DB::max_level_bytes(int level) const {
    if (level <= 0) return 0;
    size_t limit = opts_.l1_max_bytes;
    for (int l = 2; l <= level; l++)
        limit *= opts_.level_size_multiplier;
    return limit;
}

std::vector<ManifestEntry> DB::version_to_manifest(const Version& ver) const {
    std::vector<ManifestEntry> entries;
    for (int l = 0; l < Version::kMaxLevels; l++) {
        for (const auto& fm : ver.files[l]) {
            entries.push_back({fm.number, fm.level, fm.smallest_key, fm.largest_key});
        }
    }
    return entries;
}

uint64_t DB::oldest_live_snapshot() const {
    if (live_snapshots_.empty()) return UINT64_MAX;
    return *live_snapshots_.begin();
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

std::unique_ptr<DB> DB::open(const std::string& dir, Options opts) {
    std::filesystem::create_directories(dir);

    auto db = std::unique_ptr<DB>(new DB());
    db->dir_ = dir;
    db->opts_ = opts;

    // Replay WAL
    std::string wal_path = dir + "/wal.log";
    uint64_t max_wal_seq = 0;
    if (std::filesystem::exists(wal_path)) {
        auto records = WAL::replay(wal_path);
        for (const auto& rec : records) {
            if (rec.type == RecType::Put)
                db->memtable_->put(rec.key, rec.value, rec.seq);
            else
                db->memtable_->del(rec.key, rec.seq);
            if (rec.seq > max_wal_seq) max_wal_seq = rec.seq;
        }
        size_t valid_end = 0;
        for (const auto& rec : records)
            valid_end += 21 + rec.key.size() + rec.value.size();
        std::filesystem::resize_file(wal_path, valid_end);
    }

    // Scan directory for .sst files and stray .sst.tmp files
    std::map<int, std::string> num_to_path;
    int max_num = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        auto p = entry.path();
        if (p.extension() == ".sst") {
            try {
                int num = std::stoi(p.stem().string());
                num_to_path[num] = p.string();
                max_num = std::max(max_num, num);
            } catch (...) { continue; }
        } else if (p.extension() == ".tmp" &&
                   p.stem().extension() == ".sst") {
            std::filesystem::remove(p);
        }
    }

    // Load MANIFEST
    std::string manifest_path = dir + "/MANIFEST";
    auto live = Manifest::load_latest(manifest_path);
    bool manifest_exists = std::filesystem::exists(manifest_path);

    if (live.empty() && !manifest_exists && !num_to_path.empty()) {
        for (const auto& [num, path] : num_to_path)
            live.push_back({num, 0, "", ""});
    }

    for (const auto& entry : live)
        max_num = std::max(max_num, entry.number);

    // Build initial Version
    if (opts.block_cache_bytes > 0)
        db->cache_ = std::make_shared<BlockCache>(opts.block_cache_bytes);

    auto ver = std::make_shared<Version>();
    std::set<int> live_set;
    for (const auto& entry : live) {
        live_set.insert(entry.number);
        auto it = num_to_path.find(entry.number);
        if (it == num_to_path.end()) continue;
        auto table = SSTable::open(it->second, db->cache_);
        int level = entry.level;
        if (level < 0 || level >= Version::kMaxLevels) level = 0;
        std::string sk = entry.smallest_key;
        std::string lk = entry.largest_key;
        if (sk.empty() || lk.empty()) {
            auto begin_it = table->begin();
            auto end_it = table->end();
            if (begin_it != end_it) {
                sk = (*begin_it).key;
                for (auto sit = table->begin(); sit != end_it; ++sit)
                    lk = (*sit).key;
            }
        }
        ver->files[level].push_back({entry.number, level, sk, lk, table});
    }

    for (auto& level_files : ver->files) {
        std::sort(level_files.begin(), level_files.end(),
                  [](const FileMeta& a, const FileMeta& b) {
                      return a.number < b.number;
                  });
    }
    for (int l = 1; l < Version::kMaxLevels; l++) {
        std::sort(ver->files[l].begin(), ver->files[l].end(),
                  [](const FileMeta& a, const FileMeta& b) {
                      return a.smallest_key < b.smallest_key;
                  });
    }

    db->current_version_ = ver;

    // Delete orphan .sst files not in the live set
    for (const auto& [num, path] : num_to_path) {
        if (live_set.find(num) == live_set.end())
            std::filesystem::remove(path);
    }

    db->next_sst_number_ = max_num + 1;
    if (db->next_sst_number_ < 1) db->next_sst_number_ = 1;

    uint64_t max_sst_seq = 0;
    for (int l = 0; l < Version::kMaxLevels; l++) {
        for (const auto& fm : ver->files[l]) {
            if (fm.table && fm.table->max_seq() > max_sst_seq)
                max_sst_seq = fm.table->max_seq();
        }
    }
    db->next_seq_ = 1 + std::max(max_wal_seq, max_sst_seq);

    db->manifest_ = Manifest::open(manifest_path);

    if (!manifest_exists && !live.empty())
        db->manifest_->append_snapshot(live);

    db->wal_ = WAL::open(wal_path, opts.sync);

    // Start background compaction thread last, after all state is initialized
    db->compaction_thread_ = std::thread([raw = db.get()] {
        raw->compaction_loop();
    });

    return db;
}

DB::~DB() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_ = true;
    }
    cv_.notify_one();
    if (compaction_thread_.joinable())
        compaction_thread_.join();
}

// ── Writer operations (leader/follower group commit) ────────────────────────

void DB::put(std::string_view key, std::string_view value) {
    write(RecType::Put, key, value);
}

void DB::del(std::string_view key) {
    write(RecType::Delete, key, {});
}

void DB::write(RecType type, std::string_view key, std::string_view value) {
    WriteRequest req{type, key, value, false};

    std::unique_lock<std::mutex> wlock(write_mu_);
    write_queue_.push_back(&req);

    // Wait until we become the leader (front of queue) or a leader handles us.
    write_cv_.wait(wlock, [&] {
        return req.done || write_queue_.front() == &req;
    });
    if (req.done) return;

    // === LEADER: snapshot the current batch ===
    std::vector<WriteRequest*> batch(write_queue_.begin(), write_queue_.end());
    for (auto* r : batch)
        r->seq = next_seq_++;
    wlock.unlock();

    std::vector<WAL::Record> wal_records;
    wal_records.reserve(batch.size());
    for (auto* r : batch)
        wal_records.push_back(
            {r->type, r->seq, std::string(r->key), std::string(r->value)});

    {
        std::lock_guard<std::mutex> lock(mu_);
        wal_->append_batch(wal_records);
        for (auto* r : batch) {
            if (r->type == RecType::Put)
                memtable_->put(r->key, r->value, r->seq);
            else
                memtable_->del(r->key, r->seq);
        }
    }

    maybe_flush();

    wlock.lock();
    for (size_t i = 0; i < batch.size(); i++)
        write_queue_.pop_front();
    for (auto* r : batch)
        r->done = true;
    write_cv_.notify_all();
}

void DB::maybe_flush() {
    if (memtable_->approx_bytes() >= opts_.flush_threshold_bytes)
        flush();
}

void DB::flush() {
    std::lock_guard<std::mutex> lock(mu_);
    if (memtable_->approx_bytes() == 0) return;

    int new_num = next_sst_number_++;
    std::string final_path = sst_path(dir_, new_num);

    SSTableWriter writer(final_path);
    for (auto it = memtable_->raw_begin(); it != memtable_->raw_end(); ++it) {
        auto e = *it;
        writer.add(e.key, e.value, e.tombstone, e.seq);
    }
    writer.finish();
    SSTableWriter::install(writer.temp_path(), final_path, dir_);

    auto new_ver = std::make_shared<Version>();
    for (int l = 0; l < Version::kMaxLevels; l++)
        new_ver->files[l] = current_version_->files[l];

    auto table = SSTable::open(final_path, cache_);
    new_ver->files[0].push_back(
        {new_num, 0, writer.smallest_key(), writer.largest_key(), table});

    manifest_->append_snapshot(version_to_manifest(*new_ver));
    current_version_ = new_ver;

    wal_.reset();
    std::string wal_path = dir_ + "/wal.log";
    std::filesystem::resize_file(wal_path, 0);
    wal_ = WAL::open(wal_path, opts_.sync);

    memtable_ = std::make_unique<Memtable>();

    if (current_version_->files[0].size() >= opts_.compaction_trigger &&
        !compaction_pending_ && !compaction_running_) {
        compaction_pending_ = true;
        cv_.notify_one();
    }
}

// ── Snapshot operations ────────────────────────────────────────────────────

Snapshot DB::get_snapshot() {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t s = next_seq_ - 1;
    live_snapshots_.insert(s);
    return Snapshot{s};
}

void DB::release_snapshot(const Snapshot& snap) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = live_snapshots_.find(snap.seq);
    if (it != live_snapshots_.end())
        live_snapshots_.erase(it);
}

// ── Reader operations ───────────────────────────────────────────────────────

std::optional<std::string> DB::get(std::string_view key) {
    return get_impl(key, UINT64_MAX);
}

std::optional<std::string> DB::get(std::string_view key, const Snapshot& snap) {
    return get_impl(key, snap.seq);
}

std::optional<std::string> DB::get_impl(std::string_view key, uint64_t snap_seq) {
    std::string value;
    Memtable::Status status;
    std::shared_ptr<const Version> ver;
    {
        std::lock_guard<std::mutex> lock(mu_);
        status = memtable_->lookup(key, &value, snap_seq);
        ver = current_version_;
    }

    if (status == Memtable::Status::Found) return value;
    if (status == Memtable::Status::Deleted) return std::nullopt;

    // L0: check all files newest-first (back to front)
    const auto& l0 = ver->files[0];
    for (auto it = l0.rbegin(); it != l0.rend(); ++it) {
        if (!it->table->may_contain(key)) continue;
        auto result = it->table->get(key, snap_seq);
        if (result.status == SSTable::LookupStatus::Found)
            return result.value;
        if (result.status == SSTable::LookupStatus::Deleted)
            return std::nullopt;
    }

    // L1+: binary search for the single covering file
    for (int level = 1; level < Version::kMaxLevels; level++) {
        const auto& files = ver->files[level];
        if (files.empty()) continue;

        auto it = std::upper_bound(files.begin(), files.end(), key,
            [](std::string_view k, const FileMeta& f) {
                return k < f.smallest_key;
            });
        if (it != files.begin()) {
            --it;
            if (key <= it->largest_key) {
                if (!it->table->may_contain(key)) continue;
                auto result = it->table->get(key, snap_seq);
                if (result.status == SSTable::LookupStatus::Found)
                    return result.value;
                if (result.status == SSTable::LookupStatus::Deleted)
                    return std::nullopt;
            }
        }
    }

    return std::nullopt;
}

void DB::scan(std::string_view start, std::string_view end,
              const std::function<void(std::string_view,
                                       std::string_view)>& emit) const {
    scan_impl(start, end, emit, UINT64_MAX);
}

void DB::scan(std::string_view start, std::string_view end,
              const std::function<void(std::string_view,
                                       std::string_view)>& emit,
              const Snapshot& snap) const {
    scan_impl(start, end, emit, snap.seq);
}

void DB::scan_impl(std::string_view start, std::string_view end,
                   const std::function<void(std::string_view,
                                            std::string_view)>& emit,
                   uint64_t snap_seq) const {
    std::vector<MergeEntry> mt_snapshot;
    std::shared_ptr<const Version> ver;
    {
        std::lock_guard<std::mutex> lock(mu_);
        ver = current_version_;
        for (auto it = memtable_->raw_lower_bound(start);
             it != memtable_->raw_end(); ++it) {
            auto e = *it;
            if (e.key > end) break;
            mt_snapshot.push_back(
                {std::string(e.key), std::string(e.value), e.tombstone, e.seq});
        }
    }

    std::vector<std::unique_ptr<MergeSource>> sources;
    sources.push_back(std::make_unique<SnapshotMergeSource>(
        std::move(mt_snapshot), 0));

    // L0: all files
    for (const auto& fm : ver->files[0])
        sources.push_back(std::make_unique<SSTableMergeSource>(
            *fm.table, 0, start));

    // L1+: only files whose range overlaps [start, end]
    for (int level = 1; level < Version::kMaxLevels; level++) {
        for (const auto& fm : ver->files[level]) {
            if (fm.largest_key < start || fm.smallest_key > end) continue;
            sources.push_back(std::make_unique<SSTableMergeSource>(
                *fm.table, 0, start));
        }
    }

    MergeIterator iter(std::move(sources), false, snap_seq);

    while (iter.valid() && iter.key() <= end) {
        emit(iter.key(), iter.value());
        iter.next();
    }
}

uint64_t DB::wal_fsync_count() const {
    return wal_->fsync_count();
}

// ── Public compaction trigger (blocks until complete) ────────────────────────

void DB::compact() {
    std::unique_lock<std::mutex> lock(mu_);
    force_compact_ = true;
    while (true) {
        if (total_file_count() <= 1) break;
        bool l0_empty = current_version_->files[0].empty();
        int populated = 0;
        for (int l = 1; l < Version::kMaxLevels; l++) {
            if (!current_version_->files[l].empty()) populated++;
        }
        if (l0_empty && populated <= 1) break;

        if (!compaction_pending_ && !compaction_running_) {
            compaction_pending_ = true;
            cv_.notify_one();
        }
        cv_.wait(lock, [this] {
            return !compaction_pending_ && !compaction_running_;
        });
    }
    force_compact_ = false;
}

// ── Compaction picking ──────────────────────────────────────────────────────

DB::CompactionJob DB::pick_compaction(const std::shared_ptr<const Version>& ver) {
    CompactionJob job;

    // Force compact: full merge of all files across all levels
    if (force_compact_) {
        for (int l = 0; l < Version::kMaxLevels; l++) {
            for (const auto& fm : ver->files[l])
                job.inputs.push_back(fm);
        }
        if (job.inputs.size() >= 2) {
            job.output_level = 1;
            job.is_bottommost = true;
        }
        return job;
    }

    // Priority 1: L0 compaction
    if (ver->files[0].size() >= opts_.compaction_trigger) {
        std::string range_min, range_max;
        for (const auto& fm : ver->files[0]) {
            job.inputs.push_back(fm);
            if (range_min.empty() || fm.smallest_key < range_min)
                range_min = fm.smallest_key;
            if (range_max.empty() || fm.largest_key > range_max)
                range_max = fm.largest_key;
        }

        for (const auto& fm : ver->files[1]) {
            if (fm.largest_key < range_min || fm.smallest_key > range_max)
                continue;
            job.inputs.push_back(fm);
        }

        job.output_level = 1;
        int max_level = max_populated_level(*ver);
        job.is_bottommost = (job.output_level >= max_level) ||
                            (max_level == 0);
        return job;
    }

    // Priority 2: Li overflow (i >= 1)
    for (int level = 1; level < Version::kMaxLevels - 1; level++) {
        if (level_bytes(*ver, level) <= max_level_bytes(level)) continue;

        const FileMeta* pick = nullptr;
        for (const auto& fm : ver->files[level]) {
            if (!pick || fm.number < pick->number)
                pick = &fm;
        }
        if (!pick) continue;

        job.inputs.push_back(*pick);

        int next_level = level + 1;
        for (const auto& fm : ver->files[next_level]) {
            if (fm.largest_key < pick->smallest_key ||
                fm.smallest_key > pick->largest_key)
                continue;
            job.inputs.push_back(fm);
        }

        job.output_level = next_level;
        int max_level = max_populated_level(*ver);
        job.is_bottommost = (job.output_level >= max_level);
        return job;
    }

    return job;
}

// ── Background compaction thread ────────────────────────────────────────────

void DB::compaction_loop() {
    while (true) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return stop_ || compaction_pending_; });
        if (stop_) return;

        compaction_pending_ = false;
        compaction_running_ = true;

        // SNAPSHOT and PICK under lock
        auto snap_ver = current_version_;
        auto job = pick_compaction(snap_ver);
        uint64_t oldest_snap = oldest_live_snapshot();

        if (job.inputs.empty()) {
            compaction_running_ = false;
            cv_.notify_all();
            continue;
        }

        std::set<int> input_numbers;
        for (const auto& fm : job.inputs)
            input_numbers.insert(fm.number);

        lock.unlock();

        // MERGE with no lock held
        std::vector<FileMeta> outputs;

        {
            std::vector<std::unique_ptr<MergeSource>> sources;
            for (const auto& fm : job.inputs)
                sources.push_back(std::make_unique<SSTableMergeSource>(
                    *fm.table, 0));

            MergeIterator iter(std::move(sources), true, UINT64_MAX, true);

            SSTableWriter* writer = nullptr;
            std::unique_ptr<SSTableWriter> writer_owner;
            int cur_num = 0;
            std::string cur_path;
            size_t cur_bytes = 0;

            auto finish_writer = [&]() {
                if (!writer) return;
                writer->finish();
                SSTableWriter::install(writer->temp_path(), cur_path, dir_);
                auto table = SSTable::open(cur_path, cache_);
                outputs.push_back({cur_num, job.output_level,
                                   writer->smallest_key(), writer->largest_key(),
                                   table});
                writer_owner.reset();
                writer = nullptr;
            };

            auto start_writer = [&]() {
                cur_num = alloc_sst_number();
                cur_path = sst_path(dir_, cur_num);
                writer_owner = std::make_unique<SSTableWriter>(cur_path);
                writer = writer_owner.get();
                cur_bytes = 0;
            };

            std::string prev_key;
            bool kept_anchor = false;

            while (iter.valid()) {
                bool is_new_key = (iter.key() != prev_key);
                if (is_new_key) {
                    prev_key = std::string(iter.key());
                    kept_anchor = false;
                }

                bool keep = false;
                if (iter.seq() > oldest_snap) {
                    keep = true;
                } else if (!kept_anchor) {
                    kept_anchor = true;
                    if (iter.tombstone() && job.is_bottommost) {
                        keep = false;
                    } else {
                        keep = true;
                    }
                }

                if (keep) {
                    if (!writer) start_writer();

                    writer->add(iter.key(), iter.value(), iter.tombstone(),
                                iter.seq());
                    cur_bytes += 17 + iter.key().size() + iter.value().size();

                    if (cur_bytes >= opts_.target_file_size) {
                        finish_writer();
                    }
                }

                iter.next();
            }

            finish_writer();
        }

        // COMMIT under lock
        lock.lock();
        if (!outputs.empty()) {
            auto cur_ver = current_version_;
            auto new_ver = std::make_shared<Version>();

            for (int l = 0; l < Version::kMaxLevels; l++) {
                for (const auto& fm : cur_ver->files[l]) {
                    if (input_numbers.find(fm.number) == input_numbers.end())
                        new_ver->files[l].push_back(fm);
                }
            }

            for (const auto& out : outputs)
                new_ver->files[job.output_level].push_back(out);

            // Sort output level by smallest_key for L1+
            if (job.output_level >= 1) {
                std::sort(new_ver->files[job.output_level].begin(),
                          new_ver->files[job.output_level].end(),
                          [](const FileMeta& a, const FileMeta& b) {
                              return a.smallest_key < b.smallest_key;
                          });
            }

            manifest_->append_snapshot(version_to_manifest(*new_ver));

            // Mark old inputs for deletion
            for (const auto& fm : snap_ver->files[0]) {
                if (input_numbers.count(fm.number))
                    fm.table->set_unlink_on_destroy(true);
            }
            for (int l = 1; l < Version::kMaxLevels; l++) {
                for (const auto& fm : snap_ver->files[l]) {
                    if (input_numbers.count(fm.number))
                        fm.table->set_unlink_on_destroy(true);
                }
            }

            current_version_ = new_ver;

            snap_ver.reset();
            cur_ver.reset();
        }

        if (needs_compaction())
            compaction_pending_ = true;

        compaction_running_ = false;
        cv_.notify_all();
    }
}

}  // namespace keystone

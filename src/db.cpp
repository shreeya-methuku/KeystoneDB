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

// ── Lifecycle ───────────────────────────────────────────────────────────────

std::unique_ptr<DB> DB::open(const std::string& dir, Options opts) {
    std::filesystem::create_directories(dir);

    auto db = std::unique_ptr<DB>(new DB());
    db->dir_ = dir;
    db->opts_ = opts;

    // Replay WAL
    std::string wal_path = dir + "/wal.log";
    if (std::filesystem::exists(wal_path)) {
        auto records = WAL::replay(wal_path);
        for (const auto& rec : records) {
            if (rec.type == RecType::Put)
                db->memtable_->put(rec.key, rec.value);
            else
                db->memtable_->del(rec.key);
        }
        size_t valid_end = 0;
        for (const auto& rec : records)
            valid_end += 13 + rec.key.size() + rec.value.size();
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
    std::vector<int> live;
    bool manifest_exists = std::filesystem::exists(manifest_path);

    if (manifest_exists)
        live = Manifest::load_latest(manifest_path);

    if (!manifest_exists && !num_to_path.empty()) {
        for (const auto& [num, path] : num_to_path)
            live.push_back(num);
    }

    for (int num : live)
        max_num = std::max(max_num, num);

    // Build initial Version
    auto ver = std::make_shared<Version>();
    std::set<int> live_set(live.begin(), live.end());
    for (int num : live) {
        auto it = num_to_path.find(num);
        if (it == num_to_path.end()) continue;
        ver->ssts.push_back(SSTable::open(it->second));
    }
    db->current_version_ = ver;
    db->live_numbers_ = live;

    // Delete orphan .sst files not in the live set
    for (const auto& [num, path] : num_to_path) {
        if (live_set.find(num) == live_set.end())
            std::filesystem::remove(path);
    }

    db->next_sst_number_ = max_num + 1;
    if (db->next_sst_number_ < 1) db->next_sst_number_ = 1;

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

    if (write_queue_.front() != &req) {
        write_cv_.wait(wlock, [&] { return req.done; });
        return;
    }

    // === LEADER: snapshot the current batch ===
    std::vector<WriteRequest*> batch(write_queue_.begin(), write_queue_.end());
    wlock.unlock();

    // Build WAL records (owned copies for encoding)
    std::vector<WAL::Record> wal_records;
    wal_records.reserve(batch.size());
    for (auto* r : batch)
        wal_records.push_back(
            {r->type, std::string(r->key), std::string(r->value)});

    // WAL batch write + fsync + memtable apply under mu_.
    // Crash safety: done=true is set only AFTER append_batch returns (which
    // includes fsync for EveryWrite mode). If the leader crashes mid-write,
    // no caller was told its write succeeded; WAL replay stops at the first
    // bad CRC, recovering a correct prefix of the batch.
    {
        std::lock_guard<std::mutex> lock(mu_);
        wal_->append_batch(wal_records);
        for (auto* r : batch) {
            if (r->type == RecType::Put)
                memtable_->put(r->key, r->value);
            else
                memtable_->del(r->key);
        }
    }

    maybe_flush();

    // Pop batch, mark done, wake followers + next leader
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
    for (auto it = memtable_->begin(); it != memtable_->end(); ++it) {
        auto e = *it;
        writer.add(e.key, e.value, e.tombstone);
    }
    writer.finish();
    SSTableWriter::install(writer.temp_path(), final_path, dir_);

    std::vector<int> new_live = live_numbers_;
    new_live.push_back(new_num);
    manifest_->append_snapshot(new_live);
    live_numbers_ = new_live;

    auto new_ver = std::make_shared<Version>();
    new_ver->ssts = current_version_->ssts;
    new_ver->ssts.push_back(SSTable::open(final_path));
    current_version_ = new_ver;

    wal_.reset();
    std::string wal_path = dir_ + "/wal.log";
    std::filesystem::resize_file(wal_path, 0);
    wal_ = WAL::open(wal_path, opts_.sync);

    memtable_ = std::make_unique<Memtable>();

    if (current_version_->ssts.size() >= opts_.compaction_trigger &&
        !compaction_pending_ && !compaction_running_) {
        compaction_pending_ = true;
        cv_.notify_one();
    }
}

// ── Reader operations ───────────────────────────────────────────────────────

std::optional<std::string> DB::get(std::string_view key) {
    std::string value;
    Memtable::Status status;
    std::shared_ptr<const Version> ver;
    {
        std::lock_guard<std::mutex> lock(mu_);
        status = memtable_->lookup(key, &value);
        ver = current_version_;
    }

    if (status == Memtable::Status::Found) return value;
    if (status == Memtable::Status::Deleted) return std::nullopt;

    for (auto it = ver->ssts.rbegin(); it != ver->ssts.rend(); ++it) {
        auto result = (*it)->get(key);
        if (result.status == SSTable::LookupStatus::Found)
            return result.value;
        if (result.status == SSTable::LookupStatus::Deleted)
            return std::nullopt;
    }

    return std::nullopt;
}

// Holds mu_ for the entire scan. This blocks the writer during scans but is
// the simplest correct approach — the memtable is a skiplist with raw Node
// pointers, and concurrent iteration + mutation would be a data race.
void DB::scan(std::string_view start, std::string_view end,
              const std::function<void(std::string_view,
                                       std::string_view)>& emit) const {
    std::lock_guard<std::mutex> lock(mu_);

    auto ver = current_version_;

    std::vector<std::unique_ptr<MergeSource>> sources;

    int mt_rank = static_cast<int>(ver->ssts.size());
    sources.push_back(
        std::make_unique<MemtableMergeSource>(*memtable_, mt_rank));

    for (size_t i = 0; i < ver->ssts.size(); i++)
        sources.push_back(std::make_unique<SSTableMergeSource>(
            *ver->ssts[i], static_cast<int>(i)));

    MergeIterator iter(std::move(sources));

    while (iter.valid() && iter.key() < start)
        iter.next();

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
    if (current_version_->ssts.size() < 2) return;
    if (!compaction_pending_ && !compaction_running_) {
        compaction_pending_ = true;
        cv_.notify_one();
    }
    cv_.wait(lock, [this] {
        return !compaction_pending_ && !compaction_running_;
    });
}

// ── Background compaction thread ────────────────────────────────────────────

void DB::compaction_loop() {
    while (true) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return stop_ || compaction_pending_; });
        if (stop_) return;

        compaction_pending_ = false;
        compaction_running_ = true;

        // SNAPSHOT under lock
        auto snap_ver = current_version_;
        if (snap_ver->ssts.size() < 2) {
            compaction_running_ = false;
            cv_.notify_all();
            continue;
        }

        std::set<int> input_numbers;
        for (const auto& sst : snap_ver->ssts)
            input_numbers.insert(sst->number());
        int merged_num = next_sst_number_++;
        lock.unlock();

        // MERGE with no lock held
        std::string final_path = sst_path(dir_, merged_num);

        std::vector<std::unique_ptr<MergeSource>> sources;
        for (size_t i = 0; i < snap_ver->ssts.size(); i++)
            sources.push_back(std::make_unique<SSTableMergeSource>(
                *snap_ver->ssts[i], static_cast<int>(i)));

        MergeIterator iter(std::move(sources), false);

        SSTableWriter writer(final_path);
        while (iter.valid()) {
            writer.add(iter.key(), iter.value(), iter.tombstone());
            iter.next();
        }
        writer.finish();
        SSTableWriter::install(writer.temp_path(), final_path, dir_);

        // COMMIT under lock
        lock.lock();
        auto cur_ver = current_version_;

        auto new_ver = std::make_shared<Version>();
        auto merged_sst = SSTable::open(final_path);
        new_ver->ssts.push_back(merged_sst);

        std::vector<int> new_live = {merged_num};
        for (const auto& sst : cur_ver->ssts) {
            if (input_numbers.find(sst->number()) == input_numbers.end()) {
                new_ver->ssts.push_back(sst);
                new_live.push_back(sst->number());
            }
        }

        manifest_->append_snapshot(new_live);
        live_numbers_ = new_live;

        for (const auto& sst : snap_ver->ssts)
            sst->set_unlink_on_destroy(true);

        current_version_ = new_ver;

        if (current_version_->ssts.size() >= opts_.compaction_trigger)
            compaction_pending_ = true;

        compaction_running_ = false;
        cv_.notify_all();
    }
}

}  // namespace keystone

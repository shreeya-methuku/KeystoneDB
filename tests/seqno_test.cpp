#include <gtest/gtest.h>
#include "keystone/db.h"
#include "keystone/sstable.h"

#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_seq_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::filesystem::remove_all(path); }
};

int count_sst_files(const std::string& dir) {
    int n = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".sst") n++;
    }
    return n;
}

bool wait_for_sst_count(const std::string& dir, int expected,
                        int timeout_ms = 5000) {
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 5) {
        if (count_sst_files(dir) == expected) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return count_sst_files(dir) == expected;
}
}  // namespace

TEST(SeqnoTest, MonotonicSeqnos) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);
    for (int i = 0; i < 20; i++) {
        char key[32], val[32];
        std::snprintf(key, sizeof(key), "key_%04d", i);
        std::snprintf(val, sizeof(val), "val_%04d", i);
        db->put(key, val);
    }
    db->flush();

    std::string sst_path;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") {
            sst_path = entry.path().string();
            break;
        }
    }
    ASSERT_FALSE(sst_path.empty());

    auto sst = keystone::SSTable::open(sst_path);
    uint64_t prev_seq = 0;
    int count = 0;
    for (auto it = sst->begin(); it != sst->end(); ++it) {
        EXPECT_GT((*it).seq, prev_seq) << "Seq not monotonic at entry " << count;
        prev_seq = (*it).seq;
        count++;
    }
    EXPECT_EQ(count, 20);
}

TEST(SeqnoTest, RecoverySeqnoContinuity) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    uint64_t max_seq_before = 0;

    {
        auto db = keystone::DB::open(dir.path, opts);
        for (int i = 0; i < 10; i++) {
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "pre_%04d", i);
            std::snprintf(val, sizeof(val), "val_%04d", i);
            db->put(key, val);
        }
        db->flush();

        for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
            if (entry.path().extension() == ".sst") {
                auto sst = keystone::SSTable::open(entry.path().string());
                if (sst->max_seq() > max_seq_before)
                    max_seq_before = sst->max_seq();
            }
        }
    }

    ASSERT_GT(max_seq_before, 0u);

    {
        auto db = keystone::DB::open(dir.path, opts);
        for (int i = 0; i < 5; i++) {
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "post_%04d", i);
            std::snprintf(val, sizeof(val), "val_%04d", i);
            db->put(key, val);
        }
        db->flush();

        uint64_t max_seq_after = 0;
        for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
            if (entry.path().extension() == ".sst") {
                auto sst = keystone::SSTable::open(entry.path().string());
                if (sst->max_seq() > max_seq_after)
                    max_seq_after = sst->max_seq();
            }
        }
        EXPECT_GT(max_seq_after, max_seq_before);
    }
}

TEST(SeqnoTest, SeqnoResolvesRecency) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);
    db->put("key", "first");
    db->flush();
    db->put("key", "second");

    EXPECT_EQ(db->get("key"), "second");

    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });
    ASSERT_EQ(scan_result.size(), 1u);
    EXPECT_EQ(scan_result[0].second, "second");
}

TEST(SeqnoTest, FlushPreservesSeq) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);
    db->put("a", "1");
    db->put("b", "2");
    db->put("c", "3");
    db->flush();

    std::string sst_path;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") {
            sst_path = entry.path().string();
            break;
        }
    }
    ASSERT_FALSE(sst_path.empty());
    auto sst = keystone::SSTable::open(sst_path);

    std::vector<uint64_t> seqs;
    for (auto it = sst->begin(); it != sst->end(); ++it)
        seqs.push_back((*it).seq);

    ASSERT_EQ(seqs.size(), 3u);
    EXPECT_GT(seqs[0], 0u);
    EXPECT_GT(seqs[1], seqs[0]);
    EXPECT_GT(seqs[2], seqs[1]);
}

TEST(SeqnoTest, CompactionPreservesSeq) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    for (int batch = 0; batch < 4; batch++) {
        for (int i = 0; i < 5; i++) {
            int idx = batch * 5 + i;
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "key_%04d", idx);
            std::snprintf(val, sizeof(val), "val_%04d", idx);
            db->put(key, val);
        }
        db->flush();
    }

    db->compact();
    ASSERT_TRUE(wait_for_sst_count(dir.path, 1))
        << "Expected 1 SST file, got " << count_sst_files(dir.path);

    std::string sst_path;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") {
            sst_path = entry.path().string();
            break;
        }
    }
    auto sst = keystone::SSTable::open(sst_path);

    uint64_t prev_seq = 0;
    int count = 0;
    for (auto it = sst->begin(); it != sst->end(); ++it) {
        EXPECT_GT((*it).seq, 0u) << "Seq should be >0 at entry " << count;
        prev_seq = (*it).seq;
        count++;
    }
    EXPECT_EQ(count, 20);
    (void)prev_seq;
}

TEST(SeqnoTest, FooterMaxSeq) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);
    for (int i = 0; i < 10; i++) {
        char key[32], val[32];
        std::snprintf(key, sizeof(key), "key_%04d", i);
        std::snprintf(val, sizeof(val), "val_%04d", i);
        db->put(key, val);
    }
    db->flush();

    std::string sst_path;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") {
            sst_path = entry.path().string();
            break;
        }
    }
    ASSERT_FALSE(sst_path.empty());
    auto sst = keystone::SSTable::open(sst_path);

    uint64_t actual_max = 0;
    for (auto it = sst->begin(); it != sst->end(); ++it) {
        if ((*it).seq > actual_max) actual_max = (*it).seq;
    }
    EXPECT_EQ(sst->max_seq(), actual_max);
    EXPECT_GT(sst->max_seq(), 0u);
}

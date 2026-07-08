#include <gtest/gtest.h>
#include "keystone/db.h"
#include "keystone/sstable.h"

#include <cstdio>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <thread>
#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_compact_XXXXXX";
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
                        int timeout_ms = 2000) {
    for (int elapsed = 0; elapsed < timeout_ms; elapsed += 5) {
        if (count_sst_files(dir) == expected) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return count_sst_files(dir) == expected;
}
}  // namespace

TEST(CompactionTest, TriggerAndReduction) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    std::vector<std::pair<std::string, std::string>> all;
    for (int batch = 0; batch < 7; batch++) {
        for (int i = 0; i < 50; i++) {
            int idx = batch * 50 + i;
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "key_%04d", idx);
            std::snprintf(val, sizeof(val), "val_%04d", idx);
            db->put(key, val);
            all.emplace_back(key, val);
        }
        db->flush();
    }

    db->compact();
    EXPECT_TRUE(wait_for_sst_count(dir.path, 1))
        << "Expected 1 SST file, got " << count_sst_files(dir.path);

    for (const auto& [key, val] : all) {
        auto result = db->get(key);
        ASSERT_TRUE(result.has_value()) << "Key missing: " << key;
        EXPECT_EQ(*result, val);
    }
}

TEST(CompactionTest, OverwriteReclamation) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    for (int round = 0; round < 7; round++) {
        for (int i = 0; i < 5; i++) {
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "key_%02d", i);
            std::snprintf(val, sizeof(val), "val_%02d_r%02d", i, round);
            db->put(key, val);
        }
        db->flush();
    }

    db->compact();

    for (int i = 0; i < 5; i++) {
        char key[32], expected[32];
        std::snprintf(key, sizeof(key), "key_%02d", i);
        std::snprintf(expected, sizeof(expected), "val_%02d_r%02d", i, 6);
        EXPECT_EQ(db->get(key), expected);
    }

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
    EXPECT_EQ(sst->entry_count(), 5u);
}

TEST(CompactionTest, TombstonePreserved) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    {
        auto db = keystone::DB::open(dir.path, opts);
        db->put("key_A", "value_A");
        db->flush();
        db->del("key_A");
        db->flush();
        db->put("other_1", "v1");
        db->flush();
        db->put("other_2", "v2");
        db->flush();

        EXPECT_EQ(db->get("key_A"), std::nullopt);
        EXPECT_EQ(db->get("other_1"), "v1");
        EXPECT_EQ(db->get("other_2"), "v2");
    }

    {
        auto db = keystone::DB::open(dir.path, opts);
        EXPECT_EQ(db->get("key_A"), std::nullopt);
        EXPECT_EQ(db->get("other_1"), "v1");
        EXPECT_EQ(db->get("other_2"), "v2");
    }
}

TEST(CompactionTest, CorrectnessOracle) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 512;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);
    std::map<std::string, std::optional<std::string>> oracle;

    std::mt19937 rng(42);
    for (int i = 0; i < 3000; i++) {
        int idx = static_cast<int>(rng() % 200);
        char key[32];
        std::snprintf(key, sizeof(key), "k_%04d", idx);

        if (rng() % 5 == 0) {
            db->del(key);
            oracle[key] = std::nullopt;
        } else {
            char val[32];
            std::snprintf(val, sizeof(val), "v_%d", i);
            db->put(key, val);
            oracle[key] = val;
        }
    }
    db->flush();

    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });

    std::vector<std::pair<std::string, std::string>> expected;
    for (const auto& [k, v] : oracle) {
        if (v.has_value())
            expected.emplace_back(k, *v);
    }

    EXPECT_EQ(scan_result, expected);
}

TEST(CompactionTest, ReopenAfterCompaction) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    std::vector<std::pair<std::string, std::string>> all;
    {
        auto db = keystone::DB::open(dir.path, opts);
        for (int batch = 0; batch < 7; batch++) {
            for (int i = 0; i < 20; i++) {
                int idx = batch * 20 + i;
                char key[32], val[32];
                std::snprintf(key, sizeof(key), "rk_%04d", idx);
                std::snprintf(val, sizeof(val), "rv_%04d", idx);
                db->put(key, val);
                all.emplace_back(key, val);
            }
            db->flush();
        }
        db->compact();
    }

    auto db = keystone::DB::open(dir.path, opts);

    for (const auto& [key, val] : all) {
        auto result = db->get(key);
        ASSERT_TRUE(result.has_value()) << "Key missing: " << key;
        EXPECT_EQ(*result, val);
    }

    EXPECT_TRUE(wait_for_sst_count(dir.path, 1, 10000))
        << "Expected 1 SST file, got " << count_sst_files(dir.path);
}

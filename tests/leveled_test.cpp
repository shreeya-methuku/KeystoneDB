#include <gtest/gtest.h>
#include "keystone/db.h"
#include "keystone/manifest.h"
#include "keystone/sstable.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_leveled_XXXXXX";
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

TEST(LeveledTest, L0CompactionReducesLevel) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    std::map<std::string, std::string> expected;
    for (int batch = 0; batch < 6; batch++) {
        for (int i = 0; i < 10; i++) {
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "key_%04d", batch * 10 + i);
            std::snprintf(val, sizeof(val), "val_%04d", batch * 10 + i);
            db->put(key, val);
            expected[key] = val;
        }
        db->flush();
    }

    db->compact();

    EXPECT_TRUE(wait_for_sst_count(dir.path, 1))
        << "Expected 1 SST file after compact, got " << count_sst_files(dir.path);

    for (const auto& [key, val] : expected) {
        auto result = db->get(key);
        ASSERT_TRUE(result.has_value()) << "Key missing after compact: " << key;
        EXPECT_EQ(*result, val);
    }

    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });
    EXPECT_EQ(scan_result.size(), expected.size());
}

TEST(LeveledTest, NonOverlappingL1) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;
    opts.target_file_size = 128;

    auto db = keystone::DB::open(dir.path, opts);

    for (int batch = 0; batch < 8; batch++) {
        for (int i = 0; i < 50; i++) {
            char key[32], val[64];
            std::snprintf(key, sizeof(key), "key_%04d", batch * 50 + i);
            std::snprintf(val, sizeof(val), "value_for_%04d_batch_%d",
                          batch * 50 + i, batch);
            db->put(key, val);
        }
        db->flush();
    }

    db->compact();

    int sst_count = count_sst_files(dir.path);
    EXPECT_GE(sst_count, 1);

    auto live = keystone::Manifest::load_latest(dir.path + "/MANIFEST");
    std::vector<keystone::ManifestEntry> l1_files;
    for (const auto& e : live) {
        if (e.level == 1) l1_files.push_back(e);
    }

    std::sort(l1_files.begin(), l1_files.end(),
              [](const keystone::ManifestEntry& a,
                 const keystone::ManifestEntry& b) {
                  return a.smallest_key < b.smallest_key;
              });

    for (size_t i = 1; i < l1_files.size(); i++) {
        EXPECT_GT(l1_files[i].smallest_key, l1_files[i - 1].largest_key)
            << "L1 files overlap at index " << i;
    }
}

TEST(LeveledTest, MultiLevelCompaction) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 256;
    opts.compaction_trigger = 4;
    opts.l1_max_bytes = 256;
    opts.level_size_multiplier = 2;

    auto db = keystone::DB::open(dir.path, opts);

    std::map<std::string, std::string> expected;
    for (int i = 0; i < 500; i++) {
        char key[32], val[64];
        std::snprintf(key, sizeof(key), "ml_%04d", i);
        std::snprintf(val, sizeof(val), "val_%04d", i);
        db->put(key, val);
        expected[key] = val;
    }

    db->flush();
    db->compact();

    for (const auto& [key, val] : expected) {
        auto result = db->get(key);
        ASSERT_TRUE(result.has_value()) << "Key missing: " << key;
        EXPECT_EQ(*result, val);
    }

    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });
    EXPECT_EQ(scan_result.size(), expected.size());
}

TEST(LeveledTest, TombstoneDroppedAtBottommost) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    db->put("key_A", "value_A");
    db->put("keep_1", "v1");
    db->flush();

    db->del("key_A");
    db->put("keep_2", "v2");
    db->flush();

    db->put("pad_1", "p1");
    db->flush();

    db->put("pad_2", "p2");
    db->flush();

    db->compact();

    EXPECT_EQ(db->get("key_A"), std::nullopt);
    EXPECT_TRUE(wait_for_sst_count(dir.path, 1));

    std::string sst_path;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") {
            sst_path = entry.path().string();
            break;
        }
    }
    ASSERT_FALSE(sst_path.empty());
    auto sst = keystone::SSTable::open(sst_path);

    bool found_key_A = false;
    for (auto it = sst->begin(); it != sst->end(); ++it) {
        if ((*it).key == "key_A") found_key_A = true;
    }
    EXPECT_FALSE(found_key_A)
        << "key_A tombstone should be dropped at bottommost level";
}

TEST(LeveledTest, KeyRangesInManifest) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    {
        auto db = keystone::DB::open(dir.path, opts);
        db->put("alpha", "1");
        db->put("beta", "2");
        db->put("gamma", "3");
        db->flush();
    }

    auto live = keystone::Manifest::load_latest(dir.path + "/MANIFEST");
    ASSERT_EQ(live.size(), 1u);
    EXPECT_EQ(live[0].level, 0);
    EXPECT_EQ(live[0].smallest_key, "alpha");
    EXPECT_EQ(live[0].largest_key, "gamma");

    auto db = keystone::DB::open(dir.path, opts);
    EXPECT_EQ(db->get("alpha"), "1");
    EXPECT_EQ(db->get("beta"), "2");
    EXPECT_EQ(db->get("gamma"), "3");
}

TEST(LeveledTest, OutputSplitting) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;
    opts.target_file_size = 512;

    auto db = keystone::DB::open(dir.path, opts);

    std::map<std::string, std::string> expected;
    for (int batch = 0; batch < 4; batch++) {
        for (int i = 0; i < 100; i++) {
            int idx = batch * 100 + i;
            char key[32], val[128];
            std::snprintf(key, sizeof(key), "key_%04d", idx);
            std::memset(val, 'A' + (idx % 26), 100);
            val[100] = '\0';
            db->put(key, val);
            expected[key] = val;
        }
        db->flush();
    }

    db->compact();

    int sst_count = count_sst_files(dir.path);
    EXPECT_GT(sst_count, 1) << "Expected multiple output files from splitting";

    for (const auto& [key, val] : expected) {
        auto result = db->get(key);
        ASSERT_TRUE(result.has_value()) << "Key missing: " << key;
        EXPECT_EQ(*result, val);
    }

    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });
    EXPECT_EQ(scan_result.size(), expected.size());
}

TEST(LeveledTest, CorrectnessOracle) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 512;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    std::map<std::string, std::optional<std::string>> oracle;
    std::mt19937 rng(777);

    for (int i = 0; i < 5000; i++) {
        int idx = static_cast<int>(rng() % 300);
        char key[32];
        std::snprintf(key, sizeof(key), "lk_%04d", idx);

        if (rng() % 5 == 0) {
            db->del(key);
            oracle[key] = std::nullopt;
        } else {
            char val[32];
            std::snprintf(val, sizeof(val), "lv_%d", i);
            db->put(key, val);
            oracle[key] = val;
        }
    }

    db->flush();
    db->compact();

    for (const auto& [key, val] : oracle) {
        auto result = db->get(key);
        if (val.has_value()) {
            ASSERT_TRUE(result.has_value()) << "Key missing: " << key;
            EXPECT_EQ(*result, *val);
        } else {
            EXPECT_FALSE(result.has_value()) << "Deleted key present: " << key;
        }
    }

    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });

    std::vector<std::pair<std::string, std::string>> expected_scan;
    for (const auto& [k, v] : oracle) {
        if (v.has_value())
            expected_scan.emplace_back(k, *v);
    }
    EXPECT_EQ(scan_result, expected_scan);
}

#include <gtest/gtest.h>
#include "keystone/db.h"

#include <cstdio>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_scan_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::filesystem::remove_all(path); }
};
}  // namespace

TEST(ScanTest, Basic) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);
    db->put("apple", "1");
    db->put("banana", "2");
    db->put("cherry", "3");
    db->put("date", "4");
    db->put("elderberry", "5");

    std::vector<std::pair<std::string, std::string>> results;
    db->scan("banana", "date",
             [&](std::string_view k, std::string_view v) {
                 results.emplace_back(k, v);
             });

    ASSERT_EQ(results.size(), 3u);
    EXPECT_EQ(results[0], std::make_pair(std::string("banana"), std::string("2")));
    EXPECT_EQ(results[1], std::make_pair(std::string("cherry"), std::string("3")));
    EXPECT_EQ(results[2], std::make_pair(std::string("date"), std::string("4")));
}

TEST(ScanTest, InclusiveBounds) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);
    db->put("a", "1");
    db->put("b", "2");
    db->put("c", "3");

    std::vector<std::string> keys;
    db->scan("a", "c", [&](std::string_view k, std::string_view) {
        keys.emplace_back(k);
    });

    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "a");
    EXPECT_EQ(keys[1], "b");
    EXPECT_EQ(keys[2], "c");
}

TEST(ScanTest, NewestWinsAcrossLayers) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);
    db->put("key", "old");
    db->flush();
    db->put("key", "new");

    std::vector<std::pair<std::string, std::string>> results;
    db->scan("key", "key", [&](std::string_view k, std::string_view v) {
        results.emplace_back(k, v);
    });

    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].first, "key");
    EXPECT_EQ(results[0].second, "new");
}

TEST(ScanTest, TombstoneHiding) {
    TempDir dir;

    {
        auto db = keystone::DB::open(dir.path);
        db->put("keep", "yes");
        db->put("kill", "doomed");
        db->flush();
        db->del("kill");

        std::vector<std::string> keys;
        db->scan("a", "z", [&](std::string_view k, std::string_view) {
            keys.emplace_back(k);
        });
        ASSERT_EQ(keys.size(), 1u);
        EXPECT_EQ(keys[0], "keep");

        db->flush();
    }

    {
        auto db = keystone::DB::open(dir.path);
        std::vector<std::string> keys;
        db->scan("a", "z", [&](std::string_view k, std::string_view) {
            keys.emplace_back(k);
        });
        ASSERT_EQ(keys.size(), 1u);
        EXPECT_EQ(keys[0], "keep");
    }
}

TEST(ScanTest, CrossLayerMerge) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4096;
    auto db = keystone::DB::open(dir.path, opts);

    std::set<std::string> expected;
    for (int i = 0; i < 500; i++) {
        char key[32], val[32];
        std::snprintf(key, sizeof(key), "key_%04d", i);
        std::snprintf(val, sizeof(val), "val_%04d", i);
        db->put(key, val);
        expected.insert(key);
    }

    std::vector<std::string> scanned;
    db->scan("key_0000", "key_9999",
             [&](std::string_view k, std::string_view) {
                 scanned.emplace_back(k);
             });

    ASSERT_EQ(scanned.size(), expected.size());
    for (size_t i = 1; i < scanned.size(); i++)
        EXPECT_LT(scanned[i - 1], scanned[i]);
    std::set<std::string> scanned_set(scanned.begin(), scanned.end());
    EXPECT_EQ(scanned_set, expected);
}

TEST(ScanTest, RandomizedOracle) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4096;
    auto db = keystone::DB::open(dir.path, opts);

    std::map<std::string, std::optional<std::string>> oracle;
    std::mt19937 rng(42);

    for (int i = 0; i < 3000; i++) {
        int key_num = static_cast<int>(rng() % 200);
        char key[32];
        std::snprintf(key, sizeof(key), "key_%04d", key_num);

        if (rng() % 4 == 0) {
            db->del(key);
            oracle[key] = std::nullopt;
        } else {
            char val[32];
            std::snprintf(val, sizeof(val), "val_%d", i);
            db->put(key, val);
            oracle[key] = val;
        }

        if (rng() % 100 == 0)
            db->flush();
    }

    std::vector<std::pair<std::string, std::string>> expected;
    for (const auto& [k, v] : oracle) {
        if (v.has_value())
            expected.emplace_back(k, *v);
    }

    std::vector<std::pair<std::string, std::string>> scanned;
    db->scan("key_0000", "key_9999",
             [&](std::string_view k, std::string_view v) {
                 scanned.emplace_back(k, v);
             });

    EXPECT_EQ(scanned, expected);
}

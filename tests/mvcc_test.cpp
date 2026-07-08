#include <gtest/gtest.h>
#include "keystone/db.h"

#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_mvcc_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::filesystem::remove_all(path); }
};
}  // namespace

TEST(MVCCTest, SnapshotReadAfterOverwrite) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);

    db->put("key", "v1");
    auto snap = db->get_snapshot();
    db->put("key", "v2");

    EXPECT_EQ(db->get("key", snap), "v1");
    EXPECT_EQ(db->get("key"), "v2");

    db->release_snapshot(snap);
}

TEST(MVCCTest, SnapshotReadAfterDelete) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);

    db->put("key", "v1");
    auto snap = db->get_snapshot();
    db->del("key");

    EXPECT_EQ(db->get("key", snap), "v1");
    EXPECT_EQ(db->get("key"), std::nullopt);

    db->release_snapshot(snap);
}

TEST(MVCCTest, ScanAtSnapshot) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);

    db->put("a", "1");
    db->put("b", "2");
    db->put("c", "3");
    auto snap = db->get_snapshot();

    db->put("a", "10");
    db->del("b");
    db->put("d", "4");

    std::vector<std::pair<std::string, std::string>> snap_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        snap_result.emplace_back(k, v);
    }, snap);

    ASSERT_EQ(snap_result.size(), 3u);
    EXPECT_EQ(snap_result[0], std::make_pair(std::string("a"), std::string("1")));
    EXPECT_EQ(snap_result[1], std::make_pair(std::string("b"), std::string("2")));
    EXPECT_EQ(snap_result[2], std::make_pair(std::string("c"), std::string("3")));

    std::vector<std::pair<std::string, std::string>> latest_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        latest_result.emplace_back(k, v);
    });

    ASSERT_EQ(latest_result.size(), 3u);
    EXPECT_EQ(latest_result[0], std::make_pair(std::string("a"), std::string("10")));
    EXPECT_EQ(latest_result[1], std::make_pair(std::string("c"), std::string("3")));
    EXPECT_EQ(latest_result[2], std::make_pair(std::string("d"), std::string("4")));

    db->release_snapshot(snap);
}

TEST(MVCCTest, CompactionRetainsSnapshotVersions) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);

    db->put("key", "v1");
    db->flush();
    auto snap = db->get_snapshot();
    db->put("key", "v2");
    db->flush();

    db->compact();

    EXPECT_EQ(db->get("key", snap), "v1");
    EXPECT_EQ(db->get("key"), "v2");

    db->release_snapshot(snap);
}

TEST(MVCCTest, CompactionDropsAfterRelease) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);

    db->put("key", "v1");
    db->flush();
    auto snap = db->get_snapshot();
    db->put("key", "v2");
    db->flush();

    db->release_snapshot(snap);
    db->compact();

    EXPECT_EQ(db->get("key"), "v2");

    int sst_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") sst_count++;
    }
    EXPECT_EQ(sst_count, 1);

    std::string sst_path;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") {
            sst_path = entry.path().string();
            break;
        }
    }
    auto sst = keystone::SSTable::open(sst_path);
    int entry_count = 0;
    for (auto it = sst->begin(); it != sst->end(); ++it)
        entry_count++;
    EXPECT_EQ(entry_count, 1);
}

TEST(MVCCTest, MultipleSnapshots) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);

    db->put("key", "v1");
    auto snap1 = db->get_snapshot();

    db->put("key", "v2");
    auto snap2 = db->get_snapshot();

    db->put("key", "v3");

    EXPECT_EQ(db->get("key", snap1), "v1");
    EXPECT_EQ(db->get("key", snap2), "v2");
    EXPECT_EQ(db->get("key"), "v3");

    db->release_snapshot(snap1);
    db->release_snapshot(snap2);
}

TEST(MVCCTest, NoSnapshotBehaviorUnchanged) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);

    db->put("a", "1");
    db->put("b", "2");
    db->put("a", "10");
    db->del("b");

    EXPECT_EQ(db->get("a"), "10");
    EXPECT_EQ(db->get("b"), std::nullopt);

    db->flush();

    EXPECT_EQ(db->get("a"), "10");
    EXPECT_EQ(db->get("b"), std::nullopt);

    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });
    ASSERT_EQ(scan_result.size(), 1u);
    EXPECT_EQ(scan_result[0].first, "a");
    EXPECT_EQ(scan_result[0].second, "10");
}

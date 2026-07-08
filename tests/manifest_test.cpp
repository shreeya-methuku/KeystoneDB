#include <gtest/gtest.h>
#include "keystone/db.h"
#include "keystone/manifest.h"
#include "keystone/sstable.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
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
        char tmpl[] = "/tmp/ks_manifest_XXXXXX";
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

// ── Manifest unit tests ─────────────────────────────────────────────────────

TEST(ManifestTest, RoundTrip) {
    TempDir dir;
    std::string path = dir.path + "/MANIFEST";

    {
        auto m = keystone::Manifest::open(path);
        m->append_snapshot({{1,0,"a","c"}, {2,0,"d","f"}, {3,1,"a","f"}});
        m->append_snapshot({{4,1,"a","d"}, {5,1,"e","h"}});
    }

    auto live = keystone::Manifest::load_latest(path);
    ASSERT_EQ(live.size(), 2u);
    EXPECT_EQ(live[0].number, 4);
    EXPECT_EQ(live[0].level, 1);
    EXPECT_EQ(live[0].smallest_key, "a");
    EXPECT_EQ(live[0].largest_key, "d");
    EXPECT_EQ(live[1].number, 5);
    EXPECT_EQ(live[1].level, 1);
}

TEST(ManifestTest, TornTail) {
    TempDir dir;
    std::string path = dir.path + "/MANIFEST";

    {
        auto m = keystone::Manifest::open(path);
        m->append_snapshot({{10,0,"a","b"}, {20,1,"c","d"}, {30,1,"e","f"}});
    }

    {
        std::ofstream ofs(path, std::ios::binary | std::ios::app);
        char garbage[] = {0x01, 0x02, 0x03};
        ofs.write(garbage, sizeof(garbage));
    }

    auto live = keystone::Manifest::load_latest(path);
    ASSERT_EQ(live.size(), 3u);
    EXPECT_EQ(live[0].number, 10);
    EXPECT_EQ(live[1].number, 20);
    EXPECT_EQ(live[2].number, 30);
}

TEST(ManifestTest, EmptyFile) {
    TempDir dir;
    std::string path = dir.path + "/MANIFEST";

    {
        auto m = keystone::Manifest::open(path);
    }

    auto live = keystone::Manifest::load_latest(path);
    EXPECT_TRUE(live.empty());
}

TEST(ManifestTest, MultipleSnapshots) {
    TempDir dir;
    std::string path = dir.path + "/MANIFEST";

    {
        auto m = keystone::Manifest::open(path);
        m->append_snapshot({{1,0,"a","z"}});
        m->append_snapshot({{1,0,"a","m"}, {2,0,"n","z"}});
        m->append_snapshot({{1,0,"a","m"}, {2,0,"n","z"}, {3,1,"a","z"}});
        m->append_snapshot({{5,1,"a","z"}});
    }

    auto live = keystone::Manifest::load_latest(path);
    ASSERT_EQ(live.size(), 1u);
    EXPECT_EQ(live[0].number, 5);
}

// ── Tombstone dropping ──────────────────────────────────────────────────────

TEST(ManifestDBTest, TombstoneDropped) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);
    db->put("key_A", "value_A");
    db->flush();
    db->del("key_A");
    db->flush();
    db->put("other_1", "v1");
    db->flush();
    db->put("other_2", "v2");
    db->flush();  // triggers compaction
    db->compact();

    EXPECT_EQ(db->get("key_A"), std::nullopt);

    EXPECT_TRUE(wait_for_sst_count(dir.path, 1))
        << "Expected 1 SST file, got " << count_sst_files(dir.path);

    // Verify the merged SSTable contains NO entry for key_A
    std::string sst_file;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") {
            sst_file = entry.path().string();
            break;
        }
    }
    ASSERT_FALSE(sst_file.empty());
    auto sst = keystone::SSTable::open(sst_file);

    bool found_key_A = false;
    for (auto it = sst->begin(); it != sst->end(); ++it) {
        if ((*it).key == "key_A") {
            found_key_A = true;
            break;
        }
    }
    EXPECT_FALSE(found_key_A) << "key_A should not exist in merged SSTable";

    // Survives reopen
    db.reset();
    auto db2 = keystone::DB::open(dir.path, opts);
    EXPECT_EQ(db2->get("key_A"), std::nullopt);
    EXPECT_EQ(db2->get("other_1"), "v1");
    EXPECT_EQ(db2->get("other_2"), "v2");
}

// ── Orphan cleanup ──────────────────────────────────────────────────────────

TEST(ManifestDBTest, OrphanCleanup) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    {
        auto db = keystone::DB::open(dir.path, opts);
        db->put("real_key", "real_value");
        db->flush();
    }

    // Create a stray SSTable not referenced by the manifest
    std::string orphan_path = dir.path + "/999999.sst";
    {
        keystone::SSTableWriter writer(orphan_path);
        writer.add("orphan_key", "orphan_value", false, 1);
        writer.finish();
        keystone::SSTableWriter::install(writer.temp_path(), orphan_path,
                                         dir.path);
    }
    ASSERT_TRUE(std::filesystem::exists(orphan_path));

    // Reopen: orphan should be deleted
    {
        auto db = keystone::DB::open(dir.path, opts);
        EXPECT_FALSE(std::filesystem::exists(orphan_path));
        EXPECT_EQ(db->get("real_key"), "real_value");
        EXPECT_EQ(db->get("orphan_key"), std::nullopt);
    }
}

// ── Manifest-driven recency ─────────────────────────────────────────────────

TEST(ManifestDBTest, SeqnoRecencyOverridesManifestOrder) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    // Create two SSTables with different values for the same key
    {
        auto db = keystone::DB::open(dir.path, opts);
        db->put("key", "first");
        db->flush();   // 000001.sst (lower seqno)
        db->put("key", "second");
        db->flush();   // 000002.sst (higher seqno)
    }

    // Rewrite MANIFEST with reversed order: [2, 1]
    // Both at L0 — seqno should determine recency, not manifest order
    std::filesystem::remove(dir.path + "/MANIFEST");
    {
        auto m = keystone::Manifest::open(dir.path + "/MANIFEST");
        m->append_snapshot({{2, 0, "key", "key"}, {1, 0, "key", "key"}});
    }

    // Reopen — seqno-based recency: SSTable 2 has higher seqno → "second" wins
    auto db = keystone::DB::open(dir.path, opts);
    EXPECT_EQ(db->get("key"), "second");
}

// ── Correctness oracle with tombstone dropping ──────────────────────────────

TEST(ManifestDBTest, CorrectnessOracleWithReopen) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 512;
    opts.compaction_trigger = 4;

    std::map<std::string, std::optional<std::string>> oracle;

    {
        auto db = keystone::DB::open(dir.path, opts);
        std::mt19937 rng(99);
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
    }

    // Reopen
    auto db = keystone::DB::open(dir.path, opts);

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

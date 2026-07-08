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
}  // namespace

// ── Manifest unit tests ─────────────────────────────────────────────────────

TEST(ManifestTest, RoundTrip) {
    TempDir dir;
    std::string path = dir.path + "/MANIFEST";

    {
        auto m = keystone::Manifest::open(path);
        m->append_snapshot({1, 2, 3});
        m->append_snapshot({4, 5});
    }

    auto live = keystone::Manifest::load_latest(path);
    ASSERT_EQ(live.size(), 2u);
    EXPECT_EQ(live[0], 4);
    EXPECT_EQ(live[1], 5);
}

TEST(ManifestTest, TornTail) {
    TempDir dir;
    std::string path = dir.path + "/MANIFEST";

    {
        auto m = keystone::Manifest::open(path);
        m->append_snapshot({10, 20, 30});
    }

    {
        std::ofstream ofs(path, std::ios::binary | std::ios::app);
        char garbage[] = {0x01, 0x02, 0x03};
        ofs.write(garbage, sizeof(garbage));
    }

    auto live = keystone::Manifest::load_latest(path);
    ASSERT_EQ(live.size(), 3u);
    EXPECT_EQ(live[0], 10);
    EXPECT_EQ(live[1], 20);
    EXPECT_EQ(live[2], 30);
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
        m->append_snapshot({1});
        m->append_snapshot({1, 2});
        m->append_snapshot({1, 2, 3});
        m->append_snapshot({5});
    }

    auto live = keystone::Manifest::load_latest(path);
    ASSERT_EQ(live.size(), 1u);
    EXPECT_EQ(live[0], 5);
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
        writer.add("orphan_key", "orphan_value", false);
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

TEST(ManifestDBTest, ManifestDrivenRecency) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    // Create two SSTables with different values for the same key
    {
        auto db = keystone::DB::open(dir.path, opts);
        db->put("key", "first");
        db->flush();   // 000001.sst
        db->put("key", "second");
        db->flush();   // 000002.sst
    }

    // Rewrite MANIFEST with reversed order: [2, 1]
    // In MANIFEST order, SSTable 1 is at the BACK (newest)
    std::filesystem::remove(dir.path + "/MANIFEST");
    {
        auto m = keystone::Manifest::open(dir.path + "/MANIFEST");
        m->append_snapshot({2, 1});
    }

    // Reopen
    auto db = keystone::DB::open(dir.path, opts);
    // MANIFEST says [2, 1]: back=1 is newest → key="first"
    // File-number ordering would say 2>1 → key="second"
    EXPECT_EQ(db->get("key"), "first");
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

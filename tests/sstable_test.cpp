#include <gtest/gtest.h>
#include "keystone/db.h"
#include "keystone/sstable.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_sst_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::filesystem::remove_all(path); }
};
}  // namespace

// ── SSTable unit tests ───────────────────────────────────────────────────────

TEST(SSTableTest, RoundTrip) {
    TempDir dir;
    std::string path = dir.path + "/test.sst";

    struct E { std::string k, v; bool t; };
    std::vector<E> entries = {
        {"alpha",   "val_alpha", false},
        {"bravo",   "val_bravo", false},
        {"charlie", "",          true },
        {"delta",   "val_delta", false},
        {"echo",    "",          true },
    };

    {
        keystone::SSTableWriter writer(path);
        for (const auto& e : entries)
            writer.add(e.k, e.v, e.t);
        writer.finish();
        keystone::SSTableWriter::install(writer.temp_path(), path, dir.path);
    }

    auto sst = keystone::SSTable::open(path);
    EXPECT_EQ(sst->entry_count(), 5u);

    size_t i = 0;
    for (auto it = sst->begin(); it != sst->end(); ++it, ++i) {
        ASSERT_LT(i, entries.size());
        EXPECT_EQ((*it).key,       entries[i].k);
        EXPECT_EQ((*it).value,     entries[i].v);
        EXPECT_EQ((*it).tombstone, entries[i].t);
    }
    EXPECT_EQ(i, entries.size());
}

TEST(SSTableTest, MultiBlock) {
    TempDir dir;
    std::string path = dir.path + "/test.sst";

    {
        keystone::SSTableWriter writer(path);
        for (int n = 0; n < 500; n++) {
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "key_%04d", n);
            std::snprintf(val, sizeof(val), "value_%04d", n);
            writer.add(key, val, false);
        }
        writer.finish();
        keystone::SSTableWriter::install(writer.temp_path(), path, dir.path);
    }

    auto sst = keystone::SSTable::open(path);
    EXPECT_GT(sst->block_count(), 1u);
    EXPECT_EQ(sst->entry_count(), 500u);

    auto r0 = sst->get("key_0000");
    EXPECT_EQ(r0.status, keystone::SSTable::LookupStatus::Found);
    EXPECT_EQ(r0.value,  "value_0000");

    auto r250 = sst->get("key_0250");
    EXPECT_EQ(r250.status, keystone::SSTable::LookupStatus::Found);
    EXPECT_EQ(r250.value,  "value_0250");

    auto r499 = sst->get("key_0499");
    EXPECT_EQ(r499.status, keystone::SSTable::LookupStatus::Found);
    EXPECT_EQ(r499.value,  "value_0499");

    auto rn = sst->get("key_9999");
    EXPECT_EQ(rn.status, keystone::SSTable::LookupStatus::NotFound);
}

TEST(SSTableTest, BadMagic) {
    TempDir dir;
    std::string path = dir.path + "/test.sst";

    {
        keystone::SSTableWriter writer(path);
        writer.add("key", "value", false);
        writer.finish();
        keystone::SSTableWriter::install(writer.temp_path(), path, dir.path);
    }

    std::string content;
    {
        std::ifstream ifs(path, std::ios::binary);
        content.assign(std::istreambuf_iterator<char>(ifs), {});
    }
    for (size_t i = content.size() - 4; i < content.size(); i++)
        content[i] = ~content[i];
    {
        std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
        ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
    }

    EXPECT_THROW(keystone::SSTable::open(path), std::runtime_error);
}

// ── DB + flush integration tests ─────────────────────────────────────────────

TEST(SSTableDBTest, FlushProducesSSTFiles) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 4096;
    opts.compaction_trigger = 1000;
    auto db = keystone::DB::open(dir.path, opts);

    for (int i = 0; i < 500; i++) {
        char key[32], val[256];
        std::snprintf(key, sizeof(key), "key_%04d", i);
        std::memset(val, 'x', 200);
        val[200] = '\0';
        db->put(key, val);
    }

    int sst_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() == ".sst") sst_count++;
    }
    EXPECT_GE(sst_count, 2);

    auto wal_size =
        std::filesystem::file_size(dir.path + std::string("/wal.log"));
    EXPECT_LT(wal_size,
              static_cast<std::uintmax_t>(opts.flush_threshold_bytes));
}

TEST(SSTableDBTest, ReadThroughAfterFlush) {
    TempDir dir;
    auto db = keystone::DB::open(dir.path);

    db->put("alpha",   "1");
    db->put("bravo",   "2");
    db->put("charlie", "3");
    db->flush();

    EXPECT_EQ(db->get("alpha"),   "1");
    EXPECT_EQ(db->get("bravo"),   "2");
    EXPECT_EQ(db->get("charlie"), "3");
    EXPECT_EQ(db->get("missing"), std::nullopt);
}

TEST(SSTableDBTest, ReopenAfterFlush) {
    TempDir dir;

    {
        auto db = keystone::DB::open(dir.path);
        db->put("alpha", "1");
        db->put("bravo", "2");
        db->flush();
    }

    {
        auto db = keystone::DB::open(dir.path);
        EXPECT_EQ(db->get("alpha"),   "1");
        EXPECT_EQ(db->get("bravo"),   "2");
        EXPECT_EQ(db->get("missing"), std::nullopt);
    }
}

TEST(SSTableDBTest, TombstoneAfterFlush) {
    TempDir dir;

    {
        auto db = keystone::DB::open(dir.path);
        db->put("key", "value");
        db->flush();

        db->del("key");
        EXPECT_EQ(db->get("key"), std::nullopt);

        db->flush();
    }

    {
        auto db = keystone::DB::open(dir.path);
        EXPECT_EQ(db->get("key"), std::nullopt);
    }
}

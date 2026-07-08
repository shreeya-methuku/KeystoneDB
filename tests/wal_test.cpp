#include <gtest/gtest.h>
#include "keystone/crc32.h"
#include "keystone/db.h"
#include "keystone/wal.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_wal_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::filesystem::remove_all(path); }
};
}  // namespace

// ── CRC32 ────────────────────────────────────────────────────────────────────

TEST(CRC32Test, KnownAnswerVector) {
    EXPECT_EQ(keystone::crc32("123456789", 9), 0xCBF43926u);
}

TEST(CRC32Test, EmptyInput) {
    EXPECT_EQ(keystone::crc32("", 0), 0x00000000u);
}

// ── WAL ──────────────────────────────────────────────────────────────────────

TEST(WALTest, AppendAndReplay) {
    TempDir dir;
    std::string wal_path = dir.path + "/test.wal";

    {
        auto wal = keystone::WAL::open(wal_path, keystone::SyncMode::EveryWrite);
        wal->append(keystone::RecType::Put, 1, "key1", "val1");
        wal->append(keystone::RecType::Put, 2, "key2", "val2");
        wal->append(keystone::RecType::Delete, 3, "key1", "");
    }

    auto records = keystone::WAL::replay(wal_path);
    ASSERT_EQ(records.size(), 3u);

    EXPECT_EQ(records[0].type, keystone::RecType::Put);
    EXPECT_EQ(records[0].seq, 1u);
    EXPECT_EQ(records[0].key, "key1");
    EXPECT_EQ(records[0].value, "val1");

    EXPECT_EQ(records[1].type, keystone::RecType::Put);
    EXPECT_EQ(records[1].seq, 2u);
    EXPECT_EQ(records[1].key, "key2");
    EXPECT_EQ(records[1].value, "val2");

    EXPECT_EQ(records[2].type, keystone::RecType::Delete);
    EXPECT_EQ(records[2].seq, 3u);
    EXPECT_EQ(records[2].key, "key1");
    EXPECT_EQ(records[2].value, "");
}

TEST(WALTest, PutThenDelete) {
    TempDir dir;

    {
        auto db = keystone::DB::open(dir.path);
        db->put("x", "hello");
        db->del("x");
    }

    auto records = keystone::WAL::replay(dir.path + "/wal.log");
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].type, keystone::RecType::Put);
    EXPECT_EQ(records[0].key, "x");
    EXPECT_EQ(records[0].value, "hello");
    EXPECT_EQ(records[1].type, keystone::RecType::Delete);
    EXPECT_EQ(records[1].key, "x");

    auto db = keystone::DB::open(dir.path);
    EXPECT_EQ(db->get("x"), std::nullopt);
}

TEST(WALTest, TornTail) {
    TempDir dir;
    std::string wal_path = dir.path + "/test.wal";

    {
        auto wal = keystone::WAL::open(wal_path, keystone::SyncMode::EveryWrite);
        wal->append(keystone::RecType::Put, 1, "key1", "val1");
        wal->append(keystone::RecType::Put, 2, "key2", "val2");
    }

    {
        std::ofstream ofs(wal_path, std::ios::binary | std::ios::app);
        ofs.write("\xDE\xAD\xBE", 3);
    }

    auto records = keystone::WAL::replay(wal_path);
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].key, "key1");
    EXPECT_EQ(records[1].key, "key2");
}

TEST(WALTest, CRCCorruption) {
    TempDir dir;
    std::string wal_path = dir.path + "/test.wal";

    {
        auto wal = keystone::WAL::open(wal_path, keystone::SyncMode::EveryWrite);
        wal->append(keystone::RecType::Put, 1, "key1", "val1");
        wal->append(keystone::RecType::Put, 2, "key2", "val2");
        wal->append(keystone::RecType::Put, 3, "key3", "val3");
    }

    // Record 1 is 21 + 4 + 4 = 29 bytes.  Record 2 body starts at byte 33.
    std::string content;
    {
        std::ifstream ifs(wal_path, std::ios::binary);
        content.assign(std::istreambuf_iterator<char>(ifs), {});
    }
    content[33] = ~content[33];
    {
        std::ofstream ofs(wal_path, std::ios::binary | std::ios::trunc);
        ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
    }

    auto records = keystone::WAL::replay(wal_path);
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].key, "key1");
}

TEST(WALTest, ReopenPersistence) {
    TempDir dir;

    {
        auto db = keystone::DB::open(dir.path);
        db->put("alpha", "1");
        db->put("bravo", "2");
        db->put("charlie", "3");
    }

    {
        auto db = keystone::DB::open(dir.path);
        EXPECT_EQ(db->get("alpha"), "1");
        EXPECT_EQ(db->get("bravo"), "2");
        EXPECT_EQ(db->get("charlie"), "3");
        EXPECT_EQ(db->get("delta"), std::nullopt);
    }
}

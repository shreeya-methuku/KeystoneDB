#include <gtest/gtest.h>
#include "keystone/bloom.h"
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
        char tmpl[] = "/tmp/ks_bloom_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::filesystem::remove_all(path); }
};
}  // namespace

TEST(BloomTest, NoFalseNegatives) {
    keystone::Bloom bloom(10000);
    for (int i = 0; i < 10000; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "key_%06d", i);
        bloom.add(key);
    }
    for (int i = 0; i < 10000; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "key_%06d", i);
        EXPECT_TRUE(bloom.may_contain(key)) << "False negative for " << key;
    }
}

TEST(BloomTest, FalsePositiveRate) {
    keystone::Bloom bloom(10000);
    for (int i = 0; i < 10000; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "key_%06d", i);
        bloom.add(key);
    }
    int fp = 0;
    for (int i = 100000; i < 200000; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "key_%06d", i);
        if (bloom.may_contain(key)) fp++;
    }
    double fp_rate = static_cast<double>(fp) / 100000.0;
    std::printf("Bloom FP rate: %.4f%% (theory ~0.82%%)\n", fp_rate * 100);
    EXPECT_LT(fp_rate, 0.015);
}

TEST(BloomTest, SerializeRoundTrip) {
    keystone::Bloom bloom(1000);
    for (int i = 0; i < 1000; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "rt_%05d", i);
        bloom.add(key);
    }

    auto data = bloom.serialize();
    auto loaded = keystone::Bloom::load(data.data(), data.size());

    for (int i = 0; i < 1000; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "rt_%05d", i);
        EXPECT_EQ(bloom.may_contain(key), loaded.may_contain(key));
    }
    for (int i = 10000; i < 11000; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "rt_%05d", i);
        EXPECT_EQ(bloom.may_contain(key), loaded.may_contain(key));
    }
}

TEST(BloomTest, SSTableIntegration) {
    TempDir dir;
    std::string path = dir.path + "/test.sst";

    std::vector<std::string> keys;
    for (int i = 0; i < 200; i++) {
        char key[32];
        std::snprintf(key, sizeof(key), "sst_%04d", i);
        keys.emplace_back(key);
    }

    {
        keystone::SSTableWriter writer(path);
        for (size_t i = 0; i < keys.size(); i++) {
            char val[32];
            std::snprintf(val, sizeof(val), "val_%04d", static_cast<int>(i));
            bool tombstone = (i % 50 == 0 && i > 0);
            writer.add(keys[i], tombstone ? "" : val, tombstone,
                        static_cast<uint64_t>(i + 1));
        }
        writer.finish();
        keystone::SSTableWriter::install(writer.temp_path(), path, dir.path);
    }

    auto sst = keystone::SSTable::open(path);

    for (const auto& k : keys)
        EXPECT_TRUE(sst->may_contain(k)) << "False negative for " << k;

    for (size_t i = 0; i < keys.size(); i++) {
        auto result = sst->get(keys[i]);
        if (i % 50 == 0 && i > 0) {
            EXPECT_EQ(result.status, keystone::SSTable::LookupStatus::Deleted);
        } else {
            EXPECT_EQ(result.status, keystone::SSTable::LookupStatus::Found);
            char val[32];
            std::snprintf(val, sizeof(val), "val_%04d", static_cast<int>(i));
            EXPECT_EQ(result.value, val);
        }
    }

    auto r = sst->get("totally_absent");
    EXPECT_EQ(r.status, keystone::SSTable::LookupStatus::NotFound);
}

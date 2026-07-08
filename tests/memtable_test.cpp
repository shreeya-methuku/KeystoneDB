#include <gtest/gtest.h>
#include "keystone/memtable.h"

#include <algorithm>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

using keystone::Memtable;

TEST(MemtableTest, Ordering) {
    Memtable mt;
    mt.put("delta", "4", 1);
    mt.put("alpha", "1", 2);
    mt.put("charlie", "3", 3);
    mt.put("bravo", "2", 4);
    mt.put("echo", "5", 5);

    std::vector<std::string> keys;
    for (auto it = mt.begin(); it != mt.end(); ++it) {
        keys.emplace_back((*it).key);
    }
    ASSERT_EQ(keys.size(), 5u);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));
    EXPECT_EQ(keys[0], "alpha");
    EXPECT_EQ(keys[4], "echo");
}

TEST(MemtableTest, Overwrite) {
    Memtable mt;
    mt.put("key", "v1", 1);
    mt.put("key", "v2", 2);

    std::string value;
    EXPECT_EQ(mt.lookup("key", &value), Memtable::Status::Found);
    EXPECT_EQ(value, "v2");
}

TEST(MemtableTest, TombstoneShadowing) {
    Memtable mt;
    std::string value;

    EXPECT_EQ(mt.lookup("ghost", &value), Memtable::Status::NotFound);

    mt.put("key", "value", 1);
    mt.del("key", 2);
    EXPECT_EQ(mt.lookup("key", &value), Memtable::Status::Deleted);

    mt.del("phantom", 3);
    EXPECT_EQ(mt.lookup("phantom", &value), Memtable::Status::Deleted);
}

TEST(MemtableTest, IteratorIncludesTombstones) {
    Memtable mt;
    mt.put("a", "1", 1);
    mt.put("b", "2", 2);
    mt.put("c", "3", 3);
    mt.del("b", 4);

    std::vector<Memtable::Entry> entries;
    for (auto it = mt.begin(); it != mt.end(); ++it)
        entries.push_back(*it);

    ASSERT_EQ(entries.size(), 3u);
    EXPECT_EQ(entries[0].key, "a");
    EXPECT_FALSE(entries[0].tombstone);
    EXPECT_EQ(entries[1].key, "b");
    EXPECT_TRUE(entries[1].tombstone);
    EXPECT_EQ(entries[2].key, "c");
    EXPECT_FALSE(entries[2].tombstone);
}

TEST(MemtableTest, ApproxBytesGrows) {
    Memtable mt;
    EXPECT_EQ(mt.approx_bytes(), 0u);

    mt.put("key1", "value1", 1);
    size_t after_first = mt.approx_bytes();
    EXPECT_GT(after_first, 0u);

    mt.put("key2", "value2", 2);
    EXPECT_GT(mt.approx_bytes(), after_first);
}

TEST(MemtableTest, RandomizedOracle) {
    Memtable mt;
    std::map<std::string, std::optional<std::string>> oracle;

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> op_dist(0, 2);
    std::uniform_int_distribution<int> key_dist(0, 99);
    std::uniform_int_distribution<int> val_dist(0, 999);
    uint64_t seq = 1;

    for (int i = 0; i < 5000; i++) {
        int op = op_dist(rng);
        std::string key = "key" + std::to_string(key_dist(rng));

        if (op == 0) {
            std::string val = "val" + std::to_string(val_dist(rng));
            mt.put(key, val, seq++);
            oracle[key] = val;
        } else if (op == 1) {
            mt.del(key, seq++);
            oracle[key] = std::nullopt;
        } else {
            std::string val;
            auto status = mt.lookup(key, &val);
            auto it = oracle.find(key);
            if (it == oracle.end()) {
                ASSERT_EQ(status, Memtable::Status::NotFound)
                    << "key=" << key << " i=" << i;
            } else if (!it->second.has_value()) {
                ASSERT_EQ(status, Memtable::Status::Deleted)
                    << "key=" << key << " i=" << i;
            } else {
                ASSERT_EQ(status, Memtable::Status::Found)
                    << "key=" << key << " i=" << i;
                ASSERT_EQ(val, it->second.value())
                    << "key=" << key << " i=" << i;
            }
        }
    }
}

#include <gtest/gtest.h>
#include "keystone/db.h"

TEST(DBTest, PutGetReturnsValue) {
    auto db = keystone::DB::open("/tmp/keystone_test");
    db->put("hello", "world");
    EXPECT_EQ(db->get("hello"), "world");
}

TEST(DBTest, GetMissingKeyReturnsNullopt) {
    auto db = keystone::DB::open("/tmp/keystone_test");
    EXPECT_EQ(db->get("nonexistent"), std::nullopt);
}

TEST(DBTest, OverwriteReturnsNewestValue) {
    auto db = keystone::DB::open("/tmp/keystone_test");
    db->put("key", "v1");
    db->put("key", "v2");
    EXPECT_EQ(db->get("key"), "v2");
}

TEST(DBTest, DelMakesGetReturnNullopt) {
    auto db = keystone::DB::open("/tmp/keystone_test");
    db->put("key", "value");
    db->del("key");
    EXPECT_EQ(db->get("key"), std::nullopt);
}

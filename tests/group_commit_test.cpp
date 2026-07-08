#include <gtest/gtest.h>
#include "keystone/db.h"

#include <cstdio>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_gc_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() { std::filesystem::remove_all(path); }
};
}  // namespace

TEST(GroupCommitTest, MultiWriterCorrectness) {
    TempDir dir;
    keystone::Options opts;
    opts.sync = keystone::SyncMode::Batched;
    opts.flush_threshold_bytes = 512;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    constexpr int kThreads = 4;
    constexpr int kOpsPerThread = 2000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kOpsPerThread; i++) {
                char key[32], val[32];
                std::snprintf(key, sizeof(key), "t%d_%06d", t, i);
                std::snprintf(val, sizeof(val), "v%d_%06d", t, i);
                db->put(key, val);
            }
        });
    }
    for (auto& t : threads) t.join();

    db->compact();

    // Build oracle
    std::map<std::string, std::string> oracle;
    for (int t = 0; t < kThreads; t++) {
        for (int i = 0; i < kOpsPerThread; i++) {
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "t%d_%06d", t, i);
            std::snprintf(val, sizeof(val), "v%d_%06d", t, i);
            oracle[key] = val;
        }
    }

    // Verify via scan
    std::vector<std::pair<std::string, std::string>> scan_result;
    db->scan("", "~", [&](std::string_view k, std::string_view v) {
        scan_result.emplace_back(k, v);
    });

    std::vector<std::pair<std::string, std::string>> expected(
        oracle.begin(), oracle.end());
    EXPECT_EQ(scan_result, expected);
}

TEST(GroupCommitTest, DurabilityUnderConcurrency) {
    TempDir dir;
    keystone::Options opts;
    opts.sync = keystone::SyncMode::EveryWrite;
    opts.flush_threshold_bytes = 64 * 1024;
    opts.compaction_trigger = 1000;

    constexpr int kThreads = 4;
    constexpr int kOpsPerThread = 200;

    // Build oracle
    std::map<std::string, std::string> oracle;
    for (int t = 0; t < kThreads; t++) {
        for (int i = 0; i < kOpsPerThread; i++) {
            char key[32], val[32];
            std::snprintf(key, sizeof(key), "d%d_%06d", t, i);
            std::snprintf(val, sizeof(val), "w%d_%06d", t, i);
            oracle[key] = val;
        }
    }

    {
        auto db = keystone::DB::open(dir.path, opts);

        std::vector<std::thread> threads;
        for (int t = 0; t < kThreads; t++) {
            threads.emplace_back([&, t] {
                for (int i = 0; i < kOpsPerThread; i++) {
                    char key[32], val[32];
                    std::snprintf(key, sizeof(key), "d%d_%06d", t, i);
                    std::snprintf(val, sizeof(val), "w%d_%06d", t, i);
                    db->put(key, val);
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Reopen and verify every acknowledged write is present
    auto db = keystone::DB::open(dir.path, opts);

    for (const auto& [key, val] : oracle) {
        auto result = db->get(key);
        ASSERT_TRUE(result.has_value()) << "Key missing after reopen: " << key;
        EXPECT_EQ(*result, val);
    }
}

TEST(GroupCommitTest, BatchCoalescingEvidence) {
    TempDir dir;
    keystone::Options opts;
    opts.sync = keystone::SyncMode::EveryWrite;
    opts.flush_threshold_bytes = 64 * 1024 * 1024;
    opts.compaction_trigger = 1000;

    auto db = keystone::DB::open(dir.path, opts);

    constexpr int kThreads = 4;
    constexpr int kOpsPerThread = 500;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kOpsPerThread; i++) {
                char key[32], val[32];
                std::snprintf(key, sizeof(key), "b%d_%06d", t, i);
                std::snprintf(val, sizeof(val), "x%d_%06d", t, i);
                db->put(key, val);
            }
        });
    }
    for (auto& t : threads) t.join();

    uint64_t fsyncs = db->wal_fsync_count();
    uint64_t total_ops = kThreads * kOpsPerThread;

    EXPECT_LT(fsyncs, total_ops / 2)
        << "Expected batching to reduce fsyncs below " << total_ops / 2
        << " but got " << fsyncs << " fsyncs for " << total_ops << " ops";
}

#include <gtest/gtest.h>
#include "keystone/db.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ks_conc_XXXXXX";
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
}  // namespace

TEST(ConcurrencyTest, StressTest) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 512;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    constexpr int kNumKeys = 100;
    constexpr int kWriterOps = 5000;
    constexpr int kNumReaders = 4;

    std::atomic<bool> writer_done{false};
    std::atomic<int> reader_errors{0};

    // Oracle tracks the final state per key (writer-only, read after join)
    std::map<std::string, std::optional<std::string>> oracle;

    // Writer thread
    std::thread writer([&] {
        std::mt19937 rng(42);
        for (int i = 0; i < kWriterOps; i++) {
            int idx = static_cast<int>(rng() % kNumKeys);
            char key[32];
            std::snprintf(key, sizeof(key), "k_%04d", idx);

            if (rng() % 5 == 0) {
                db->del(key);
                oracle[key] = std::nullopt;
            } else {
                char val[64];
                std::snprintf(val, sizeof(val), "k_%04d_v_%d", idx, i);
                db->put(key, val);
                oracle[key] = val;
            }
        }
        db->flush();
        writer_done.store(true, std::memory_order_release);
    });

    // Reader threads: call get() on random keys, check value is not garbled
    std::vector<std::thread> readers;
    for (int r = 0; r < kNumReaders; r++) {
        readers.emplace_back([&, r] {
            std::mt19937 rng(1000 + r);
            while (!writer_done.load(std::memory_order_acquire)) {
                int idx = static_cast<int>(rng() % kNumKeys);
                char key[32];
                std::snprintf(key, sizeof(key), "k_%04d", idx);
                char prefix[16];
                std::snprintf(prefix, sizeof(prefix), "k_%04d_v_", idx);

                auto result = db->get(key);
                if (result.has_value()) {
                    if (result->substr(0, std::strlen(prefix)) != prefix) {
                        reader_errors.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    writer.join();
    for (auto& t : readers) t.join();

    EXPECT_EQ(reader_errors.load(), 0) << "Reader saw corrupted value";

    // Drain background compaction
    db->compact();

    // Final scan vs oracle
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

TEST(ConcurrencyTest, BackgroundCompactionReducesFiles) {
    TempDir dir;
    keystone::Options opts;
    opts.flush_threshold_bytes = 512;
    opts.compaction_trigger = 4;

    auto db = keystone::DB::open(dir.path, opts);

    for (int i = 0; i < 200; i++) {
        char key[32], val[128];
        std::snprintf(key, sizeof(key), "key_%04d", i % 20);
        std::memset(val, 'x', 100);
        val[100] = '\0';
        db->put(key, val);
    }
    db->flush();

    int initial_count = count_sst_files(dir.path);
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(10);
    int final_count = -1;
    while (std::chrono::steady_clock::now() < deadline) {
        int count = count_sst_files(dir.path);
        if (count < initial_count || count < static_cast<int>(opts.compaction_trigger)) {
            final_count = count;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    EXPECT_GT(final_count, 0)
        << "Background compaction did not reduce file count within 10s";
}

#include "keystone/db.h"

#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <functional>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <sys/utsname.h>

// ── Engine interface ────────────────────────────────────────────────────────

struct Engine {
    virtual ~Engine() = default;
    virtual void put(std::string_view key, std::string_view value) = 0;
    virtual std::optional<std::string> get(std::string_view key) = 0;
    virtual void scan(std::string_view start, std::string_view end,
                      const std::function<void(std::string_view,
                                               std::string_view)>& cb) = 0;
    virtual const char* name() const = 0;
};

// ── KeystoneDB engine ───────────────────────────────────────────────────────

struct KeystoneEngine : Engine {
    std::unique_ptr<keystone::DB> db;

    KeystoneEngine(const std::string& dir, bool durable) {
        keystone::Options opts;
        opts.sync = durable ? keystone::SyncMode::EveryWrite
                            : keystone::SyncMode::Batched;
        db = keystone::DB::open(dir, opts);
    }

    void put(std::string_view k, std::string_view v) override { db->put(k, v); }
    std::optional<std::string> get(std::string_view k) override { return db->get(k); }
    void scan(std::string_view start, std::string_view end,
              const std::function<void(std::string_view,
                                       std::string_view)>& cb) override {
        db->scan(start, end, cb);
    }
    const char* name() const override { return "KeystoneDB"; }
};

// ── SQLite engine ───────────────────────────────────────────────────────────

struct SqliteEngine : Engine {
    sqlite3* conn = nullptr;
    sqlite3_stmt* stmt_put = nullptr;
    sqlite3_stmt* stmt_get = nullptr;
    sqlite3_stmt* stmt_scan = nullptr;
    std::mutex mu_;

    SqliteEngine(const std::string& dir, bool durable) {
        std::string path = dir + "/bench.db";
        if (sqlite3_open(path.c_str(), &conn) != SQLITE_OK) {
            std::fprintf(stderr, "sqlite3_open failed: %s\n",
                         sqlite3_errmsg(conn));
            std::exit(1);
        }

        exec("PRAGMA journal_mode=WAL");
        exec(durable ? "PRAGMA synchronous=FULL" : "PRAGMA synchronous=NORMAL");
        exec("CREATE TABLE IF NOT EXISTS kv(key BLOB PRIMARY KEY, value BLOB)");

        prepare("INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)", &stmt_put);
        prepare("SELECT value FROM kv WHERE key=?", &stmt_get);
        prepare("SELECT key,value FROM kv WHERE key BETWEEN ? AND ? ORDER BY key",
                &stmt_scan);
    }

    ~SqliteEngine() override {
        if (stmt_put) sqlite3_finalize(stmt_put);
        if (stmt_get) sqlite3_finalize(stmt_get);
        if (stmt_scan) sqlite3_finalize(stmt_scan);
        if (conn) sqlite3_close(conn);
    }

    void put(std::string_view k, std::string_view v) override {
        std::lock_guard<std::mutex> lock(mu_);
        sqlite3_bind_blob(stmt_put, 1, k.data(),
                          static_cast<int>(k.size()), SQLITE_STATIC);
        sqlite3_bind_blob(stmt_put, 2, v.data(),
                          static_cast<int>(v.size()), SQLITE_STATIC);
        sqlite3_step(stmt_put);
        sqlite3_reset(stmt_put);
    }

    std::optional<std::string> get(std::string_view k) override {
        sqlite3_bind_blob(stmt_get, 1, k.data(),
                          static_cast<int>(k.size()), SQLITE_STATIC);
        std::optional<std::string> result;
        if (sqlite3_step(stmt_get) == SQLITE_ROW) {
            auto ptr = static_cast<const char*>(sqlite3_column_blob(stmt_get, 0));
            int len = sqlite3_column_bytes(stmt_get, 0);
            result.emplace(ptr, static_cast<size_t>(len));
        }
        sqlite3_reset(stmt_get);
        return result;
    }

    void scan(std::string_view start, std::string_view end,
              const std::function<void(std::string_view,
                                       std::string_view)>& cb) override {
        sqlite3_bind_blob(stmt_scan, 1, start.data(),
                          static_cast<int>(start.size()), SQLITE_STATIC);
        sqlite3_bind_blob(stmt_scan, 2, end.data(),
                          static_cast<int>(end.size()), SQLITE_STATIC);
        while (sqlite3_step(stmt_scan) == SQLITE_ROW) {
            auto kp = static_cast<const char*>(sqlite3_column_blob(stmt_scan, 0));
            int kl = sqlite3_column_bytes(stmt_scan, 0);
            auto vp = static_cast<const char*>(sqlite3_column_blob(stmt_scan, 1));
            int vl = sqlite3_column_bytes(stmt_scan, 1);
            cb({kp, static_cast<size_t>(kl)}, {vp, static_cast<size_t>(vl)});
        }
        sqlite3_reset(stmt_scan);
    }

    const char* name() const override { return "SQLite"; }

private:
    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(conn, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            std::fprintf(stderr, "sqlite3_exec(%s) failed: %s\n", sql, err);
            sqlite3_free(err);
            std::exit(1);
        }
    }

    void prepare(const char* sql, sqlite3_stmt** out) {
        if (sqlite3_prepare_v2(conn, sql, -1, out, nullptr) != SQLITE_OK) {
            std::fprintf(stderr, "sqlite3_prepare(%s) failed: %s\n",
                         sql, sqlite3_errmsg(conn));
            std::exit(1);
        }
    }
};

// ── Key helpers ─────────────────────────────────────────────────────────────

static std::string make_key(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key%012d", i);
    return buf;
}

// ── Timing helpers ──────────────────────────────────────────────────────────

struct Stats {
    double ops_per_sec;
    double p50_us;
    double p99_us;
};

static Stats compute_stats(std::vector<double>& latencies) {
    if (latencies.empty()) return {0, 0, 0};

    double total = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double ops_per_sec = static_cast<double>(latencies.size()) / (total / 1e6);

    size_t n = latencies.size();
    size_t p50_idx = n / 2;
    size_t p99_idx = n * 99 / 100;

    std::nth_element(latencies.begin(),
                     latencies.begin() + static_cast<long>(p50_idx),
                     latencies.end());
    double p50 = latencies[p50_idx];

    std::nth_element(latencies.begin(),
                     latencies.begin() + static_cast<long>(p99_idx),
                     latencies.end());
    double p99 = latencies[p99_idx];

    return {ops_per_sec, p50, p99};
}

// ── Workloads ───────────────────────────────────────────────────────────────

static Stats run_seq_write(Engine& eng, int n,
                           const std::vector<char>& value) {
    std::string_view val(value.data(), value.size());
    std::vector<double> lat;
    lat.reserve(static_cast<size_t>(n));

    for (int i = 0; i < n; i++) {
        std::string key = make_key(i);
        auto t0 = std::chrono::steady_clock::now();
        eng.put(key, val);
        auto t1 = std::chrono::steady_clock::now();
        lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return compute_stats(lat);
}

static Stats run_rand_write(Engine& eng, int n,
                            const std::vector<char>& value,
                            std::mt19937& rng) {
    std::string_view val(value.data(), value.size());
    std::vector<int> order(static_cast<size_t>(n));
    std::iota(order.begin(), order.end(), 0);
    std::shuffle(order.begin(), order.end(), rng);

    std::vector<double> lat;
    lat.reserve(static_cast<size_t>(n));

    for (int i : order) {
        std::string key = make_key(i);
        auto t0 = std::chrono::steady_clock::now();
        eng.put(key, val);
        auto t1 = std::chrono::steady_clock::now();
        lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return compute_stats(lat);
}

static void load_sequential(Engine& eng, int n,
                            const std::vector<char>& value) {
    std::string_view val(value.data(), value.size());
    for (int i = 0; i < n; i++)
        eng.put(make_key(i), val);
}

static Stats run_rand_read_hit(Engine& eng, int n, std::mt19937& rng) {
    std::uniform_int_distribution<int> dist(0, n - 1);
    std::vector<double> lat;
    lat.reserve(static_cast<size_t>(n));

    for (int i = 0; i < n; i++) {
        std::string key = make_key(dist(rng));
        auto t0 = std::chrono::steady_clock::now();
        auto r = eng.get(key);
        auto t1 = std::chrono::steady_clock::now();
        (void)r;
        lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return compute_stats(lat);
}

static Stats run_rand_read_miss(Engine& eng, int n, std::mt19937& rng) {
    std::uniform_int_distribution<int> dist(n, 2 * n - 1);
    std::vector<double> lat;
    lat.reserve(static_cast<size_t>(n));

    for (int i = 0; i < n; i++) {
        std::string key = make_key(dist(rng));
        auto t0 = std::chrono::steady_clock::now();
        auto r = eng.get(key);
        auto t1 = std::chrono::steady_clock::now();
        (void)r;
        lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return compute_stats(lat);
}

static Stats run_scan(Engine& eng, int n, int scan_len, int num_scans,
                      std::mt19937& rng) {
    int max_start = n - scan_len;
    if (max_start < 0) max_start = 0;
    std::uniform_int_distribution<int> dist(0, max_start);

    std::vector<double> lat;
    lat.reserve(static_cast<size_t>(num_scans));

    for (int i = 0; i < num_scans; i++) {
        int start_idx = dist(rng);
        std::string start_key = make_key(start_idx);
        std::string end_key = make_key(start_idx + scan_len - 1);
        int count = 0;

        auto t0 = std::chrono::steady_clock::now();
        eng.scan(start_key, end_key,
                 [&](std::string_view, std::string_view) { count++; });
        auto t1 = std::chrono::steady_clock::now();
        (void)count;
        lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return compute_stats(lat);
}

static Stats run_conc_write(Engine& eng, int n,
                            const std::vector<char>& value,
                            int num_writers) {
    std::string_view val(value.data(), value.size());
    int per_thread = n / num_writers;

    std::vector<std::vector<double>> thread_lats(
        static_cast<size_t>(num_writers));

    auto wall_t0 = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < num_writers; t++) {
        threads.emplace_back([&, t] {
            auto& lat = thread_lats[static_cast<size_t>(t)];
            lat.reserve(static_cast<size_t>(per_thread));
            int start = t * per_thread;
            int end = start + per_thread;
            for (int i = start; i < end; i++) {
                std::string key = make_key(i);
                auto t0 = std::chrono::steady_clock::now();
                eng.put(key, val);
                auto t1 = std::chrono::steady_clock::now();
                lat.push_back(
                    std::chrono::duration<double, std::micro>(t1 - t0).count());
            }
        });
    }
    for (auto& t : threads) t.join();

    auto wall_t1 = std::chrono::steady_clock::now();

    std::vector<double> all_lats;
    all_lats.reserve(static_cast<size_t>(n));
    for (auto& v : thread_lats)
        all_lats.insert(all_lats.end(), v.begin(), v.end());

    double wall_secs =
        std::chrono::duration<double>(wall_t1 - wall_t0).count();
    auto stats = compute_stats(all_lats);
    stats.ops_per_sec = static_cast<double>(all_lats.size()) / wall_secs;
    return stats;
}

// ── Result row ──────────────────────────────────────────────────────────────

struct Result {
    const char* engine;
    const char* workload;
    Stats stats;
};

// ── Workload filter helper ──────────────────────────────────────────────────

static std::set<std::string> parse_workloads(const std::string& arg) {
    std::set<std::string> result;
    std::istringstream ss(arg);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) result.insert(token);
    }
    return result;
}

// ── Main ────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    int n = 200000;
    int value_bytes = 100;
    int scan_len = 100;
    int num_scans = 2000;
    int num_writers = 1;
    std::string base_dir;
    std::string sync_mode = "durable";
    std::string workload_filter;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-n" && i + 1 < argc) n = std::atoi(argv[++i]);
        else if (arg == "-v" && i + 1 < argc) value_bytes = std::atoi(argv[++i]);
        else if (arg == "-scan-len" && i + 1 < argc) scan_len = std::atoi(argv[++i]);
        else if (arg == "-scans" && i + 1 < argc) num_scans = std::atoi(argv[++i]);
        else if (arg == "-writers" && i + 1 < argc) num_writers = std::atoi(argv[++i]);
        else if (arg == "-dir" && i + 1 < argc) base_dir = argv[++i];
        else if (arg == "-sync" && i + 1 < argc) sync_mode = argv[++i];
        else if (arg == "-workloads" && i + 1 < argc) workload_filter = argv[++i];
        else {
            std::fprintf(stderr,
                "Usage: bench [-n ops] [-v value_bytes] [-scan-len L] "
                "[-scans M] [-writers T] [-dir tmpdir] [-sync durable|batched] "
                "[-workloads name,name,...]\n");
            return 1;
        }
    }

    bool durable = (sync_mode == "durable");
    std::set<std::string> selected_workloads;
    if (!workload_filter.empty())
        selected_workloads = parse_workloads(workload_filter);

    // Print environment
    struct utsname uts;
    if (uname(&uts) == 0) {
        std::printf("Environment: %s %s %s %s\n",
                    uts.sysname, uts.release, uts.machine, uts.nodename);
    }
    std::printf("Config: n=%d value_bytes=%d scan_len=%d scans=%d writers=%d sync=%s\n",
                n, value_bytes, scan_len, num_scans, num_writers,
                sync_mode.c_str());
    if (!selected_workloads.empty()) {
        std::printf("Workloads:");
        for (const auto& w : selected_workloads) std::printf(" %s", w.c_str());
        std::printf("\n");
    }
    std::printf("\n");

    // Generate value buffer
    std::mt19937 val_rng(12345);
    std::vector<char> value(static_cast<size_t>(value_bytes));
    for (auto& c : value)
        c = static_cast<char>(val_rng() & 0xFF);

    // Temp dir helper
    auto make_temp = [&](const std::string& label) -> std::string {
        std::string dir;
        if (!base_dir.empty()) {
            dir = base_dir + "/" + label;
        } else {
            dir = std::filesystem::temp_directory_path().string() +
                  "/ks_bench_" + label;
        }
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        return dir;
    };

    // Run one workload for one engine
    struct WorkloadDef {
        const char* name;
        std::function<Stats(Engine& eng, const std::string& dir)> run;
    };

    std::mt19937 rng_seed_gen(42);

    // Loading for read/scan workloads always uses batched sync (SyncMode::Batched
    // / PRAGMA synchronous=NORMAL) regardless of -sync.  Loading is setup, not
    // the measured operation — using durable sync for setup makes large-n runs
    // take hours due to F_FULLFSYNC overhead.  Only write workloads (seq_write,
    // rand_write, conc_write) honor -sync for the measured writes.

    auto make_keystone = [&](const std::string& dir, bool dur) {
        return std::make_unique<KeystoneEngine>(dir, dur);
    };
    auto make_sqlite = [&](const std::string& dir, bool dur) {
        return std::make_unique<SqliteEngine>(dir, dur);
    };

    // For read/scan workloads: load with batched sync, reopen with user's sync
    // mode for the measured operations.
    using MakeEngine = std::function<std::unique_ptr<Engine>(const std::string&, bool)>;

    auto load_then_read = [&](MakeEngine maker, const std::string& dir,
                              int count, const std::vector<char>& val,
                              std::function<Stats(Engine&)> measure) -> Stats {
        {
            auto loader = maker(dir, false);
            load_sequential(*loader, count, val);
        }
        auto eng = maker(dir, durable);
        return measure(*eng);
    };

    std::vector<WorkloadDef> workloads = {
        {"seq_write", [&](Engine& eng, const std::string&) {
            return run_seq_write(eng, n, value);
        }},
        {"rand_write", [&](Engine& eng, const std::string&) {
            std::mt19937 rng(rng_seed_gen());
            return run_rand_write(eng, n, value, rng);
        }},
        {"rand_read_hit", [&](Engine&, const std::string&) {
            // Placeholder — handled specially below
            return Stats{0, 0, 0};
        }},
        {"rand_read_miss", [&](Engine&, const std::string&) {
            return Stats{0, 0, 0};
        }},
        {"scan", [&](Engine&, const std::string&) {
            return Stats{0, 0, 0};
        }},
        {"conc_write", [&](Engine& eng, const std::string&) {
            return run_conc_write(eng, n, value, num_writers);
        }},
    };

    // Collect results
    std::vector<Result> results;

    for (auto& wl : workloads) {
        std::string wl_name(wl.name);

        if (!selected_workloads.empty() &&
            selected_workloads.find(wl_name) == selected_workloads.end())
            continue;

        bool is_read_workload = (wl_name == "rand_read_hit" ||
                                 wl_name == "rand_read_miss" ||
                                 wl_name == "scan");

        if (is_read_workload) {
            // Read/scan: load with batched sync, measure with user's sync mode
            // KeystoneDB
            {
                std::string dir = make_temp(std::string("keystone_") + wl.name);
                std::mt19937 rng(rng_seed_gen());
                auto stats = load_then_read(
                    make_keystone, dir, n, value,
                    [&](Engine& eng) -> Stats {
                        if (wl_name == "rand_read_hit")
                            return run_rand_read_hit(eng, n, rng);
                        else if (wl_name == "rand_read_miss")
                            return run_rand_read_miss(eng, n, rng);
                        else
                            return run_scan(eng, n, scan_len, num_scans, rng);
                    });
                results.push_back({"KeystoneDB", wl.name, stats});
                std::filesystem::remove_all(dir);
            }
            // SQLite
            {
                std::string dir = make_temp(std::string("sqlite_") + wl.name);
                std::mt19937 rng(rng_seed_gen());
                auto stats = load_then_read(
                    make_sqlite, dir, n, value,
                    [&](Engine& eng) -> Stats {
                        if (wl_name == "rand_read_hit")
                            return run_rand_read_hit(eng, n, rng);
                        else if (wl_name == "rand_read_miss")
                            return run_rand_read_miss(eng, n, rng);
                        else
                            return run_scan(eng, n, scan_len, num_scans, rng);
                    });
                results.push_back({"SQLite", wl.name, stats});
                std::filesystem::remove_all(dir);
            }
        } else {
            // Write workloads: engine uses the user's -sync mode
            // KeystoneDB
            {
                std::string dir = make_temp(std::string("keystone_") + wl.name);
                auto eng = std::make_unique<KeystoneEngine>(dir, durable);
                auto stats = wl.run(*eng, dir);
                results.push_back({eng->name(), wl.name, stats});
                eng.reset();
                std::filesystem::remove_all(dir);
            }
            // SQLite
            {
                std::string dir = make_temp(std::string("sqlite_") + wl.name);
                auto eng = std::make_unique<SqliteEngine>(dir, durable);
                auto stats = wl.run(*eng, dir);
                results.push_back({eng->name(), wl.name, stats});
                eng.reset();
                std::filesystem::remove_all(dir);
            }
        }
    }

    // Print table
    std::printf("%-12s %-16s %12s %10s %10s\n",
                "Engine", "Workload", "ops/s", "p50 us", "p99 us");
    std::printf("%-12s %-16s %12s %10s %10s\n",
                "------", "--------", "-----", "------", "------");

    for (const auto& r : results) {
        std::printf("%-12s %-16s %12.0f %10.1f %10.1f\n",
                    r.engine, r.workload,
                    r.stats.ops_per_sec, r.stats.p50_us, r.stats.p99_us);
    }

    // Write CSV
    std::string exe_path = std::filesystem::path(argv[0]).parent_path().string();
    std::string csv_path = exe_path + "/results.csv";

    {
        auto src_bench = std::filesystem::path(argv[0]).parent_path().parent_path()
                         / "bench";
        if (std::filesystem::exists(src_bench) &&
            std::filesystem::is_directory(src_bench)) {
            csv_path = (src_bench / "results.csv").string();
        }
    }

    bool write_header = !std::filesystem::exists(csv_path);
    FILE* csv = std::fopen(csv_path.c_str(), "a");
    if (csv) {
        if (write_header) {
            std::fprintf(csv,
                "engine,workload,sync,ops_per_s,p50_us,p99_us,n,value_bytes,writers\n");
        }
        for (const auto& r : results) {
            std::fprintf(csv, "%s,%s,%s,%.0f,%.1f,%.1f,%d,%d,%d\n",
                         r.engine, r.workload, sync_mode.c_str(),
                         r.stats.ops_per_sec, r.stats.p50_us, r.stats.p99_us,
                         n, value_bytes, num_writers);
        }
        std::fclose(csv);
        std::printf("\nResults appended to %s\n", csv_path.c_str());
    } else {
        std::fprintf(stderr, "Warning: could not open %s for writing\n",
                     csv_path.c_str());
    }

    return 0;
}

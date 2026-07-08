#include "keystone/db.h"

#include <cstdio>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: torture <write|verify> <dir>\n");
        return 1;
    }

    std::string cmd = argv[1];
    std::string dir = argv[2];

    if (cmd == "write") {
        keystone::Options opts;
        opts.flush_threshold_bytes = 64 * 1024;
        auto db = keystone::DB::open(dir, opts);
        int start = 0;
        char key[32];
        while (true) {
            std::snprintf(key, sizeof(key), "key%08d", start);
            if (!db->get(key).has_value()) break;
            ++start;
        }
        for (int i = start;; ++i) {
            std::snprintf(key, sizeof(key), "key%08d", i);
            db->put(key, std::string("val:") + key);
        }
    } else if (cmd == "verify") {
        auto db = keystone::DB::open(dir);
        int prefix_len = 0;
        char key[32];
        while (true) {
            std::snprintf(key, sizeof(key), "key%08d", prefix_len);
            auto val = db->get(key);
            if (!val.has_value()) break;
            std::string expected = "val:" + std::string(key);
            if (val.value() != expected) {
                std::fprintf(stderr, "CORRUPTION: %s expected=%s got=%s\n",
                             key, expected.c_str(), val.value().c_str());
                return 1;
            }
            ++prefix_len;
        }
        for (int i = prefix_len + 1; i < prefix_len + 1000; ++i) {
            std::snprintf(key, sizeof(key), "key%08d", i);
            if (db->get(key).has_value()) {
                std::fprintf(stderr, "HOLE: %s present after gap at %d\n",
                             key, prefix_len);
                return 1;
            }
        }
        std::printf("%d\n", prefix_len);
        return 0;
    } else {
        std::fprintf(stderr, "unknown subcommand: %s\n", cmd.c_str());
        return 1;
    }
    return 0;
}

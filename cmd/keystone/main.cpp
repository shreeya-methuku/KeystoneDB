#include "keystone/db.h"

#include <iostream>
#include <sstream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "usage: keystone <dir>\n";
        return 1;
    }

    auto db = keystone::DB::open(argv[1]);
    std::string line;

    while (std::getline(std::cin, line)) {
        std::istringstream iss(line);
        std::string cmd;
        if (!(iss >> cmd)) continue;

        if (cmd == "put") {
            std::string key, value;
            if (!(iss >> key >> value)) {
                std::cout << "ERR: put <key> <value>\n";
                continue;
            }
            db->put(key, value);
            std::cout << "OK\n";
        } else if (cmd == "get") {
            std::string key;
            if (!(iss >> key)) {
                std::cout << "ERR: get <key>\n";
                continue;
            }
            auto val = db->get(key);
            if (val)
                std::cout << *val << "\n";
            else
                std::cout << "(nil)\n";
        } else if (cmd == "del") {
            std::string key;
            if (!(iss >> key)) {
                std::cout << "ERR: del <key>\n";
                continue;
            }
            db->del(key);
            std::cout << "OK\n";
        } else if (cmd == "scan") {
            std::string k1, k2;
            if (!(iss >> k1 >> k2)) {
                std::cout << "ERR: scan <start> <end>\n";
                continue;
            }
            int count = 0;
            db->scan(k1, k2, [&](std::string_view k, std::string_view v) {
                std::cout << k << " = " << v << "\n";
                ++count;
            });
            std::cout << "(" << count << " results)\n";
        } else if (cmd == "flush") {
            db->flush();
            std::cout << "OK\n";
        } else if (cmd == "help") {
            std::cout << "put <key> <value>  — store a key-value pair\n"
                      << "get <key>          — retrieve a value\n"
                      << "del <key>          — delete a key\n"
                      << "scan <start> <end> — range scan [start, end]\n"
                      << "flush              — flush memtable to SSTable\n"
                      << "exit / quit        — shut down\n";
        } else if (cmd == "exit" || cmd == "quit") {
            break;
        } else {
            std::cout << "ERR: unknown command '" << cmd
                      << "'. Type 'help'.\n";
        }
    }

    return 0;
}

#pragma once

#include <memory>
#include <string>
#include <vector>

namespace keystone {

class Manifest {
public:
    static std::unique_ptr<Manifest> open(const std::string& path);

    void append_snapshot(const std::vector<int>& live_numbers);

    static std::vector<int> load_latest(const std::string& path);

    ~Manifest();

    Manifest(const Manifest&) = delete;
    Manifest& operator=(const Manifest&) = delete;

private:
    explicit Manifest(int fd);
    int fd_;
};

}  // namespace keystone

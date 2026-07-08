#pragma once

#include <memory>
#include <string>
#include <vector>

#include "keystone/sstable.h"

namespace keystone {

struct FileMeta {
    int number;
    int level;
    std::string smallest_key;
    std::string largest_key;
    std::shared_ptr<SSTable> table;
};

struct Version {
    static constexpr int kMaxLevels = 7;
    std::vector<FileMeta> files[kMaxLevels];
};

}  // namespace keystone

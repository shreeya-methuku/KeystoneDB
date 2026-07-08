#pragma once

#include <memory>
#include <vector>

#include "keystone/sstable.h"

namespace keystone {

struct Version {
    std::vector<std::shared_ptr<SSTable>> ssts;  // front=oldest, back=newest
};

}  // namespace keystone

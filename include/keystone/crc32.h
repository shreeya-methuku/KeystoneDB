#pragma once

#include <cstddef>
#include <cstdint>

namespace keystone {
uint32_t crc32(const void* data, size_t len);
}

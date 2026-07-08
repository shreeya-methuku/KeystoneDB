#include "keystone/crc32.h"

namespace keystone {
namespace {

struct Table {
    uint32_t entries[256];
    constexpr Table() : entries{} {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int j = 0; j < 8; j++)
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            entries[i] = c;
        }
    }
};

static constexpr Table kTable{};

}  // namespace

uint32_t crc32(const void* data, size_t len) {
    auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        crc = kTable.entries[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

}  // namespace keystone

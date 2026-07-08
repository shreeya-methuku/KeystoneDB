#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace keystone {

class Bloom {
public:
    // Builder: create from expected key count and bits-per-key (default 10).
    // Sets m = key_count * bits_per_key rounded up to a byte multiple (>= 8),
    // and k = clamp(round(bits_per_key * ln2), 1, 30).
    explicit Bloom(size_t key_count, int bits_per_key = 10);

    void add(std::string_view key);
    bool may_contain(std::string_view key) const;

    // Serialize: [k: u32][m_bits: u64][bit array: m_bits/8 bytes]
    std::vector<uint8_t> serialize() const;
    static Bloom load(const uint8_t* data, size_t len);

private:
    Bloom(std::vector<uint8_t> bits, uint64_t m_bits, int k);

    // FNV-1a 64-bit split into two 32-bit halves for double hashing.
    static void hash(std::string_view key, uint32_t& h1, uint32_t& h2);

    std::vector<uint8_t> bits_;
    uint64_t m_bits_;
    int k_;
};

}  // namespace keystone

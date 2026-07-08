#include "keystone/bloom.h"
#include "coding.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <stdexcept>

namespace keystone {

using coding::put_u32_le;
using coding::get_u32_le;
using coding::put_u64_le;
using coding::get_u64_le;

Bloom::Bloom(size_t key_count, int bits_per_key) {
    if (key_count == 0) key_count = 1;
    uint64_t raw = static_cast<uint64_t>(key_count) *
                   static_cast<uint64_t>(bits_per_key);
    m_bits_ = ((raw + 7) / 8) * 8;
    if (m_bits_ < 8) m_bits_ = 8;
    bits_.resize(static_cast<size_t>(m_bits_ / 8), 0);
    int raw_k = static_cast<int>(std::lround(bits_per_key * 0.6931));
    k_ = std::max(1, std::min(30, raw_k));
}

Bloom::Bloom(std::vector<uint8_t> bits, uint64_t m_bits, int k)
    : bits_(std::move(bits)), m_bits_(m_bits), k_(k) {}

void Bloom::hash(std::string_view key, uint32_t& h1, uint32_t& h2) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < key.size(); i++) {
        h ^= static_cast<uint64_t>(static_cast<uint8_t>(key[i]));
        h *= 0x00000100000001B3ULL;
    }
    h1 = static_cast<uint32_t>(h);
    h2 = static_cast<uint32_t>(h >> 32);
}

void Bloom::add(std::string_view key) {
    uint32_t h1, h2;
    hash(key, h1, h2);
    for (int i = 0; i < k_; i++) {
        uint64_t idx = (static_cast<uint64_t>(h1) +
                        static_cast<uint64_t>(i) *
                            static_cast<uint64_t>(h2)) %
                       m_bits_;
        unsigned bit_pos = static_cast<unsigned>(idx % 8);
        bits_[static_cast<size_t>(idx / 8)] |=
            static_cast<uint8_t>(1u << bit_pos);
    }
}

bool Bloom::may_contain(std::string_view key) const {
    uint32_t h1, h2;
    hash(key, h1, h2);
    for (int i = 0; i < k_; i++) {
        uint64_t idx = (static_cast<uint64_t>(h1) +
                        static_cast<uint64_t>(i) *
                            static_cast<uint64_t>(h2)) %
                       m_bits_;
        unsigned bit_pos = static_cast<unsigned>(idx % 8);
        if (!(bits_[static_cast<size_t>(idx / 8)] & (1u << bit_pos)))
            return false;
    }
    return true;
}

std::vector<uint8_t> Bloom::serialize() const {
    std::vector<uint8_t> out(12 + bits_.size());
    put_u32_le(out.data(), static_cast<uint32_t>(k_));
    put_u64_le(out.data() + 4, m_bits_);
    std::memcpy(out.data() + 12, bits_.data(), bits_.size());
    return out;
}

Bloom Bloom::load(const uint8_t* data, size_t len) {
    if (len < 12)
        throw std::runtime_error("Bloom: data too short");
    int k = static_cast<int>(get_u32_le(data));
    uint64_t m_bits = get_u64_le(data + 4);
    size_t byte_count = static_cast<size_t>((m_bits + 7) / 8);
    if (len < 12 + byte_count)
        throw std::runtime_error("Bloom: truncated bit array");
    std::vector<uint8_t> bits(data + 12, data + 12 + byte_count);
    return Bloom(std::move(bits), m_bits, k);
}

}  // namespace keystone

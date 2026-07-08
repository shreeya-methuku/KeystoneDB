// Internal encoding and I/O helpers — not part of the public API.
#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <unistd.h>

namespace keystone {
namespace coding {

inline void put_u32_le(uint8_t* buf, uint32_t v) {
    buf[0] = static_cast<uint8_t>(v);
    buf[1] = static_cast<uint8_t>(v >> 8);
    buf[2] = static_cast<uint8_t>(v >> 16);
    buf[3] = static_cast<uint8_t>(v >> 24);
}

inline uint32_t get_u32_le(const uint8_t* buf) {
    return static_cast<uint32_t>(buf[0])
         | (static_cast<uint32_t>(buf[1]) << 8)
         | (static_cast<uint32_t>(buf[2]) << 16)
         | (static_cast<uint32_t>(buf[3]) << 24);
}

inline void put_u64_le(uint8_t* buf, uint64_t v) {
    buf[0] = static_cast<uint8_t>(v);
    buf[1] = static_cast<uint8_t>(v >> 8);
    buf[2] = static_cast<uint8_t>(v >> 16);
    buf[3] = static_cast<uint8_t>(v >> 24);
    buf[4] = static_cast<uint8_t>(v >> 32);
    buf[5] = static_cast<uint8_t>(v >> 40);
    buf[6] = static_cast<uint8_t>(v >> 48);
    buf[7] = static_cast<uint8_t>(v >> 56);
}

inline uint64_t get_u64_le(const uint8_t* buf) {
    return static_cast<uint64_t>(buf[0])
         | (static_cast<uint64_t>(buf[1]) << 8)
         | (static_cast<uint64_t>(buf[2]) << 16)
         | (static_cast<uint64_t>(buf[3]) << 24)
         | (static_cast<uint64_t>(buf[4]) << 32)
         | (static_cast<uint64_t>(buf[5]) << 40)
         | (static_cast<uint64_t>(buf[6]) << 48)
         | (static_cast<uint64_t>(buf[7]) << 56);
}

}  // namespace coding

namespace io {

inline void write_fully(int fd, const void* buf, size_t count) {
    auto* p = static_cast<const uint8_t*>(buf);
    while (count > 0) {
        ssize_t n = ::write(fd, p, count);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(
                std::string("write failed: ") + std::strerror(errno));
        }
        p += n;
        count -= static_cast<size_t>(n);
    }
}

inline bool read_fully(int fd, void* buf, size_t count) {
    auto* p = static_cast<uint8_t*>(buf);
    while (count > 0) {
        ssize_t n = ::read(fd, p, count);
        if (n <= 0) return false;
        p += n;
        count -= static_cast<size_t>(n);
    }
    return true;
}

// macOS fsync() only guarantees data reaches the drive's write cache, not the
// physical media. F_FULLFSYNC flushes the drive's volatile write cache to the
// storage medium, giving true durability.
inline void durable_sync(int fd) {
#ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) == -1) {
        if (errno == EINVAL || errno == ENOTSUP)
            ::fsync(fd);
    }
#else
    ::fdatasync(fd);
#endif
}

}  // namespace io
}  // namespace keystone

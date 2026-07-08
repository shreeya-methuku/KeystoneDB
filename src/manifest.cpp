#include "keystone/manifest.h"
#include "keystone/crc32.h"
#include "coding.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace keystone {

using coding::put_u32_le;
using coding::get_u32_le;
using io::write_fully;
using io::read_fully;
using io::durable_sync;

Manifest::Manifest(int fd) : fd_(fd) {}

Manifest::~Manifest() {
    if (fd_ != -1)
        ::close(fd_);
}

std::unique_ptr<Manifest> Manifest::open(const std::string& path) {
    int fd = ::open(path.c_str(), O_APPEND | O_CREAT | O_WRONLY, 0644);
    if (fd == -1)
        throw std::runtime_error(
            std::string("Manifest open failed: ") + std::strerror(errno));
    return std::unique_ptr<Manifest>(new Manifest(fd));
}

void Manifest::append_snapshot(const std::vector<int>& live_numbers) {
    uint32_t count = static_cast<uint32_t>(live_numbers.size());
    size_t body_len = 4 + 4 * static_cast<size_t>(count);
    std::vector<uint8_t> buf(4 + body_len);

    uint8_t* body = buf.data() + 4;
    put_u32_le(body, count);
    for (uint32_t i = 0; i < count; i++)
        put_u32_le(body + 4 + 4 * i,
                   static_cast<uint32_t>(live_numbers[i]));

    put_u32_le(buf.data(), crc32(body, body_len));

    write_fully(fd_, buf.data(), buf.size());
    durable_sync(fd_);
}

std::vector<int> Manifest::load_latest(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd == -1) return {};

    std::vector<int> latest;

    for (;;) {
        uint8_t crc_buf[4];
        if (!read_fully(fd, crc_buf, 4)) break;
        uint32_t stored_crc = get_u32_le(crc_buf);

        uint8_t count_buf[4];
        if (!read_fully(fd, count_buf, 4)) break;
        uint32_t count = get_u32_le(count_buf);

        if (count > 64u * 1024) break;

        std::vector<uint8_t> body(4 + 4 * static_cast<size_t>(count));
        std::memcpy(body.data(), count_buf, 4);
        if (count > 0 &&
            !read_fully(fd, body.data() + 4, 4 * static_cast<size_t>(count)))
            break;

        if (crc32(body.data(), body.size()) != stored_crc) break;

        latest.clear();
        for (uint32_t i = 0; i < count; i++)
            latest.push_back(
                static_cast<int>(get_u32_le(body.data() + 4 + 4 * i)));
    }

    ::close(fd);
    return latest;
}

}  // namespace keystone

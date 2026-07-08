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

void Manifest::append_snapshot(const std::vector<ManifestEntry>& files) {
    uint32_t count = static_cast<uint32_t>(files.size());
    size_t body_len = 4;
    for (const auto& f : files)
        body_len += 4 + 4 + 4 + f.smallest_key.size() + 4 + f.largest_key.size();

    std::vector<uint8_t> buf(4 + body_len);
    uint8_t* body = buf.data() + 4;
    size_t pos = 0;
    put_u32_le(body + pos, count); pos += 4;

    for (const auto& f : files) {
        put_u32_le(body + pos, static_cast<uint32_t>(f.number)); pos += 4;
        put_u32_le(body + pos, static_cast<uint32_t>(f.level)); pos += 4;
        uint32_t sk_len = static_cast<uint32_t>(f.smallest_key.size());
        put_u32_le(body + pos, sk_len); pos += 4;
        std::memcpy(body + pos, f.smallest_key.data(), sk_len); pos += sk_len;
        uint32_t lk_len = static_cast<uint32_t>(f.largest_key.size());
        put_u32_le(body + pos, lk_len); pos += 4;
        std::memcpy(body + pos, f.largest_key.data(), lk_len); pos += lk_len;
    }

    put_u32_le(buf.data(), crc32(body, body_len));

    write_fully(fd_, buf.data(), buf.size());
    durable_sync(fd_);
}

std::vector<ManifestEntry> Manifest::load_latest(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd == -1) return {};

    std::vector<ManifestEntry> latest;

    for (;;) {
        uint8_t crc_buf[4];
        if (!read_fully(fd, crc_buf, 4)) break;
        uint32_t stored_crc = get_u32_le(crc_buf);

        uint8_t count_buf[4];
        if (!read_fully(fd, count_buf, 4)) break;
        uint32_t count = get_u32_le(count_buf);

        if (count > 64u * 1024) break;

        size_t remaining_min = count * (4 + 4 + 4 + 4);
        std::vector<uint8_t> body(4);
        std::memcpy(body.data(), count_buf, 4);

        std::vector<uint8_t> rest(remaining_min + 256 * 1024);
        size_t rest_read = 0;

        auto read_u32 = [&](uint32_t& out) -> bool {
            if (rest_read + 4 > rest.size()) return false;
            uint8_t tmp[4];
            if (!read_fully(fd, tmp, 4)) return false;
            if (rest_read + 4 > rest.size()) rest.resize(rest_read + 4);
            std::memcpy(rest.data() + rest_read, tmp, 4);
            rest_read += 4;
            out = get_u32_le(tmp);
            return true;
        };

        auto read_bytes = [&](size_t n, std::string& out) -> bool {
            if (n == 0) { out.clear(); return true; }
            if (rest_read + n > rest.size()) rest.resize(rest_read + n);
            if (!read_fully(fd, rest.data() + rest_read, n)) return false;
            out.assign(reinterpret_cast<const char*>(rest.data() + rest_read), n);
            rest_read += n;
            return true;
        };

        bool ok = true;
        std::vector<ManifestEntry> snapshot;
        for (uint32_t i = 0; i < count && ok; i++) {
            ManifestEntry e{};
            uint32_t num, level, sk_len, lk_len;
            if (!read_u32(num)) { ok = false; break; }
            if (!read_u32(level)) { ok = false; break; }
            if (!read_u32(sk_len)) { ok = false; break; }
            if (!read_bytes(sk_len, e.smallest_key)) { ok = false; break; }
            if (!read_u32(lk_len)) { ok = false; break; }
            if (!read_bytes(lk_len, e.largest_key)) { ok = false; break; }
            e.number = static_cast<int>(num);
            e.level = static_cast<int>(level);
            snapshot.push_back(std::move(e));
        }
        if (!ok) break;

        body.insert(body.end(), rest.data(), rest.data() + rest_read);
        if (crc32(body.data(), body.size()) != stored_crc) break;

        latest = std::move(snapshot);
    }

    ::close(fd);
    return latest;
}

}  // namespace keystone

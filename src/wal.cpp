#include "keystone/wal.h"
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

WAL::WAL(int fd, SyncMode mode) : fd_(fd), mode_(mode) {}

WAL::~WAL() {
    if (fd_ != -1)
        ::close(fd_);
}

std::unique_ptr<WAL> WAL::open(const std::string& path, SyncMode mode) {
    int fd = ::open(path.c_str(), O_APPEND | O_CREAT | O_WRONLY, 0644);
    if (fd == -1)
        throw std::runtime_error(
            std::string("WAL open failed: ") + std::strerror(errno));
    return std::unique_ptr<WAL>(new WAL(fd, mode));
}

void WAL::append(RecType type, std::string_view key, std::string_view value) {
    size_t body_len = 1 + 4 + 4 + key.size() + value.size();
    std::vector<uint8_t> buf(4 + body_len);

    uint8_t* body = buf.data() + 4;
    body[0] = static_cast<uint8_t>(type);
    put_u32_le(body + 1, static_cast<uint32_t>(key.size()));
    put_u32_le(body + 5, static_cast<uint32_t>(value.size()));
    std::memcpy(body + 9, key.data(), key.size());
    if (!value.empty())
        std::memcpy(body + 9 + key.size(), value.data(), value.size());

    put_u32_le(buf.data(), crc32(body, body_len));

    write_fully(fd_, buf.data(), buf.size());

    if (mode_ == SyncMode::EveryWrite) {
        durable_sync(fd_);
        fsync_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void WAL::append_batch(const std::vector<Record>& records) {
    if (records.empty()) return;

    size_t total = 0;
    for (const auto& rec : records)
        total += 4 + 1 + 4 + 4 + rec.key.size() + rec.value.size();

    std::vector<uint8_t> buf(total);
    size_t off = 0;

    for (const auto& rec : records) {
        size_t body_len = 1 + 4 + 4 + rec.key.size() + rec.value.size();
        uint8_t* body = buf.data() + off + 4;
        body[0] = static_cast<uint8_t>(rec.type);
        put_u32_le(body + 1, static_cast<uint32_t>(rec.key.size()));
        put_u32_le(body + 5, static_cast<uint32_t>(rec.value.size()));
        std::memcpy(body + 9, rec.key.data(), rec.key.size());
        if (!rec.value.empty())
            std::memcpy(body + 9 + rec.key.size(), rec.value.data(),
                        rec.value.size());
        put_u32_le(buf.data() + off, crc32(body, body_len));
        off += 4 + body_len;
    }

    write_fully(fd_, buf.data(), buf.size());

    if (mode_ == SyncMode::EveryWrite) {
        durable_sync(fd_);
        fsync_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void WAL::sync() {
    durable_sync(fd_);
}

std::vector<WAL::Record> WAL::replay(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd == -1) return {};

    std::vector<Record> records;

    for (;;) {
        uint8_t crc_buf[4];
        if (!read_fully(fd, crc_buf, 4)) break;
        uint32_t stored_crc = get_u32_le(crc_buf);

        uint8_t header[9];
        if (!read_fully(fd, header, 9)) break;

        uint32_t keylen = get_u32_le(header + 1);
        uint32_t vallen = get_u32_le(header + 5);

        if (keylen > 64u * 1024 * 1024 || vallen > 64u * 1024 * 1024) break;

        std::string key(keylen, '\0');
        std::string value(vallen, '\0');
        if (keylen > 0 && !read_fully(fd, key.data(), keylen)) break;
        if (vallen > 0 && !read_fully(fd, value.data(), vallen)) break;

        size_t body_len = 9 + keylen + vallen;
        std::vector<uint8_t> body(body_len);
        std::memcpy(body.data(), header, 9);
        if (keylen > 0) std::memcpy(body.data() + 9, key.data(), keylen);
        if (vallen > 0) std::memcpy(body.data() + 9 + keylen, value.data(), vallen);

        if (crc32(body.data(), body_len) != stored_crc) break;

        records.push_back(
            {static_cast<RecType>(header[0]), std::move(key), std::move(value)});
    }

    ::close(fd);
    return records;
}

}  // namespace keystone

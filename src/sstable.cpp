#include "keystone/sstable.h"
#include "keystone/block_cache.h"
#include "keystone/crc32.h"
#include "coding.h"

#include <filesystem>
#include <stdexcept>

#include <fcntl.h>
#include <unistd.h>

namespace keystone {

using coding::put_u32_le;
using coding::get_u32_le;
using coding::put_u64_le;
using coding::get_u64_le;
using io::write_fully;
using io::durable_sync;

static constexpr uint32_t kMagic = 0x4B535442u;        // "KSTB"
static constexpr uint32_t kFormatVersion = 3;
static constexpr size_t   kFooterSize = 40;
static constexpr size_t   kBlockTargetSize = 4096;

// ── SSTableWriter ────────────────────────────────────────────────────────────

SSTableWriter::SSTableWriter(const std::string& final_path)
    : final_path_(final_path),
      temp_path_(final_path + ".tmp"),
      current_offset_(0),
      entry_count_(0) {
    fd_ = ::open(temp_path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd_ == -1)
        throw std::runtime_error("SSTableWriter: cannot open " + temp_path_);
}

SSTableWriter::~SSTableWriter() {
    if (fd_ != -1)
        ::close(fd_);
}

void SSTableWriter::add(std::string_view key, std::string_view value,
                         bool tombstone, uint64_t seq) {
    if (!block_buf_.empty() && block_buf_.size() >= kBlockTargetSize &&
        key != block_first_key_)
        flush_block();

    if (block_buf_.empty())
        block_first_key_ = std::string(key);

    if (entry_count_ == 0) smallest_key_ = std::string(key);
    largest_key_ = std::string(key);

    block_buf_.push_back(tombstone ? uint8_t(1) : uint8_t(0));

    uint8_t seq_buf[8];
    put_u64_le(seq_buf, seq);
    block_buf_.insert(block_buf_.end(), seq_buf, seq_buf + 8);

    uint8_t lens[8];
    put_u32_le(lens, static_cast<uint32_t>(key.size()));
    uint32_t vallen = tombstone ? 0u : static_cast<uint32_t>(value.size());
    put_u32_le(lens + 4, vallen);
    block_buf_.insert(block_buf_.end(), lens, lens + 8);

    if (seq > max_seq_) max_seq_ = seq;

    auto kd = reinterpret_cast<const uint8_t*>(key.data());
    block_buf_.insert(block_buf_.end(), kd, kd + key.size());

    if (!tombstone && !value.empty()) {
        auto vd = reinterpret_cast<const uint8_t*>(value.data());
        block_buf_.insert(block_buf_.end(), vd, vd + value.size());
    }

    keys_.emplace_back(key);
    entry_count_++;
}

void SSTableWriter::flush_block() {
    if (block_buf_.empty()) return;

    uint32_t checksum = crc32(block_buf_.data(), block_buf_.size());
    uint8_t crc_buf[4];
    put_u32_le(crc_buf, checksum);

    uint64_t total = block_buf_.size() + 4;
    index_.push_back({block_first_key_, current_offset_, total});
    write_fully(fd_, block_buf_.data(), block_buf_.size());
    write_fully(fd_, crc_buf, 4);
    current_offset_ += total;
    block_buf_.clear();
    block_first_key_.clear();
}

void SSTableWriter::finish() {
    flush_block();

    // Write index block
    uint64_t index_offset = current_offset_;
    uint64_t index_size = 0;
    for (const auto& ie : index_) {
        uint8_t kl[4];
        put_u32_le(kl, static_cast<uint32_t>(ie.first_key.size()));
        write_fully(fd_, kl, 4);
        write_fully(fd_, ie.first_key.data(), ie.first_key.size());
        uint8_t offlen[16];
        put_u64_le(offlen, ie.offset);
        put_u64_le(offlen + 8, ie.length);
        write_fully(fd_, offlen, 16);
        index_size += 4 + ie.first_key.size() + 16;
    }

    // Build and write bloom block
    uint64_t bloom_offset = index_offset + index_size;
    Bloom bloom(keys_.size());
    for (const auto& k : keys_)
        bloom.add(k);
    auto bloom_data = bloom.serialize();
    write_fully(fd_, bloom_data.data(), bloom_data.size());

    // Write footer (40 bytes)
    uint8_t footer[kFooterSize] = {};
    put_u64_le(footer, index_offset);
    put_u64_le(footer + 8, bloom_offset);
    put_u64_le(footer + 16, entry_count_);
    put_u64_le(footer + 24, max_seq_);
    put_u32_le(footer + 32, kFormatVersion);
    put_u32_le(footer + 36, kMagic);
    write_fully(fd_, footer, kFooterSize);

    durable_sync(fd_);
    ::close(fd_);
    fd_ = -1;
}

void SSTableWriter::install(const std::string& temp_path,
                             const std::string& final_path,
                             const std::string& dir_path) {
    if (::rename(temp_path.c_str(), final_path.c_str()) != 0)
        throw std::runtime_error("SSTable install: rename failed");
    int dir_fd = ::open(dir_path.c_str(), O_RDONLY);
    if (dir_fd != -1) {
        durable_sync(dir_fd);
        ::close(dir_fd);
    }
}

// ── SSTable (reader) ─────────────────────────────────────────────────────────

SSTable::SSTable(int fd, std::string path, int number, Footer footer,
                 std::vector<IndexEntry> index, std::optional<Bloom> bloom)
    : fd_(fd), path_(std::move(path)), number_(number), footer_(footer),
      index_(std::move(index)), bloom_(std::move(bloom)) {}

SSTable::~SSTable() {
    if (fd_ != -1)
        ::close(fd_);
    if (unlink_on_destroy_)
        ::unlink(path_.c_str());
}

std::shared_ptr<SSTable> SSTable::open(const std::string& path,
                                       std::shared_ptr<BlockCache> cache) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd == -1)
        throw std::runtime_error("SSTable::open: cannot open " + path);

    off_t file_size = ::lseek(fd, 0, SEEK_END);
    if (file_size < static_cast<off_t>(kFooterSize)) {
        ::close(fd);
        throw std::runtime_error("SSTable::open: file too small");
    }

    uint8_t fbuf[kFooterSize];
    if (::pread(fd, fbuf, kFooterSize,
                file_size - static_cast<off_t>(kFooterSize)) !=
        static_cast<ssize_t>(kFooterSize)) {
        ::close(fd);
        throw std::runtime_error("SSTable::open: cannot read footer");
    }

    Footer footer{};
    footer.index_offset   = get_u64_le(fbuf);
    footer.bloom_offset   = get_u64_le(fbuf + 8);
    footer.entry_count    = get_u64_le(fbuf + 16);
    footer.max_seq        = get_u64_le(fbuf + 24);
    footer.format_version = get_u32_le(fbuf + 32);
    footer.magic          = get_u32_le(fbuf + 36);

    if (footer.magic != kMagic) {
        ::close(fd);
        throw std::runtime_error("SSTable::open: bad magic");
    }

    // Index runs up to bloom block (or footer if no bloom)
    uint64_t index_end = (footer.bloom_offset != 0)
        ? footer.bloom_offset
        : static_cast<uint64_t>(file_size) - kFooterSize;
    uint64_t index_len = index_end - footer.index_offset;

    std::vector<uint8_t> ibuf(index_len);
    if (index_len > 0 &&
        ::pread(fd, ibuf.data(), index_len,
                static_cast<off_t>(footer.index_offset)) !=
            static_cast<ssize_t>(index_len)) {
        ::close(fd);
        throw std::runtime_error("SSTable::open: cannot read index");
    }

    std::vector<IndexEntry> index;
    size_t pos = 0;
    while (pos + 4 <= ibuf.size()) {
        uint32_t keylen = get_u32_le(ibuf.data() + pos);
        pos += 4;
        if (pos + keylen + 16 > ibuf.size()) break;
        std::string key(reinterpret_cast<const char*>(ibuf.data() + pos),
                        keylen);
        pos += keylen;
        uint64_t offset = get_u64_le(ibuf.data() + pos); pos += 8;
        uint64_t length = get_u64_le(ibuf.data() + pos); pos += 8;
        index.push_back({std::move(key), offset, length});
    }

    // Load bloom filter if present
    std::optional<Bloom> bloom;
    if (footer.bloom_offset != 0) {
        uint64_t bloom_len =
            static_cast<uint64_t>(file_size) - kFooterSize -
            footer.bloom_offset;
        std::vector<uint8_t> bloom_data(bloom_len);
        if (::pread(fd, bloom_data.data(), bloom_len,
                    static_cast<off_t>(footer.bloom_offset)) !=
            static_cast<ssize_t>(bloom_len)) {
            ::close(fd);
            throw std::runtime_error("SSTable::open: cannot read bloom");
        }
        bloom = Bloom::load(bloom_data.data(), bloom_data.size());
    }

    int number = 0;
    try { number = std::stoi(std::filesystem::path(path).stem().string()); }
    catch (...) {}

    auto sst = std::shared_ptr<SSTable>(
        new SSTable(fd, path, number, footer, std::move(index), std::move(bloom)));
    sst->cache_ = std::move(cache);
    return sst;
}

bool SSTable::may_contain(std::string_view key) const {
    if (!bloom_) return true;
    return bloom_->may_contain(key);
}

ssize_t SSTable::find_block_idx(std::string_view key) const {
    if (index_.empty() || index_[0].first_key > key) return -1;
    size_t lo = 0, hi = index_.size();
    while (lo + 1 < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (index_[mid].first_key <= key)
            lo = mid;
        else
            hi = mid;
    }
    return static_cast<ssize_t>(lo);
}

std::shared_ptr<const std::vector<uint8_t>>
SSTable::read_block(size_t idx) const {
    const auto& ie = index_[idx];
    auto fmtver = footer_.format_version;
    auto loader = [&]() -> std::vector<uint8_t> {
        std::vector<uint8_t> buf(ie.length);
        ssize_t n = ::pread(fd_, buf.data(), ie.length,
                            static_cast<off_t>(ie.offset));
        if (n != static_cast<ssize_t>(ie.length)) buf.clear();
        if (fmtver >= 3 && buf.size() >= 4) {
            size_t payload_len = buf.size() - 4;
            uint32_t stored = get_u32_le(buf.data() + payload_len);
            uint32_t computed = crc32(buf.data(), payload_len);
            if (stored != computed)
                throw std::runtime_error("SSTable block checksum mismatch");
            buf.resize(payload_len);
        }
        return buf;
    };
    if (cache_)
        return cache_->get_or_load(number_, ie.offset, loader);
    return std::make_shared<const std::vector<uint8_t>>(loader());
}

SSTable::LookupResult SSTable::get(std::string_view key,
                                    uint64_t snapshot_seq) const {
    if (bloom_ && !bloom_->may_contain(key))
        return {LookupStatus::NotFound, {}};

    ssize_t bi = find_block_idx(key);
    if (bi < 0) return {LookupStatus::NotFound, {}};

    auto block = read_block(static_cast<size_t>(bi));
    if (!block || block->empty()) return {LookupStatus::NotFound, {}};

    size_t pos = 0;
    size_t entry_hdr = (footer_.format_version >= 2) ? 17 : 9;
    while (pos + entry_hdr <= block->size()) {
        uint8_t tag = (*block)[pos++];
        uint64_t seq = 0;
        if (footer_.format_version >= 2) {
            seq = get_u64_le(block->data() + pos);
            pos += 8;
        }
        uint32_t keylen = get_u32_le(block->data() + pos); pos += 4;
        uint32_t vallen = get_u32_le(block->data() + pos); pos += 4;
        if (pos + keylen + vallen > block->size()) break;

        std::string_view entry_key(
            reinterpret_cast<const char*>(block->data() + pos), keylen);
        pos += keylen;

        if (entry_key == key) {
            if (seq <= snapshot_seq) {
                if (tag == 1) return {LookupStatus::Deleted, {}};
                return {LookupStatus::Found,
                        std::string(reinterpret_cast<const char*>(
                                        block->data() + pos),
                                    vallen)};
            }
            pos += vallen;
            continue;
        }
        if (entry_key > key) return {LookupStatus::NotFound, {}};

        pos += vallen;
    }
    return {LookupStatus::NotFound, {}};
}

// ── Iterator ─────────────────────────────────────────────────────────────────

SSTable::Iterator::Iterator(const SSTable* t, size_t block_idx)
    : table_(t), block_idx_(block_idx), pos_(0), valid_(true) {
    load_block();
    if (valid_) parse_entry();
}

void SSTable::Iterator::load_block() {
    if (block_idx_ >= table_->index_.size()) {
        valid_ = false;
        return;
    }
    block_ = table_->read_block(block_idx_);
    if (!block_ || block_->empty()) {
        valid_ = false;
        return;
    }
    pos_ = 0;
}

void SSTable::Iterator::parse_entry() {
    if (!valid_) return;

    size_t entry_hdr = (table_->footer_.format_version >= 2) ? 17 : 9;
    if (pos_ + entry_hdr > block_->size()) {
        block_idx_++;
        load_block();
        if (!valid_) return;
    }

    uint8_t tag = (*block_)[pos_++];
    uint64_t seq = 0;
    if (table_->footer_.format_version >= 2) {
        seq = get_u64_le(block_->data() + pos_); pos_ += 8;
    }
    uint32_t keylen = get_u32_le(block_->data() + pos_); pos_ += 4;
    uint32_t vallen = get_u32_le(block_->data() + pos_); pos_ += 4;

    current_.key.assign(
        reinterpret_cast<const char*>(block_->data() + pos_), keylen);
    pos_ += keylen;
    current_.tombstone = (tag == 1);
    current_.seq = seq;
    if (vallen > 0)
        current_.value.assign(
            reinterpret_cast<const char*>(block_->data() + pos_), vallen);
    else
        current_.value.clear();
    pos_ += vallen;
}

SSTable::Iterator::Iterator(const SSTable* t, size_t block_idx,
                             std::string_view seek_target)
    : table_(t), block_idx_(block_idx), pos_(0), valid_(true) {
    load_block();
    if (!valid_) return;
    size_t entry_hdr = (table_->footer_.format_version >= 2) ? 17 : 9;
    while (valid_) {
        if (pos_ + entry_hdr > block_->size()) {
            block_idx_++;
            load_block();
            if (!valid_) return;
        }
        size_t entry_start = pos_;
        pos_++;  // tag
        if (table_->footer_.format_version >= 2) pos_ += 8; // skip seq
        uint32_t keylen = get_u32_le(block_->data() + pos_); pos_ += 4;
        uint32_t vallen = get_u32_le(block_->data() + pos_); pos_ += 4;
        if (pos_ + keylen + vallen > block_->size()) {
            valid_ = false;
            return;
        }
        std::string_view entry_key(
            reinterpret_cast<const char*>(block_->data() + pos_), keylen);
        if (entry_key >= seek_target) {
            pos_ = entry_start;
            parse_entry();
            return;
        }
        pos_ += keylen + vallen;
    }
}

SSTable::Iterator& SSTable::Iterator::operator++() {
    parse_entry();
    return *this;
}

SSTable::Iterator SSTable::begin() const {
    if (index_.empty()) return Iterator();
    return Iterator(this, 0);
}

SSTable::Iterator SSTable::end() const {
    return Iterator();
}

SSTable::Iterator SSTable::seek(std::string_view target) const {
    if (index_.empty()) return Iterator();
    ssize_t bi = find_block_idx(target);
    if (bi < 0) bi = 0;
    return Iterator(this, static_cast<size_t>(bi), target);
}

}  // namespace keystone

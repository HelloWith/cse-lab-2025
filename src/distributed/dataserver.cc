#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 1, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 1, true));
    auto version_buf = new u8[bm->block_size()];
    memset(version_buf, 0, bm->block_size());
    bm->write_block(0, version_buf).unwrap();
    delete[] version_buf;
    auto res = block_allocator_->allocate();
    if (res.is_err()) {
        throw std::runtime_error("Failed to allocate the first block");
    }
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(data_server_mutex_);
  auto buf_version = new u8[block_allocator_->bm->block_size()];
  auto res_version = block_allocator_->bm->read_block(0, buf_version);
  if (res_version.is_err()) {
    return {};
  }
  version_t block_version = *reinterpret_cast<version_t *>(buf_version + block_id * sizeof(version_t));
  delete[] buf_version;

  if (block_version != version) {
    return {};
  }

  auto buf = new u8[block_allocator_->bm->block_size()];
  auto res = block_allocator_->bm->read_block(block_id, buf);
  if (res.is_err()) {
    delete[] buf;
    return {};
  }
  std::vector<u8> data(buf + offset, buf + offset + len);
  delete[] buf;

  return data;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(data_server_mutex_);
  auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
  if (res.is_err()) {
    return false;
  }
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(data_server_mutex_);
  auto res = block_allocator_->allocate();
  if (res.is_err()) {
    return {};
  }
  auto block_id = res.unwrap();

  auto buf_version = new u8[block_allocator_->bm->block_size()];
  auto res_version = block_allocator_->bm->read_block(0, buf_version);
  if (res_version.is_err()) {
    return {};
  }
  version_t block_version = *reinterpret_cast<version_t *>(buf_version + block_id * sizeof(version_t));
  delete[] buf_version;
  block_version++;

  // Update version
  auto res_write = block_allocator_->bm->write_partial_block(0, reinterpret_cast<const u8 *>(&block_version), block_id * sizeof(version_t), sizeof(version_t));
  if (res_write.is_err()) {
    return {};
  }

  return {res.unwrap(), block_version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(data_server_mutex_);
  auto buf_version = new u8[block_allocator_->bm->block_size()];
  auto res_version = block_allocator_->bm->read_block(0, buf_version);
  if (res_version.is_err()) {
    delete[] buf_version;
    return false;
  }
  version_t block_version = *reinterpret_cast<version_t *>(buf_version + block_id * sizeof(version_t));
  delete[] buf_version;
  block_version++;

  auto res = block_allocator_->deallocate(block_id);
  if (res.is_err()) {
    return false;
  }

  // Update version
  auto res_write = block_allocator_->bm->write_partial_block(0, reinterpret_cast<const u8 *>(&block_version), block_id * sizeof(version_t), sizeof(version_t));
  if (res_write.is_err()) {
    return false;
  }

  return true;
}
} // namespace chfs
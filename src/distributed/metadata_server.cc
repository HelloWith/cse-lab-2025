#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  auto res = operation_->mk_helper(parent, name.c_str(), static_cast<InodeType>(type));
  if (res.is_err()) {
    return -1;
  }
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  return operation_->unlink(parent, name.c_str()).is_ok(); 
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  auto res = operation_->lookup(parent, name.c_str());
  if (res.is_err()) {
    return 0;
  }
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  auto content = operation_->read_file(id);
  if (content.is_err()) {
    return {};
  }
  auto raw_data = content.unwrap();
  std::vector<BlockInfo> map;
  if (!raw_data.empty()) {
    std::stringstream ss(std::string(raw_data.begin(), raw_data.end()));
    std::string line;
    while (std::getline(ss, line, '\n')) {
      if (line.empty()) continue;
      std::stringstream ls(line);
      std::string token;
      std::vector<u64> fields;
      while (std::getline(ls, token, ',')) {
        fields.push_back(std::stoull(token));
      }
      if (fields.size() >= 2) {
        block_id_t bid = fields[0];
        mac_id_t mid = fields[1];
        version_t ver =fields.size() > 2 ? fields[2] : 0;
        map.emplace_back(bid, mid, ver);
      }
    }
  }
  return map;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  auto type_res = operation_->gettype(id);
  if (type_res.is_err() || type_res.unwrap() != InodeType::FILE) {
    return {0, 0, 0};
  }

  if (num_data_servers == 0) {
    return {0, 0, 0};
  }
  mac_id_t machine_id = static_cast<mac_id_t>(generator.rand(1, num_data_servers));
  auto cli_it = clients_.find(machine_id);
  if (cli_it == clients_.end()) {
    return {0, 0, 0};
  }

  auto rpc_res = cli_it->second->call("alloc_block");
  if (rpc_res.is_err()) {
    return {0, 0, 0};
  }
  auto [block_id, version] = rpc_res.unwrap()->as<std::pair<block_id_t, version_t>>();

  // Update block map
  auto content = operation_->read_file(id);
  std::vector<BlockInfo> map;
  if (!content.is_err() && !content.unwrap().empty()) {
    // Parse existing map
    auto raw_data = content.unwrap();
    std::stringstream ss(std::string(raw_data.begin(), raw_data.end()));
    std::string line;
    while (std::getline(ss, line, '\n')) {
      if (line.empty()) continue;
      std::stringstream ls(line);
      std::string token;
      std::vector<u64> fields;
      while (std::getline(ls, token, ',')) {
        fields.push_back(std::stoull(token));
      }
      if (fields.size() >= 2) {
        map.emplace_back(fields[0], fields[1], fields.size() > 2 ? fields[2] : 0);
      }
    }
  }
  map.emplace_back(block_id, machine_id, version);

  std::stringstream ss;
  for (const auto &[bid, mid, ver] : map) {
    ss << bid << ',' << mid << ',' << ver << '\n';
  }
  std::string data = ss.str();
  std::vector<u8> serialized(data.begin(), data.end());
  operation_->write_file(id, serialized);

  return std::make_tuple(block_id, machine_id, version);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  auto cli_it = clients_.find(machine_id);
  if (cli_it == clients_.end()) {
    return false;
  }
  auto rpc_res = cli_it->second->call("free_block", block_id);
  if (rpc_res.is_err() || !rpc_res.unwrap()->as<bool>()) {
    return false;
  }

  // Update block map
  auto content = operation_->read_file(id);
  if (content.is_err()) {
    return false;
  }
  auto raw_data = content.unwrap();
  std::vector<BlockInfo> map;
  if (!raw_data.empty()) {
    std::stringstream ss(std::string(raw_data.begin(), raw_data.end()));
    std::string line;
    while (std::getline(ss, line, '\n')) {
      if (line.empty()) continue;
      std::stringstream ls(line);
      std::string token;
      std::vector<u64> fields;
      while (std::getline(ls, token, ',')) {
        fields.push_back(std::stoull(token));
      }
      if (fields.size() >= 2) {
        block_id_t bid = fields[0];
        mac_id_t mid = fields[1];
        if (bid != block_id || mid != machine_id) {
          map.emplace_back(bid, mid, fields.size() > 2 ? fields[2] : 0);
        }
      }
    }
  }

  std::stringstream ss;
  for (const auto &[bid, mid, ver] : map) {
    ss << bid << ',' << mid << ',' << ver << '\n';
  }
  std::string data = ss.str();
  std::vector<u8> serialized(data.begin(), data.end());
  return operation_->write_file(id, serialized).is_ok();
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  std::list<DirectoryEntry> list;
  auto res = read_directory(operation_.get(), node, list);
  if (res.is_err()) {
    return {};
  }
  std::vector<std::pair<std::string, inode_id_t>> result;
  for (const auto &entry : list) {
    result.emplace_back(entry.name, entry.id);
  }
  return result;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard<std::mutex> lock(metadata_server_mutex_);
  auto res = operation_->get_type_attr(id);
  if (res.is_err()) {
    return std::make_tuple(0, 0, 0, 0, 0);
  }
  auto [type, attr] = res.unwrap();
  return std::make_tuple(attr.size, attr.atime, attr.mtime, attr.ctime, static_cast<u8>(type));
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs
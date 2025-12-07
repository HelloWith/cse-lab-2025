#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (!metadata_server_) {
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  }
  auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if (res.is_err()) {
    auto error_code = res.unwrap_error();
    return ChfsResult<inode_id_t>(error_code);
  }
  auto inode_id = res.unwrap()->as<inode_id_t>();
  if (inode_id == -1) {
    return ChfsResult<inode_id_t>(ErrorType::INVALID);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (!metadata_server_) {
    return ChfsNullResult(ErrorType::BadResponse);
  }
  auto res = metadata_server_->call("unlink", parent, name);
  if (res.is_err()) {
    auto error_code = res.unwrap_error();
    return ChfsNullResult(error_code);
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (!metadata_server_) {
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  }
  auto res = metadata_server_->call("lookup", parent, name);
  if (res.is_err()) {
    auto error_code = res.unwrap_error();
    return ChfsResult<inode_id_t>(error_code);
  }
  auto inode_id = res.unwrap()->as<inode_id_t>();
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (!metadata_server_) {
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(ErrorType::BadResponse);
  }
  auto res = metadata_server_->call("readdir", id);
  if (res.is_err()) {
    auto error_code = res.unwrap_error();
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(error_code);
  }
  auto list = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(list);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (!metadata_server_) {
    return ChfsResult<std::pair<InodeType, FileAttr>>(ErrorType::BadResponse);
  }
  auto res = metadata_server_->call("get_type_attr", id);
  if (res.is_err()) {
    auto error_code = res.unwrap_error();
    return ChfsResult<std::pair<InodeType, FileAttr>>(error_code);
  }
  auto [size, atime, mtime, ctime, type_u8] = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  FileAttr attr;
  attr.size = size;
  attr.atime = atime;
  attr.mtime = mtime;
  attr.ctime = ctime;
  InodeType type = static_cast<InodeType>(type_u8);
  return ChfsResult<std::pair<InodeType, FileAttr>>({type, attr});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (!metadata_server_) {
    return ChfsResult<std::vector<u8>>(ErrorType::BadResponse);
  }
  // Get block map
  auto map_res = metadata_server_->call("get_block_map", id);
  if (map_res.is_err()) {
    auto error_code = map_res.unwrap_error();
    return ChfsResult<std::vector<u8>>(error_code);
  }
  std::vector<BlockInfo> block_map;
  try {
    block_map = map_res.unwrap()->as<std::vector<BlockInfo>>();
  } catch (const std::exception& e) {
    return ChfsResult<std::vector<u8>>(ErrorType::BadResponse);
  }

  if (size == 0) {
    return ChfsResult<std::vector<u8>>({});
  }
  
  // Get actual filesize and calculate actual readsize (Wrong... but why)
  // auto type_attr_res = this->get_type_attr(id);
  // if(type_attr_res.is_err()) {
  //   return ChfsResult<std::vector<u8>>(ErrorType::BadResponse);
  // }
  // auto [type, fileAttr] = type_attr_res.unwrap();
  // usize actual_size = (offset + size > fileAttr.size) ? (fileAttr.size - offset) : size;

  // Get result
  std::vector<u8> result;
  result.reserve(size);
  usize pos = offset;
  usize remaining = size;

  for (size_t index = offset / DiskBlockSize; index < block_map.size(); index++) {
    const auto &[block_id, mac_id, version] = block_map[index];
    if (remaining == 0) 
      break;
    auto cli_it = data_servers_.find(mac_id);
    if (cli_it == data_servers_.end()) {
      return ChfsResult<std::vector<u8>>(ErrorType::BadResponse);
    }

    usize block_off = pos % DiskBlockSize;
    usize chunk = std::min(DiskBlockSize - block_off, remaining);
     
    auto rpc_res = cli_it->second->call("read_data", block_id, block_off, chunk, version);
    if (rpc_res.is_err()) {
      auto error_code = rpc_res.unwrap_error();
      return ChfsResult<std::vector<u8>>(error_code);
    }
    auto slice = rpc_res.unwrap()->as<std::vector<u8>>();
    if (slice.size() != chunk) {
      return ChfsResult<std::vector<u8>>(ErrorType::INVALID);
    }

    result.insert(result.end(), slice.begin(), slice.end());
    pos += chunk;
    remaining -= chunk;
  }
  return ChfsResult<std::vector<u8>>(std::move(result));
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = DiskBlockSize;
  auto write_length = data.size();
  auto get_block_map_response = metadata_server_->call("get_block_map", id);
  if (get_block_map_response.is_err()) {
    auto error_code = get_block_map_response.unwrap_error();
    return ChfsNullResult(error_code);
  }
  auto block_info_vec = get_block_map_response.unwrap()->as<std::vector<chfs::BlockInfo>>();
  auto old_file_sz = block_info_vec.size() * BLOCK_SIZE;

  if (offset + write_length > old_file_sz) {
    auto new_block_num = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) / BLOCK_SIZE + 1) : ((offset + write_length) / BLOCK_SIZE);
    auto old_block_num = block_info_vec.size();
    for (auto i = old_block_num; i < new_block_num; ++i) {
      auto alloc_response = metadata_server_->call("alloc_block", id);
      if (alloc_response.is_err()) {
        auto error_code = alloc_response.unwrap_error();
        return ChfsNullResult(error_code);
      }
      auto new_block_info = alloc_response.unwrap()->as<BlockInfo>();
      block_info_vec.push_back(new_block_info);
    }
  }

  auto write_start_idx = offset / BLOCK_SIZE;
  auto write_start_offset = offset % BLOCK_SIZE;
  auto write_end_idx = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) / BLOCK_SIZE + 1) : ((offset + write_length) / BLOCK_SIZE);
  auto write_end_offset = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) % BLOCK_SIZE) : BLOCK_SIZE;

  usize current_offset = 0;
  for (auto it = block_info_vec.begin() + write_start_idx; it != block_info_vec.begin() + write_end_idx; ++it) {
    block_id_t block_id = std::get<0>(*it);
    mac_id_t mac_id = std::get<1>(*it);
    auto mac_it = data_servers_.find(mac_id);
    if (mac_it == data_servers_.end()) {
      return ChfsNullResult(ErrorType::INVALID_ARG);
    }
    auto target_mac = mac_it->second;
    std::vector<u8> write_buf;
    usize per_write_offset = 0;
    if (it == block_info_vec.begin() + write_start_idx && it == block_info_vec.begin() + (write_end_idx - 1)) {
      auto write_response = target_mac->call("write_data", block_id, write_start_offset, data);
      if (write_response.is_err()) {
        auto error_code = write_response.unwrap_error();
        return ChfsNullResult(error_code);
      }
      auto is_success = write_response.unwrap()->as<bool>();
      if (!is_success) {
        return ChfsNullResult(ErrorType::INVALID);
      }
      return KNullOk;
    }

    if (it == block_info_vec.begin() + write_start_idx) {
      write_buf.resize(BLOCK_SIZE - write_start_offset);
      std::copy_n(data.begin(), BLOCK_SIZE - write_start_offset, write_buf.begin());
      per_write_offset = write_start_offset;
      current_offset += BLOCK_SIZE - write_start_offset;
    } else if (it == block_info_vec.begin() + (write_end_idx - 1)) {
      write_buf.resize(write_end_offset);
      std::copy_n(data.begin() + current_offset, write_end_offset, write_buf.begin());
      per_write_offset = 0;
      current_offset += write_end_offset;
    } else {
      write_buf.resize(BLOCK_SIZE);
      std::copy_n(data.begin() + current_offset, BLOCK_SIZE, write_buf.begin());
      per_write_offset = 0;
      current_offset += BLOCK_SIZE;
    }

    auto write_response = target_mac->call("write_data", block_id, per_write_offset, write_buf);
    if (write_response.is_err()) {
      auto error_code = write_response.unwrap_error();
      return ChfsNullResult(error_code);
    }
    auto is_success = write_response.unwrap()->as<bool>();
    if (!is_success) {
      return ChfsNullResult(ErrorType::INVALID);
    }
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (!metadata_server_) {
    return ChfsNullResult(ErrorType::BadResponse);
  }
  auto res = metadata_server_->call("free_block", id, block_id, mac_id);
  if (res.is_err()) {
    auto error_code = res.unwrap_error();
    return ChfsNullResult(error_code);
  }
  return KNullOk;
}

} // namespace chfs
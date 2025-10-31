#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  if (!src.empty() && src.back() != '/') {
    src += '/';
  }

  src += filename + ':' + inode_id_to_string(id);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  list.clear();
  size_t start = 0;
  size_t end = 0;
  while (start < src.length()) {
    end = src.find('/', start);
    if (end == std::string::npos) {
      end = src.length();
    }
    
    std::string entry = src.substr(start, end - start);
    if (!entry.empty()) {
      size_t colon_pos = entry.find(':');
      if (colon_pos != std::string::npos) {
        DirectoryEntry dir_entry;
        dir_entry.name = entry.substr(0, colon_pos);
        std::string id_str = entry.substr(colon_pos + 1);
        dir_entry.id = string_to_inode_id(id_str);
        list.push_back(dir_entry);
      }
    }
    start = end + 1;
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  size_t pos = 0;
  size_t next_pos = 0;
  
  while (pos < src.length()) {
    next_pos = src.find('/', pos);
    if (next_pos == std::string::npos) {
      next_pos = src.length();
    }
    
    std::string entry = src.substr(pos, next_pos - pos);
    
    if (!entry.empty()) {
      size_t colon_pos = entry.find(':');
      if (colon_pos != std::string::npos) {
        std::string entry_name = entry.substr(0, colon_pos);
        
        if (entry_name != filename) {
          if (!res.empty()) {
            res += '/';
          }
          res += entry;
        }
      }
    }
    pos = next_pos + 1;
  }
  
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  auto content_res = fs->read_file(id);
  if (content_res.is_err()) {
    return ChfsNullResult(content_res.unwrap_error());
  }
  
  auto content = content_res.unwrap();
  if (content.empty()) {
    list.clear();
    return KNullOk;
  }
  
  std::string dir_content(content.begin(), content.end());
  
  parse_directory(dir_content, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  auto read_res = read_directory(this, id, list);
  if (read_res.is_err()) {
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  }
  
  std::string target_name(name);
  for (const auto &entry : list) {
    if (entry.name == target_name) {
      return ChfsResult<inode_id_t>(entry.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  auto lookup_res = this->lookup(id, name);
  if (lookup_res.is_ok()) {
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  // 2. Create the new inode.
  auto inode_res = this->alloc_inode(type);
  if (inode_res.is_err()) {
    return ChfsResult<inode_id_t>(inode_res.unwrap_error());
  }
  auto new_inode_id = inode_res.unwrap();
  if (type == InodeType::Directory) {
    auto write_res = this->write_file(new_inode_id, std::vector<u8>());
    if (write_res.is_err()) {
      this->remove_file(new_inode_id);
      return ChfsResult<inode_id_t>(write_res.unwrap_error());
    }
  }
  // 3. Append the new entry to the parent directory.
  auto parent_content_res = this->read_file(id);
  if (parent_content_res.is_err()) {
    this->remove_file(new_inode_id);
    return ChfsResult<inode_id_t>(parent_content_res.unwrap_error());
  }
  
  auto parent_content = parent_content_res.unwrap();
  std::string dir_content(parent_content.begin(), parent_content.end());
  
  std::string new_dir_content = append_to_directory(dir_content, name, new_inode_id);
  
  std::vector<u8> new_content(new_dir_content.begin(), new_dir_content.end());
  auto write_res = this->write_file(id, new_content);
  if (write_res.is_err()) {
    this->remove_file(new_inode_id);
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }
  return ChfsResult<inode_id_t>(new_inode_id);;
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  auto target_res = this->lookup(parent, name);
  if (target_res.is_err()) {
    return ChfsNullResult(ErrorType::NotExist); // ENOENT
  }
  auto target_id = target_res.unwrap();
  
  auto type_res = this->gettype(target_id);
  if (type_res.is_err()) {
    return ChfsNullResult(type_res.unwrap_error());
  }
  
  if (type_res.unwrap() == InodeType::Directory) {
    std::list<DirectoryEntry> entries;
    auto read_res = read_directory(this, target_id, entries);
    if (read_res.is_err()) {
      return ChfsNullResult(read_res.unwrap_error());
    }
    
    if (!entries.empty()) {
      return ChfsNullResult(ErrorType::NotEmpty); // ENOTEMPTY
    }
  }
  // 1. Remove the file, you can use the function `remove_file`
  auto remove_res = this->remove_file(target_id);
  if (remove_res.is_err()) {
    return remove_res;
  }
  // 2. Remove the entry from the directory.
  auto parent_content_res = this->read_file(parent);
  if (parent_content_res.is_err()) {
    return ChfsNullResult(parent_content_res.unwrap_error());
  }
  
  auto parent_content = parent_content_res.unwrap();
  std::string dir_content(parent_content.begin(), parent_content.end());
  
  std::string new_dir_content = rm_from_directory(dir_content, name);
  
  std::vector<u8> new_content(new_dir_content.begin(), new_dir_content.end());
  auto write_res = this->write_file(parent, new_content);
  if (write_res.is_err()) {
    return write_res;
  }
  
  return KNullOk;
}

} // namespace chfs

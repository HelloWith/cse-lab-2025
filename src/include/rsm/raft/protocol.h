#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
    MSGPACK_DEFINE(
        term,
        candidate_id,
        last_log_index,
        last_log_term
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int term;
    bool vote_granted;
    MSGPACK_DEFINE(
        term,
        vote_granted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<LogEntry<Command>> entries;  // 要存储的日志条目（心跳时为空）
    int leader_commit;

    // std::vector<u8> serialize_entries() const {
    //     std::vector<u8> data;
    //     for (const auto& entry : entries) {
    //         // 获取序列化后的数据
    //         std::vector<u8> entry_data = entry.serialize(entry.size());
    //         // 存储大小（4字节）
    //         u32 size = static_cast<u32>(entry_data.size());
    //         data.push_back((size >> 24) & 0xFF);
    //         data.push_back((size >> 16) & 0xFF);
    //         data.push_back((size >> 8) & 0xFF);
    //         data.push_back(size & 0xFF);
    //         // 存储数据
    //         data.insert(data.end(), entry_data.begin(), entry_data.end());
    //     }
    //     return data;
    // }
    
    // 反序列化方法
    // void deserialize_entries(const std::vector<u8>& data, int entries_count) {
    //     entries.clear();
    //     size_t pos = 0;
        
    //     for (int i = 0; i < entries_count && pos + 4 <= data.size(); i++) {
    //         // 读取大小
    //         u32 size = (static_cast<u32>(data[pos]) << 24) |
    //                   (static_cast<u32>(data[pos+1]) << 16) |
    //                   (static_cast<u32>(data[pos+2]) << 8) |
    //                   static_cast<u32>(data[pos+3]);
    //         pos += 4;
            
    //         // 检查是否有足够的数据
    //         if (pos + size > data.size()) {
    //             break;
    //         }
            
    //         // 提取数据
    //         std::vector<u8> entry_data(data.begin() + pos, data.begin() + pos + size);
    //         pos += size;
            
    //         // 反序列化为 Command
    //         Command cmd;
    //         cmd.deserialize(entry_data, size);
    //         entries.push_back(cmd);
    //     }
    // }
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<u8> entries_data;  // 序列化后的 entries
    int leader_commit;
    MSGPACK_DEFINE(
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries_data,
        leader_commit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    RpcAppendEntriesArgs rpc_arg;
    rpc_arg.term = arg.term;
    rpc_arg.leader_id = arg.leader_id;
    rpc_arg.prev_log_index = arg.prev_log_index;
    rpc_arg.prev_log_term = arg.prev_log_term;
    rpc_arg.leader_commit = arg.leader_commit;
    // rpc_arg.entries_count = static_cast<int>(arg.entries.size());
    // rpc_arg.entries_data = arg.serialize_entries();

    for (const auto &entry : arg.entries) {
        std::vector<u8> entry_data;
        int term = entry.term();
        entry_data.push_back((term >> 24) & 0xff);
        entry_data.push_back((term >> 16) & 0xff);
        entry_data.push_back((term >> 8) & 0xff);
        entry_data.push_back(term & 0xff);

        auto cmd_data = entry.command().serialize(entry.command().size());
        entry_data.insert(entry_data.end(), cmd_data.begin(), cmd_data.end());
        rpc_arg.entries_data.insert(rpc_arg.entries_data.end(), entry_data.begin(), entry_data.end());
    }

    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> arg;
    arg.term = rpc_arg.term;
    arg.leader_id = rpc_arg.leader_id;
    arg.prev_log_index = rpc_arg.prev_log_index;
    arg.prev_log_term = rpc_arg.prev_log_term;
    arg.leader_commit = rpc_arg.leader_commit;
    
    // 反序列化 entries
    // if (rpc_arg.entries_count > 0) {
    //     arg.deserialize_entries(rpc_arg.entries_data, rpc_arg.entries_count);
    // } else {
    //     arg.entries = std::vector<Command>();
    // }

    size_t cmd_size = Command().size();
    for (size_t i = 0; i < rpc_arg.entries_data.size(); i += cmd_size + 4) {
        int term = (rpc_arg.entries_data[i] << 24) | (rpc_arg.entries_data[i + 1] << 16) | (rpc_arg.entries_data[i + 2] << 8) | rpc_arg.entries_data[i + 3];
        Command command;
        command.deserialize(std::vector<u8>(rpc_arg.entries_data.begin() + i + 4, rpc_arg.entries_data.begin() + i + 4 + cmd_size), cmd_size);
        LogEntry<Command> entry(term, command);
        arg.entries.push_back(entry);
    }
    
    return arg;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;       // 当前任期，用于领导人更新自己
    bool success;   // 如果跟随者包含的日志条目和 prevLogIndex、prevLogTerm 匹配，则为 true
    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    // leader's term
    int term;
    // so follower can redirect clients
    int leader_id;
    // the snapshot replaces all entries up through and including this index
    int last_included_index;
    // term of lastIncludedIndex
    int last_included_term;
    // byte offset where chunk is positioned in the snapshot file
    int offset;
    // raw bytes of the snapshot chunk, starting at offset
    std::vector<u8> data;
    // true if this is the last chunk
    bool done;

    MSGPACK_DEFINE(
        term,
        leader_id,
        last_included_index,
        last_included_term,
        offset,
        data,
        done
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    // currentTerm, for leader to update itself
    int term;

    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */
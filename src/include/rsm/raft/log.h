#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include "filesystem/operations.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

const int max_inode_num = 16;
const int metadata_inode = 1;
const int log_inode = 2;
const int snapshot_inode = 3;

template <typename Command>
class LogEntry {
public:
    /* Lab3: Your code here 
       >> You may need this class for better implementation. */

    LogEntry() : term_(0), command_(Command()) {}

    LogEntry(int term, const Command &command) : term_(term), command_(command) {}

    LogEntry(std::vector<u8> data, int offset, int size)
    {
        // since the offset, the first 4 bytes are the term
        term_ = (data[offset] << 24) | (data[offset + 1] << 16) | (data[offset + 2] << 8) | data[offset + 3];
        // the rest of the data is the command
        command_.deserialize(std::vector<u8>(data.begin() + offset + 4, data.begin() + offset + 4 + size), size);
    }

    ~LogEntry() {}

    int term() const
    {
        return term_;
    }

    Command command() const
    {
        return command_;
    }

    size_t size() const
    {
        return 4 + command_.size();
    }

private:
    int term_;
    Command command_;
};


/** 
 * RaftLog uses a BlockManager to manage the data.
 */
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    /* Lab3: Your code here 
       >> Define helper functions for Part3, including log persistency and restoration. */

    RaftLog(std::shared_ptr<BlockManager> bm, bool is_recovery, int &current_term, int &voted_for, std::vector<LogEntry<Command>> &logs);

    // Save metadata and logs
    void save_all(const int &current_term, const int &voted_for, const std::vector<LogEntry<Command>> &logs) const;

    // Recover metadata and logs
    void recovery(int &current_term, int &voted_for, std::vector<LogEntry<Command>> &logs);

private:
    std::shared_ptr<BlockManager> bm_;
    mutable std::mutex mtx;

    /* Lab3: Your code here */
    std::shared_ptr<FileOperation> file_op_;

    // Save metadata(must called after lock)
    void save_metadata(const int &current_term, const int &voted_for) const;

    // Save logs(must called after lock)
    void save_logs(const std::vector<LogEntry<Command>> &logs) const;
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
    : bm_(bm)
{
    /* Lab3: Your code here */
}

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, bool is_recovery, int &current_term, int &voted_for, std::vector<LogEntry<Command>> &logs)
    : bm_(bm)
{
    // Append an empty log entry at the beginning
    logs.clear();
    logs.emplace_back(0, Command());

    if (is_recovery) {
        auto res = FileOperation::create_from_raw(bm_);
        if (res.is_err()) {
            std::cerr << "Failed to create FileOperation from raw" << std::endl;
            return;
        }
        file_op_ = res.unwrap();
        recovery(current_term, voted_for, logs);
        return;
    }

    file_op_.reset(new FileOperation(bm_, max_inode_num));
    auto meta_res = file_op_->alloc_inode(InodeType::FILE);
    if (meta_res.is_err() || (meta_res.unwrap() != 1)) {
        std::cerr << "Init meta's file Error!" << std::endl;
        return;
    }
    auto log_res = file_op_->alloc_inode(InodeType::FILE);
    if (log_res.is_err() || (log_res.unwrap() != 2)) {
        std::cerr << "Init log's file Error!" << std::endl;
        return;
    }

    current_term = 0;
    voted_for = -1;
    this->save_all(current_term, voted_for, logs);
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here 
   >> Add implementations of functions defined above. */

template <typename Command>
void RaftLog<Command>::save_all(const int &current_term, const int &voted_for, const std::vector<LogEntry<Command>> &logs) const
{
    // if I use mutex here, there will be a deadlock, but why...
    // std::lock_guard<std::mutex> lock(mtx);
    save_metadata(current_term, voted_for);
    save_logs(logs);
}

template <typename Command>
void RaftLog<Command>::save_metadata(const int &current_term, const int &voted_for) const
{
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<u8> data;
    data.push_back((current_term >> 24) & 0xff);
    data.push_back((current_term >> 16) & 0xff);
    data.push_back((current_term >> 8) & 0xff);
    data.push_back(current_term & 0xff);

    data.push_back((voted_for >> 24) & 0xff);
    data.push_back((voted_for >> 16) & 0xff);
    data.push_back((voted_for >> 8) & 0xff);
    data.push_back(voted_for & 0xff);
    file_op_->write_file(metadata_inode, data);
}

template <typename Command>
void RaftLog<Command>::save_logs(const std::vector<LogEntry<Command>> &logs) const
{
    std::vector<u8> data;
    for (const auto &entry : logs) {
        data.push_back((entry.term() >> 24) & 0xff);
        data.push_back((entry.term() >> 16) & 0xff);
        data.push_back((entry.term() >> 8) & 0xff);
        data.push_back(entry.term() & 0xff);

        auto command_data = entry.command().serialize(entry.command().size());
        data.insert(data.end(), command_data.begin(), command_data.end());
    }
    file_op_->write_file(log_inode, data);
}

template <typename Command>
void RaftLog<Command>::recovery(int &current_term, int &voted_for, std::vector<LogEntry<Command>> &logs)
{
    std::vector<u8> data;
    auto metadata_res = file_op_->read_file(metadata_inode);
    if (metadata_res.is_err()) {
        std::cerr << "Failed to read metadata" << std::endl;
    }
    data = metadata_res.unwrap();
    current_term = (data[0] << 24) | (data[1] << 16) | (data[2] << 8) | data[3];
    voted_for = (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7];


    logs.clear();
    data.clear();
    auto log_res = file_op_->read_file(log_inode);
    if (log_res.is_err()) {
        std::cerr << "Failed to read log" << std::endl;
    }
    data = log_res.unwrap();
    int offset = 0;
    while (offset < data.size()) {
        int term = (data[offset] << 24) | (data[offset + 1] << 16) | (data[offset + 2] << 8) | data[offset + 3];
        offset += 4;
        Command command;
        int command_size = command.size();
        command.deserialize(std::vector<u8>(data.begin() + offset, data.begin() + offset + command_size), command_size);
        logs.emplace_back(term, command);
        offset += command_size;
    }
}

} /* namespace chfs */

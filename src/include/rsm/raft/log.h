#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

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

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;

    /* Lab3: Your code here */

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    /* Lab3: Your code here */
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here 
   >> Add implementations of functions defined above. */

} /* namespace chfs */

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

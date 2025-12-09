#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */  
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here 
       >> Define any variable or helper function that is necessary. */

    // Timers
    std::chrono::steady_clock::time_point election_timer_start;
    std::chrono::steady_clock::time_point last_heartbeat_sent;
    int election_timeout_ms;        // randomly generated timeout time(150-300 ms)
    int heartbeat_timeout_ms;

    // Vote record
    int voted_for; /* Which candidate this node voted in current_term */
    int votes_received; /* Candidate ONLY, record how many votes this node recieved in current_term */

    // 日志相关
    std::vector<LogEntry<Command>> logs;    // 日志条目数组，索引从1开始
    int commit_idx;                        // 已知已提交的最高日志索引
    int last_applied;                        // 已应用到状态机的最高日志索引

    // Leader ONLY
    std::vector<int> next_idx;             // 对于每个服务器，下一个要发送的日志索引
    std::vector<int> match_idx;            // 对于每个服务器，已知已复制的最高日志索引

    // util functions

    /* Reset election_timer_start to NOW, and reset election_timeout_ms */
    void reset_election_timer();
    bool is_election_timeout() const;

    void reset_heartbeat_timer();
    bool is_heartbeat_timeout() const;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1),
    heartbeat_timeout_ms(30),
    voted_for(-1),
    votes_received(0),
    commit_idx(0),
    last_applied(0)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

    /* Lab3: Your code here */ 


    thread_pool = std::make_unique<ThreadPool>(4);
    reset_election_timer();
    reset_heartbeat_timer();

    logs.clear();
    logs.push_back(LogEntry<Command>());  // 添加一个空条目到索引 0
    // 现在 logs[0] 是一个空条目
    // logs[1] 将是第一个真正的日志条目

    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here
       >> You may refer to test files in order to get a better understanding of these interfaces. */
    std::lock_guard<std::mutex> lock(mtx);
    if (!is_stopped()) {
        RAFT_LOG("node already started");
        return 0;
    }

    RAFT_LOG("starting node...");
    stopped.store(false);

    try {
        // 创建客户端连接
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (size_t i = 0; i < node_configs.size(); i++) {
            if (!rpc_clients_map[i]) {
                const auto& config = node_configs[i];
                rpc_clients_map[i] = std::make_unique<RpcClient>(config.ip_address, config.port, true);
                RAFT_LOG("created RPC client for node %d", static_cast<int>(i));
            }
        }

        background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
        background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
        background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
        background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

        RAFT_LOG("successfully started node");
        return 0;
    } catch(const std::exception& e) {
        RAFT_LOG("Failed to start: %s", e.what());
        stop();
        return -1;
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    RAFT_LOG("stopping node...");
    stopped.store(true);

    if (background_election && background_election->joinable()) {
        background_election->join();
    }
    if (background_ping && background_ping->joinable()) {
        background_ping->join();
    }
    if (background_commit && background_commit->joinable()) {
        background_commit->join();
    }
    if (background_apply && background_apply->joinable()) {
        background_apply->join();
    }

    RAFT_LOG("successfully stopped node");
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(mtx);
    bool is_leader = (role == RaftRole::Leader);
    return std::make_tuple(is_leader, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(mtx);
    RAFT_LOG("received new command");

    if (role != RaftRole::Leader) {
        RAFT_LOG("this node is not Leader in current_term: %d", current_term);
        return std::make_tuple(false, current_term, -1);
    }

    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);

    // 创建日志条目
    LogEntry<Command> entry(current_term, cmd);
    logs.push_back(entry);
    int log_index = logs.size();  // 注意：索引从1开始

    // TODO: 持久化log
    
    return std::make_tuple(true, current_term, log_index);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(mtx);
    RAFT_LOG("receieve request_vote: candidateId=%d, term=%d, lastLogIndex=%d, lastLogTerm=%d", args.candidate_id, args.term, args.last_log_index, args.last_log_term);

    RequestVoteReply reply;
    
    // Rule1: Reply false if term < currentTerm
    if (args.term < current_term) {
        reply.term = current_term;
        reply.vote_granted = false;
        RAFT_LOG("reject request_vote: rule1: term(%d) < currentTerm(%d)", args.term, current_term);
        return reply;
    }

    // Whenever see a higher term, trans to Follower
    if (args.term > current_term) {
        RAFT_LOG("higher term found: %d > %d, trans to Follower", args.term, current_term);
        current_term = args.term;
        role = RaftRole::Follower;
        voted_for = -1;
        votes_received = 0;
        reset_election_timer();
        // 注意：Part 3需要在此持久化状态
    }

    // Check if voted
    bool already_voted = (voted_for != -1 && voted_for != args.candidate_id);
    if (already_voted) {
        reply.term = current_term;
        reply.vote_granted = false;
        RAFT_LOG("reject request_vote: already voted to %d in term %d", voted_for, current_term);
        return reply;
    }

    bool log_is_up_to_date = true; // TODO: 在后续阶段添加关于log的判断
    if (!log_is_up_to_date) {
        RAFT_LOG("reject request_vote: candidate's log is not up-to-date");
        reply.term = current_term;
        reply.vote_granted = false;
        return reply;
    }

    voted_for = args.candidate_id;
    reset_election_timer();
    
    reply.vote_granted = true;
    RAFT_LOG("grant vote to candidate %d for term %d", args.candidate_id, current_term);
    if (args.term == current_term && role != RaftRole::Follower) {
        RAFT_LOG("convert to Follower after granting vote");
        role = RaftRole::Follower;
        leader_id = -1;
        votes_received = 0;
    }

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(mtx);

    if (is_stopped()) {
        return;
    }

    // If reply.term is higher, trans to Follower
    if (reply.term > current_term) {
        RAFT_LOG("handle_request_vote_reply: higher reply.term %d > %d, trans to Follower", reply.term, current_term);
        current_term = reply.term;
        role = RaftRole::Follower;
        voted_for = -1;
        votes_received = 0;
        // election_in_progress = false;
        leader_id = -1;
        reset_election_timer();
        return;
    }

    if (role != RaftRole::Candidate) {
        return;
    }

    if (arg.term != current_term) {
        return;
    }

    if (reply.vote_granted) {
        votes_received++;
        RAFT_LOG("received vote from node %d, total votes: %d/%d", target, votes_received, static_cast<int>(node_configs.size()));
        
        // Check if get votes from majority
        int majority = (node_configs.size() / 2) + 1;
        if (votes_received >= majority) {
            RAFT_LOG("won election with %d votes, become Leader for term %d", votes_received, current_term);

            role = RaftRole::Leader;
            leader_id = my_id;
            // election_in_progress = false;
            
            // 重置选举计时器（Leader 不需要选举计时器）
            reset_election_timer();
            
            // 初始化 Leader 状态（Part 2 会用到）
            // nextIndex[] 和 matchIndex[] 初始化
            last_heartbeat_sent = std::chrono::steady_clock::now() - std::chrono::milliseconds(heartbeat_timeout_ms + 1);
            
            // 立即发送心跳，确立领导地位
            // 注意：这里需要小心死锁，因为我们在 lock 中
            // 更好的做法是设置一个标志，让 background_ping 线程发送
            RAFT_LOG("become Leader, will send initial heartbeats");
        }
    } else {
        RAFT_LOG("did not receive vote from node %d", target);
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here 
       >> You MAY need helper functions defined in protocol.h. */
    std::lock_guard<std::mutex> lock(mtx);

    AppendEntriesReply reply;
    reply.term = current_term;
    reply.success = false;

    auto arg = transform_rpc_append_entries_args<Command>(rpc_arg);
    RAFT_LOG("receive append_entries from leader %d, term=%d", arg.leader_id, arg.term);

    if (arg.term < current_term) {
        RAFT_LOG("reject: term %d < currentTerm %d", arg.term, current_term);
        reply.term = current_term;
        reply.success = false;
        return reply;
    }

    reset_election_timer();

    // 如果对方的任期更高，转为 follower
    if (arg.term > current_term) {
        RAFT_LOG("higher term %d > %d, convert to follower", arg.term, current_term);
        current_term = arg.term;
        role = RaftRole::Follower;
        voted_for = -1;
        votes_received = 0; 
        leader_id = arg.leader_id;
        reset_election_timer();
    }

    // 如果我是 Candidate，收到合法领导者的心跳，转为 follower
    if (role == RaftRole::Candidate && arg.term == current_term) {
        RAFT_LOG("candidate recieved heartbeat, trans to follower");
        role = RaftRole::Follower;
        voted_for = -1;
        votes_received = 0; 
        leader_id = arg.leader_id;
        reset_election_timer();
    }

    if (arg.term == current_term) {
        leader_id = arg.leader_id;
    }

    //TODO: 在后续阶段添加log ...DONE in part2

    // 2. 检查prevLogIndex和prevLogTerm是否匹配
    if (rpc_arg.prev_log_index > static_cast<int>(logs.size())) {
        // 日志缺失，返回false
        reply.term = current_term;
        reply.success = false;
        return reply;
    }
    
    if (rpc_arg.prev_log_index > 0) {
        // 检查term是否匹配
        if (logs[rpc_arg.prev_log_index - 1].term() != rpc_arg.prev_log_term) {
            // 不匹配，返回false
            return reply;
        }
    }
    
    // 3. 如果存在冲突，删除冲突条目
    int index = rpc_arg.prev_log_index;
    auto args = transform_rpc_append_entries_args<Command>(rpc_arg);
    
    // 检查冲突
    for (size_t i = 0; i < args.entries.size(); i++) {
        int log_idx = index + i + 1;  // +1 因为索引从1开始
        
        if (log_idx <= static_cast<int>(logs.size())) {
            if (logs[log_idx - 1].term() != args.entries[i].term()) {
                // 发现冲突，删除从这个位置开始的所有条目
                logs.resize(log_idx - 1);
                break;
            }
        }
    }
    
    // 4. 追加新条目
    for (size_t i = logs.size() - index; i < args.entries.size(); i++) {
        LogEntry<Command> entry(current_term, args.entries[i].command());
        logs.push_back(entry);
    }
    
    // 5. 更新commit_idx
    if (rpc_arg.leader_commit > commit_idx) {
        commit_idx = std::min(rpc_arg.leader_commit, static_cast<int>(logs.size()));
    }


    reply.term = current_term;
    reply.success = true;
    // RAFT_LOG("accept heartbeat from leader %d", leader_id);

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::lock_guard<std::mutex> lock(mtx);
    
    if (is_stopped()) {
        return;
    }

    // 如果回复的任期更高，转为 follower
    if (reply.term > current_term) {
        RAFT_LOG("higher term in append_entries reply: %d > %d, convert to follower", reply.term, current_term);
        current_term = reply.term;
        role = RaftRole::Follower;
        voted_for = -1;
        leader_id = -1;
        reset_election_timer();
    }

    // TODO: 实现其他逻辑
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    return InstallSnapshotReply();
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    RAFT_LOG("background election thread started");

    while (true) {
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
            std::lock_guard<std::mutex> lock(mtx);

            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */

            if (role == RaftRole::Leader) {
                continue;
            }
            
            if (is_election_timeout()) {
                RAFT_LOG("election timeout, become Candidate for term %d", current_term + 1);
            
                // 转换为 Candidate
                role = RaftRole::Candidate;
                current_term++;
                voted_for = my_id;       // 投给自己
                votes_received = 1;      // 自己的一票
                leader_id = -1;          // 没有领导者
            
                // 重置选举计时器
                reset_election_timer();
            
                // 创建投票请求
                RequestVoteArgs args;
                args.term = current_term;
                args.candidate_id = my_id;
                args.last_log_index = 0;    // Part 1: 没有日志
                args.last_log_term = 0;     // Part 1: 没有日志
            
                RAFT_LOG("starting election, sending RequestVote to all nodes");
            
                // 向所有其他节点发送投票请求（异步）
                for (size_t i = 0; i < node_configs.size(); i++) {
                    if (i == static_cast<size_t>(my_id)) continue;
                
                    // 使用线程池异步发送
                    thread_pool->enqueue([this, i, args]() {
                        send_request_vote(i, args);
                    });
                }
            }

        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    RAFT_LOG("background commit thread started");

    while (true) {
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
            std::lock_guard<std::mutex> lock(mtx);

            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            // TODO: 在后续实现
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    RAFT_LOG("background apply thread started");

    while (true) {
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
            std::lock_guard<std::mutex> lock(mtx);

            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            // TODO: 在后续实现
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    RAFT_LOG("background ping thread started");
    
    while (true) {
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
            std::lock_guard<std::mutex> lock(mtx);

            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */

            if (role != RaftRole::Leader) {
                continue;
            }

            if (is_heartbeat_timeout()) {
                // 创建心跳参数
                AppendEntriesArgs<Command> args;
                args.term = current_term;
                args.leader_id = my_id;
                args.prev_log_index = 0;     // Part 1: 没有日志
                args.prev_log_term = 0;      // Part 1: 没有日志
                args.entries = std::vector<LogEntry<Command>>();  // 空列表（心跳）
                args.leader_commit = 0;      // Part 1: 没有提交
            
                RAFT_LOG("sending heartbeat to followers");
            
                // 向所有跟随者发送心跳
                for (size_t i = 0; i < node_configs.size(); i++) {
                    if (i == static_cast<size_t>(my_id)) continue;
                
                    thread_pool->enqueue([this, i, args]() {
                        send_append_entries(i, args);
                    });
                }
            
                // 更新最后发送心跳的时间
                reset_heartbeat_timer();
            }
        }
    }

    return;
}



/******************************************************************

                          Util Functions (defined by self)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::reset_election_timer() {
    election_timer_start = std::chrono::steady_clock::now();
    election_timeout_ms = 150 + (std::rand() % 151);
}

template <typename StateMachine, typename Command>
bool RaftNode<StateMachine, Command>::is_election_timeout() const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - election_timer_start
    );
    return elapsed.count() >= election_timeout_ms;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::reset_heartbeat_timer() {
    last_heartbeat_sent = std::chrono::steady_clock::now();
}

template <typename StateMachine, typename Command>
bool RaftNode<StateMachine, Command>::is_heartbeat_timeout() const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_heartbeat_sent
    );
    return elapsed.count() >= heartbeat_timeout_ms;
}


/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}
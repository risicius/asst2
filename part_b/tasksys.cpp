#include "tasksys.h"
#include <thread>
#include <iostream>
#include <deque>
#include <atomic>
#include <mutex>
#include <condition_variable>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}
 
TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads_): ITaskSystem(num_threads_) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // std::cout << "constructor called" << std::endl;
    task_id = 0;
    num_threads = num_threads_;
    batch_idx = -1;
    keep_alive = true;
    waiting_for_batch = false;
    num_idle = 0;
    for (int i = 0; i < num_threads; ++i)
        pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::sleeper, this, i);
    // std::cout << "all threads created" << std::endl;
}

void TaskSystemParallelThreadPoolSleeping::sleeper(int tid) {   
    while (keep_alive) {
        try{
            if (batch_idx >= 0) {
                batches[batch_idx].next_task();
            } else {
                throw std::out_of_range("no batch in queue");
            }
        } catch ( ... ) {
            std::unique_lock<std::mutex> lk(mtx);
            batch_lk.notify_all();
            ++num_idle;
            // std::cout << "num_idle = " << num_idle << std::endl;
            if (num_idle == num_threads and batch_q.empty())
                sync_lk.notify_all();
            if (not waiting_for_batch) {
                next_batch(lk, tid);
            } else {
                // std::cout << tid << " chilling" << std::endl;
                worker_lk.wait(lk);
            }
            --num_idle;
            lk.unlock();
        }   
    }
}

void TaskSystemParallelThreadPoolSleeping::next_batch(std::unique_lock<std::mutex>& lk, int tid) {
    waiting_for_batch = true;
    while (1) {
        if (batch_q.empty()) {
            // std::cout << "num_idle = " << num_idle << std::endl;
            // std::cout << "no batches in the queue, waiting for more" << std::endl;
            // worker_lk.notify_all();
            batch_lk.wait(lk);
            if (not keep_alive)
                return;
            continue;
        }
        if (batch_idx >= 0 and not batches[batch_idx].all_deployed) { 
            // std::cout << batch_idx << std::endl;
            // std::cout << "all_d: " << batches[batch_idx].all_deployed << " t_s: " << batches[batch_idx].tasks_started << std::endl;
            // std::cout << "DOUBLE FUCK from tid " << tid << std::endl;
            worker_lk.notify_all();
            batch_lk.wait(lk);
        } else {
            break;
        }
    }
    int tmp_idx, i = 0;
    while (1) {
        if (batch_idx >= 0 and not batches[batch_idx].all_deployed) { 
            // std::cout << batch_idx << std::endl;
            // std::cout << "all_d: " << batches[batch_idx].all_deployed << " t_s: " << batches[batch_idx].tasks_started << std::endl;
            // std::cout << "DOUBLE FUCK from tid " << tid << std::endl;
            worker_lk.notify_all();
            batch_lk.wait(lk);
        } else if (i < batch_q.size()) { // prevents endlessly cycling queue
            i++;
            // std::cout << "checking the queue" << std::endl;
            tmp_idx = batch_q.front();
            batch_q.pop_front();
            bool pong = false;
            for (TaskID tid_ : batches[tmp_idx].deps) {
                if (not batches[tid_].all_deployed) {
                    // std::cout << "moving it back" << std::endl;
                    batch_q.emplace_back(tmp_idx);
                    pong = true;
                    break;
                }
            }
            if (pong)
                continue;
            else
                break;
        } else {
            // std::cout << "waiting for batch at idx " << batch_idx << std::endl;
            // std::cout << "all_d: " << batches[batch_idx].all_deployed << " t_s: " << batches[batch_idx].tasks_started << std::endl;
            worker_lk.notify_all();
            batch_lk.wait(lk); // forfeit lock
            i = 0;
        }
    }
    batch_idx = tmp_idx;
    // std::cout << tid << "found new batch, back to work" << std::endl;
    waiting_for_batch = false;
    worker_lk.notify_all(); // forfeit lock
}

Batch::Batch(TaskID tid_, IRunnable* runnable_, int num_total_tasks_, const std::vector<TaskID>& deps_): 
    task_id(tid_), deps(deps_), runnable(runnable_), num_total_tasks(num_total_tasks_), tasks_done(0), tasks_started(0), all_deployed(false) { 
    }

Batch::Batch(Batch&& source): 
    task_id(source.task_id), deps(source.deps), runnable(source.runnable), num_total_tasks(source.num_total_tasks), 
    tasks_done(source.tasks_done.load()), tasks_started(source.tasks_started.load()), all_deployed(source.all_deployed.load()) {
    //    if (source.tasks_started != 0)
     //       std::cout << "WIERD SHIT" << std::endl;
    }

void Batch::next_task() {
    int local_cnt = tasks_started++;
    if (local_cnt < num_total_tasks) {
        runnable->runTask(local_cnt, num_total_tasks);
        // std::cout << "finished task " << local_cnt << std::endl;
        // if (local_cnt == num_total_tasks - 1)
            // std::cout << "all done" << std::endl;
    } else {
        // std::cout << "overflow" << std::endl;
        all_deployed = true;
        throw AllDone();
    }        
}
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // std::cout << "destructor called" << std::endl;
    keep_alive = false;
    worker_lk.notify_all();
    batch_lk.notify_all();
    for (auto& t : pool)
        t.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks_) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    runAsyncWithDeps(runnable, num_total_tasks_, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable_, int num_total_tasks_,
                                                    const std::vector<TaskID>& deps_) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    std::unique_lock<std::mutex> lk(mtx);
    int tid = task_id++;
    // std::cout << "recieved batch " << tid << std::endl;
    batches.emplace_back(tid, runnable_, num_total_tasks_, deps_);
    batch_q.push_back(tid);
    batch_lk.notify_all();
    // std::cout << "started work" << std::endl;
    return tid;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    // std::cout << "started synicing" << std::endl;
    std::unique_lock<std::mutex> lk(mtx);
    sync_lk.wait(lk);
    for (auto& b : batches) {
        if (not b.all_deployed) {
            std::cout << "batch nbr: "<< b.task_id << " failed" << std::endl;
            std::cout << "all_d: " << batches[b.task_id].all_deployed << " t_s: " << batches[b.task_id].tasks_started << std::endl;
        }
    }
    // std::cout << "last batch idx " << batch_idx << std::endl;
    // std::cout << "all_d: " << batches[batch_idx].all_deployed << " t_s: " << batches[batch_idx].tasks_started << std::endl;
    // std::cout << "i'm in sync!" << std::endl;

    return;
}

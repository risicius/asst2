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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads_): ITaskSystem(num_threads_) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads = num_threads_;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::worker() {
    int local_cnt;
    while (true) {
        local_cnt = task_cnt++;
        if (local_cnt >= num_total_tasks) break;
        runner->runTask(local_cnt, num_total_tasks);
    }
}
void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks_) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    runner = runnable;
    num_total_tasks = num_total_tasks_;
    std::atomic_init(&task_cnt, 0);
    std::vector<std::thread> pool;
    for (int i = 0; i < num_threads; ++i)
        pool.emplace_back(&TaskSystemParallelSpawn::worker, this);

    for (auto& t : pool)
        t.join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 *  
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads_): ITaskSystem(num_threads_) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    num_threads = num_threads_;
    spin = true;
    tasks_done = 0;
    tasks_started = 0;
    num_total_tasks = -1;
    run_no = 0;
    for (int i = 0; i < num_threads; ++i)
        pool.emplace_back(&TaskSystemParallelThreadPoolSpinning::spinner, this, i);
}

void TaskSystemParallelThreadPoolSpinning::spinner(int tid) {
    // std::unique_lock<std::mutex> lk(mtx);
    // std::cout << "thread " << tid << " started spinning" << std::endl;
    // lk.unlock();
    int local_cnt;
    while (spin) {
        if (tasks_started < num_total_tasks) {
            local_cnt = tasks_started++;
            if (local_cnt < num_total_tasks) {
                runner->runTask(local_cnt, num_total_tasks);
                ++tasks_done;
                // std::cout << "thread " << tid << " finished work " << local_cnt << std::endl;
            }
        }
    }
    // std::cout << "thread " << tid << " died of natural causes" << std::endl;
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // std::cout << "closing shop" << std::endl;
    std::unique_lock<std::mutex> lk(mtx);
    spin = false;
    lk.unlock();
    for (auto& t : pool)
        t.join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks_) {
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> lk(mtx);
    tasks_done = 0;
    tasks_started = 0;
    // std::cout << "run number " << ++run_no << "with total number of tasks = " <<  num_total_tasks_ << std::endl;
    runner = runnable;
    num_total_tasks = num_total_tasks_;
    lk.unlock();
    while (1)
        if (tasks_done  == num_total_tasks) {
            // std::cout << "all jobs done" << std::endl;
            num_total_tasks = -1;
            return;
        }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    std::cout << "calling spin async" <<std::endl;
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
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
    num_threads = num_threads_;
    keep_alive = true;
    num_total_tasks = -1;
    num_idle = 0;
    tasks_started = 0;
    for (int i = 0; i < num_threads; ++i)
        pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::sleeper, this, i);
}

void TaskSystemParallelThreadPoolSleeping::sleeper(int tid) {   
    int local_cnt;
    while (keep_alive) {
        if (tasks_started < num_total_tasks) {
            local_cnt = tasks_started++;
            if (local_cnt < num_total_tasks) {
                runner->runTask(local_cnt, num_total_tasks);
                ++tasks_done;
            }        
        } else {
            std::unique_lock<std::mutex> lk(mtx);
            ++num_idle;
            if (num_idle == num_threads and tasks_done >= num_total_tasks)
                main_lk.notify_all();
            worker_lk.wait(lk);
            --num_idle;
            lk.unlock();
        }            
    }
    // std::cout << "thread " << tid << " died of natural causes" << std::endl;
}


TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    std::unique_lock<std::mutex> lk(mtx);
    keep_alive = false;
    lk.unlock();
    worker_lk.notify_all();
    for (auto& t : pool)
        t.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks_) {
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    std::unique_lock<std::mutex> lk(mtx);
    runner = runnable;
    num_total_tasks = num_total_tasks_;
    tasks_done = 0;
    tasks_started = 0;
    worker_lk.notify_all();
    main_lk.wait(lk);
}


TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable_, int num_total_tasks_,
                                                    const std::vector<TaskID>& deps_) {
    // TODO: CS149 students will implement this method in Part B.
    return 1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // TODO: CS149 students will modify the implementation of this method in Part B.
    return;
}

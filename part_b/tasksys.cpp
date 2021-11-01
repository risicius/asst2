#include "tasksys.h"
#include <thread>
#include <iostream>
#include <deque>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads_): ITaskSystem(num_threads_) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads = num_threads_;
    batch_idx = -1;
    keep_alive.test_and_set();
    all_done.clear();
    waiting_for_batch.clear();
    for (int i = 0; i < num_threads; ++i)
        pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::sleeper, this, i);
    // std::cout << "all threads created" << std::endl;
}

void TaskSystemParallelThreadPoolSleeping::sleeper(int tid) {   
    std::unique_lock<std::mutex> lk(mtx);
    // std::cout << "hola from " << tid << std::endl;
    if (not waiting_for_batch.test()) {
        next_batch(lk);
    } else
        worker_lk.wait(lk); // maybe fix sporadic
    lk.unlock();
    while (keep_alive.test()) {
        switch (batches[batch_idx].next_task()) {
            case 0:
                all_done.test_and_set();
                worker_lk.notify_all();
            case 1:
                lk.lock();
                if (not waiting_for_batch.test()) {
                    //std::cout << tid << " gonna go look for a new batch" << std::endl;
                    next_batch(lk);
                    lk.unlock();
                    break;
                }
                lk.unlock();
            case 2: // go chill
                lk.lock();
                // std::cout << tid << " chillin" << std::endl;
                worker_lk.wait(lk);
                lk.unlock();
                break;
            default:
                continue;
        }
    }
    // std::cout << tid << " i'm dead" << std::endl;
}

void TaskSystemParallelThreadPoolSleeping::next_batch(std::unique_lock<std::mutex>& lk) {
check_again:
    if (batch_q.empty()) {
        //std::cout << "waiting for more batches" << std::endl;
        waiting_for_batch.test_and_set();
        batch_lk.wait(lk);
        if (not keep_alive.test())
            return;
        waiting_for_batch.clear();
        goto check_again;
    }
    int tmp_idx, i = 0;
try_again: // infinite loop?
    if (i++ < batch_q.size()) { // prevents endlessly cycling queue
        //std::cout << "checking the queue" << std::endl;
        tmp_idx = batch_q.front();
        batch_q.pop_front();
        for (TaskID tid_ : batches[tmp_idx].deps)
            if (not batches[tid_].all_deployed.test()) {
                //std::cout << "moving it back" << std::endl;
                batch_q.emplace_back(tmp_idx);
                goto try_again;
            }
    } else {
        //std::cout << "waiting for batch" << std::endl;
        worker_lk.wait(lk); // forfeit lock
        i = 0;
        goto try_again;
    }
    batch_idx = tmp_idx;
    //std::cout << "found new batch, back to work" << std::endl;
    worker_lk.notify_all(); // forfeit lock
}

Batch::Batch(TaskID tid_, IRunnable* runnable_, int num_total_tasks_, const std::vector<TaskID>& deps_): 
    task_id(tid_), runnable(runnable_), num_total_tasks(num_total_tasks_), deps(deps_) { 
        tasks_done = 0;
        tasks_started = 0;
        all_done.clear();
        all_deployed.clear();
    }
Batch::Batch(Batch&& source): 
    task_id(source.task_id), runnable(source.runnable), num_total_tasks(source.num_total_tasks), deps(source.deps),
    tasks_done(0), tasks_started(0) {
        all_done.clear();
        all_deployed.clear();
    }

int Batch::next_task() {
    int local_cnt = tasks_started++;
    if (local_cnt < num_total_tasks) {
        runnable->runTask(local_cnt, num_total_tasks);
        // std::cout << "finished task " << local_cnt << std::endl;
        if (++tasks_done == num_total_tasks) {
            all_done.test_and_set();
            // std::cout << "all tasks done" << std::endl;
            return 0;
        }
        return -1;
    } else if (local_cnt == num_total_tasks) {
        all_deployed.test_and_set();
        // std::cout << "all tasks deployed" << std::endl;
        return 1; // fetch the next batch, only returned once from each batch
    } else {
        // std::cout << "go chill" << std::endl;
        return 2; // no more work for here to be done
    }
}
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    keep_alive.clear();
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
    all_done.clear();
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
    while (not (all_done.test() and waiting_for_batch.test()));
    // std::cout << "i'm in sync!" << std::endl;
    return;
}

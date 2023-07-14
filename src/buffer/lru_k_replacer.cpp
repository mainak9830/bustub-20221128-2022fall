//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <bits/types/time_t.h>
#include <cstddef>
#include <ctime>
#include <limits>
#include "common/config.h"
#include <iostream>
#include <ostream>
#include <chrono>
#include <thread>
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {

}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    // std::cout << "Prev" << *frame_id << std::endl;
    time_t dist_max = -1;
    frame_id_t &f = *frame_id;
    bool flag = false;
    time_t earliest_timestamp = std::numeric_limits<time_t>::max();

    for(auto& kv : access_history_){
        auto frame = kv.first;
        auto d = kv.second.size() == k_ ? kv.second[k_-1]-kv.second[0] : std::numeric_limits<time_t>::max();
        
        if(is_evicted_[frame]){
            flag = true;
            if(d > dist_max){
                
                dist_max = d;
                f = frame;
                earliest_timestamp = kv.second[0];

            }else if(d == dist_max){
                if(earliest_timestamp > kv.second[0]){
                    f = frame;
                    earliest_timestamp = kv.second[0];
                }
            }
            // std::cout << "distance " << d << " timestamp " << kv.second[0] << " frame " << frame << std::endl;
        }
    }
    
    if(flag){
        // std::cout << "Next << " << *frame_id << std::endl;
        access_history_.erase(*frame_id);
        is_evicted_.erase(*frame_id);
        curr_size_--;
        return true;
    }
    return false; 
     
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    //bring in the page
    if (access_history_.count(frame_id) == 0 && access_history_.size() == replacer_size_) {
        return;
    }
    if(access_history_.count(frame_id) == 0){
        is_evicted_[frame_id] = false;
    }
    // auto t = std::time(nullptr);
    auto current_time = std::chrono::system_clock::now();

    // Convert the current time point to milliseconds
    auto timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(current_time);
    
    // for(int i= 0;i < 10000000;i++){

    // }
    // Extract the number of milliseconds
    auto milliseconds = timestamp.time_since_epoch().count();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    access_history_[frame_id].push_back(milliseconds);
    
    if(access_history_[frame_id].size() > k_){
        auto it = access_history_[frame_id].begin();
        access_history_[frame_id].erase(it);
        
    }
    // std::cout << "Record Access : " << frame_id << " " << is_evicted_[frame_id] << std::endl;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);

    if(access_history_.count(frame_id) == 0){

        return;
    }

    if(is_evicted_[frame_id] && !set_evictable){
        // std::cout << "Turning back from evicted to non-evicted" << frame_id << std::endl;
        is_evicted_[frame_id] = set_evictable;
        curr_size_--;
    }else if(!is_evicted_[frame_id] && set_evictable){
        // std::cout << "Turning back from non-evicted to evicted" << frame_id << std::endl;
        is_evicted_[frame_id] = set_evictable;
        curr_size_++;
    }
    // std::cout << "Record Update: " << frame_id << " " << is_evicted_[frame_id] << std::endl;
    // std::cout << curr_size_ << std::endl;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if(access_history_.count(frame_id) == 0 || !is_evicted_[frame_id]){
        return;
    }

    access_history_.erase(frame_id);
    is_evicted_.erase(frame_id);
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { 
    std::scoped_lock<std::mutex> lock(latch_);
    // std::cout << "To be evicted "; 
    // for(auto it : is_evicted_){
    //     if(it.second){
    //         std::cout << it.first << " ";
    //     }
    // }
    // std::cout << "currsize " << curr_size_ << std::endl;
    return curr_size_;
    }

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "common/logger.h"

#include <iostream>

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      //Added now
      
      this->dir_.push_back(std::make_shared<Bucket>(Bucket (bucket_size_, global_depth_)));
      // this->dir_.push_back(std::make_shared<Bucket>(new Bucket (bucket_size_, global_depth_)));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  return GetNumBucketsInternal();
  
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t directory_id = IndexOf(key);
  
  return this->dir_[directory_id]->Find(key, value);

}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t directory_id = IndexOf(key);
  return this->dir_[directory_id]->Remove(key);
  
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  
  std::scoped_lock<std::mutex> lock(latch_);
  
  size_t directory_id = IndexOf(key);
  
  // if((this->dir_.size() >= directory_id) && (this->dir_[directory_id] != nullptr)){
  //   // this->dir_[directory_id]
  // }

  //try inserting, if already there then inserts
  //but if is full, returns false
  if(this->dir_[directory_id]->Insert(key, value)){
    // std::cout << key << std::endl;
    // LOG_DEBUG("iNSERTED %d using %d", getpid(), (int)directory_id);
    return;
  } 

  
  RedistributeBucket(directory_id, key, value);
  // std::cout << key << std::endl;
  // LOG_DEBUG("iNSERTED %d", getpid());


  // for(size_t i = 0;i < this->dir_.size();i++){
  //   std::cout << "Directory Index: " << i << " depth: " << this->dir_[i]->GetDepth() << " Keys :";

  //   for(auto &it : this->dir_[i]->GetItems()){
  //     std::cout << " (hash :" << IndexOf(it.first)<< " key: " << it.first << ") ";
  //   }
  //   std::cout << std::endl;
  // }
  
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(size_t directory_id, const K &key, const V &value) -> void{
  // 
  // get the list in that bucket and split
  if(this->dir_[directory_id]->GetDepth() == GetGlobalDepth()){
    
    size_t directory_other_id = directory_id + (1 << this->dir_[directory_id]->GetDepth());
    // LOG_DEBUG("EQUAL glOBAL dEPTH : %d", (int)GetGlobalDepth());
    // LOG_DEBUG(" equal Directory_id %d : other Directory id : %d", (int)directory_id, (int)directory_other_id);
    this->dir_[directory_id]->IncrementDepth();
    
      
    global_depth_++;
    
    this->dir_.resize(1 << global_depth_);
    
    this->dir_[directory_other_id] = std::make_shared<Bucket>(Bucket (bucket_size_, global_depth_));
    std::vector<std::pair<K,V>> refactor = {};
    for(const std::pair<K, V>& p : dir_[directory_id]->GetItems()){
      
      auto k = p.first;
      auto v = p.second;
      size_t newdirectory_id = IndexOf(k);
      if(directory_id != newdirectory_id){
        refactor.push_back({k,v});
      }
    }
    for(const std::pair<K, V>& p : refactor){
      auto k = p.first;
      auto v = p.second;
      dir_[directory_id]->Remove(k);
      dir_[directory_other_id]->Insert(k,v);
    }
    
    size_t d = GetGlobalDepth();
    for(size_t i = (1 << (d-1));i < (1 << d);i++){
      if(i == directory_other_id){
        continue;
      }
      size_t mask = i & ((1 << (d-1))-1);
      dir_[i] = dir_[mask];
    }
    
    // RedistributeBucket(directory_id, key, value);
  }else{
    size_t d = this->dir_[directory_id]->GetDepth();
    size_t local_directory_id = directory_id & ((1 << d)-1);
    size_t directory_other_id = local_directory_id + (1 << d);
    // LOG_DEBUG("glOBAL dEPTH : %d lOCAL dEPTH %d", (int)GetGlobalDepth(), (int)d);
    this->dir_[directory_id]->IncrementDepth();
    
    
    this->dir_[directory_other_id] = std::make_shared<Bucket>(Bucket (bucket_size_, this->dir_[directory_id]->GetDepth()));

    
    std::vector<std::pair<K,V>> refactor = {};
    for(const std::pair<K, V>& p : dir_[local_directory_id]->GetItems()){
      
      auto k = p.first;
      auto v = p.second;
      size_t newdirectory_id = IndexOf(k) & ((1 << this->dir_[directory_id]->GetDepth())-1);
      // std::cout << key << " " << newdirectory_id << " " << directory_other_id << std::endl;
      if(directory_other_id == newdirectory_id){
        
        refactor.push_back({k,v});
      }
    }
    for(const std::pair<K, V>& p : refactor){
      auto k = p.first;
      auto v = p.second;
      
      dir_[local_directory_id]->Remove(k);
      dir_[directory_other_id]->Insert(k,v);
    }
    //directory id 10 depth = 2
    //m 010 
    // 110
    //update pointers for the new bin
    size_t start = GetGlobalDepth()-this->dir_[directory_other_id]->GetDepth();
    for(size_t offset = 1;offset < (1 << start);offset++){
      dir_[directory_other_id+(offset << dir_[directory_other_id]->GetDepth())] = dir_[directory_other_id];
    }
    
    // RedistributeBucket(directory_id, key, value);
  }
   

  


  if(this->dir_[IndexOf(key)]->Insert(key, value)){
    return;
  }
  // std::cout << "Trying another iteration" << std::endl;
  RedistributeBucket(IndexOf(key), key, value);

}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {
  list_ = {};
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  
  // for(size_t i = 0;i < list_.size();i++){
  //   std::pair<K,V> p = list_.;
  //   if(p.first == key && p.second == value){
  //     return true;
  //   }
  // }
  for(auto it = list_.begin();it != list_.end();it++){
    if((*it).first == key){
      value = (*it).second;
      return true;
    }
  }
  // auto it = std::find(list_.begin(), list_.end(), std::pair{key, value});

  return false;
  
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  int flag = 0;
  
  for(auto it = list_.begin(); it != list_.end();){
    //  LOG_DEBUG("I am here %d", 34);
    if((*it).first == key){
    //  LOG_DEBUG("I am here %d", 54);
      it = list_.erase(it);
      // LOG_DEBUG("I am here %d", 84);
      flag = 1;
      continue;
    }
    it++;
  }
  // LOG_DEBUG("I am here successful");
  return flag == 1;
  
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  
  //key already present, remove the key and add a new key value
  
  Remove(key);
  //If removed, atleast one slot should be empty

  if(IsFull()){
    // std::cout << "Its full :" << key << std::endl;
    return false;
  }

  list_.push_back(std::make_pair(key, value));
  return true;
  
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
#include <ostream>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "iostream"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
/**
  * TODO(P1): Add implementation
  *
  * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
  * are currently in use and not evictable (in another word, pinned).
  *
  * You should pick the replacement frame from either the free list or the replacer (always find from the free list
  * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
  * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
  *
  * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
  * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
  * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
  *
  * @param[out] page_id id of created page
  * @return nullptr if no new pages could be created, otherwise pointer to new page
*/

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);
 
  frame_id_t frame_allocated;
  bool allocated = false;
  if(!free_list_.empty()){
    frame_allocated = *(free_list_.begin());
    // std::cout << "Hey frame is free " << frame_allocated << std::endl;
    free_list_.erase(free_list_.begin());
    allocated = true;
    
  }
  if(!allocated){
    
    if(replacer_->Evict(&frame_allocated)){
      allocated = true;
      
      // std::cout << "Hey frame is evicted " << frame_allocated << std::endl;
    }
    
  }
  if(!allocated){
    // std::cout << "Failed to get a frame" << std::endl;
    return nullptr;
  }

  Page& page_in_frame = pages_[frame_allocated];
  pages_[frame_allocated].RLatch(); 
  // use locks
  if(page_in_frame.IsDirty()){
    // std::cout << "Evicting dirty page " << page_in_frame.GetPageId() << " Data: " << page_in_frame.GetData() << std::endl;
    disk_manager_->WritePage(page_in_frame.GetPageId(), page_in_frame.GetData());
  }
  
  page_table_->Remove(page_in_frame.GetPageId());
  // std::cout << " Page id old assigned" << page_in_frame.GetPageId() << std::endl;
  replacer_->Remove(frame_allocated);

  
  
  
  
  page_in_frame.ResetMemory();
  
  page_in_frame.page_id_ = AllocatePage();
  // std::cout << " Page id new assigned" << page_in_frame.GetPageId() << std::endl;
  *page_id = page_in_frame.page_id_;
  page_in_frame.is_dirty_ = false; 
  page_in_frame.pin_count_ = 1;
  pages_[frame_allocated].RUnlatch();
  
  page_table_->Insert(*page_id, frame_allocated);
  
  replacer_->RecordAccess(frame_allocated);
  replacer_->SetEvictable(frame_allocated, false);
  

  // std::cout << page_in_frame. << std::endl;
  return &page_in_frame; 
  
}


/**
  * TODO(P1): Add implementation
  *
  * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
  * but all frames are currently in use and not evictable (in another word, pinned).
  *
  * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
  * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
  * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
  * to disk and update the metadata of the new page
  *
  * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
  *
  * @param page_id id of page to be fetched
  * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
  */

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id_mapped;
  // std::cout << " hey came for fetching " << page_id << std::endl;
  if(page_table_->Find(page_id, frame_id_mapped)){
    // std::cout << "Data is " << pages_[frame_id_mapped].GetData() << std::endl;
    pages_[frame_id_mapped].RLatch();
    pages_[frame_id_mapped].pin_count_++;
    pages_[frame_id_mapped].RUnlatch();
    return &pages_[frame_id_mapped];
  }
   
  frame_id_t frame_allocated;
  bool allocated = false;
  if(!free_list_.empty()){
    frame_allocated = *(free_list_.begin());
    
    free_list_.erase(free_list_.begin());
    allocated = true;
    
  }
  if(!allocated){
    
    if(replacer_->Evict(&frame_allocated)){
      allocated = true;
      
    }
    
  }
  if(!allocated){
    //  std::cout << " sending null" << page_id << std::endl;
    return nullptr;
  }

  Page& page_in_frame = pages_[frame_allocated];
  page_in_frame.RLatch();
  // use locks
  if(page_in_frame.IsDirty()){
    disk_manager_->WritePage(page_in_frame.GetPageId(), page_in_frame.GetData());
  }
  page_table_->Remove(page_in_frame.GetPageId());
  replacer_->Remove(frame_allocated);

  page_in_frame.ResetMemory();
  
  page_in_frame.page_id_ = page_id;
  page_in_frame.pin_count_ = 1;
  page_in_frame.is_dirty_ = false;
  
  disk_manager_->ReadPage(page_id, page_in_frame.data_);
  // std::cout << "Hey assigned " << page_in_frame.page_id_ << " data " << page_in_frame.data_ << std::endl;
  page_table_->Insert(page_id, frame_allocated);
  
  replacer_->RecordAccess(frame_allocated);
  replacer_->SetEvictable(frame_allocated, false);
  
  page_in_frame.RUnlatch(); 
  // std::cout << "Data is " << page_in_frame.GetData() << std::endl;
  return &page_in_frame; 

  
  }

/**
  * TODO(P1): Add implementation
  *
  * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
  * 0, return false.
  *
  * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
  * Also, set the dirty flag on the page to indicate if the page was modified.
  *
  * @param page_id id of page to be unpinned
  * @param is_dirty true if the page should be marked as dirty, false otherwise
  * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
  */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_alloted;
  if(!page_table_->Find(page_id, frame_alloted)){
    return false; 
  }

  if(pages_[frame_alloted].GetPinCount() <= 0){
    return false;
  }
  pages_[frame_alloted].RLatch();
  
  pages_[frame_alloted].pin_count_--;
  pages_[frame_alloted].is_dirty_ = is_dirty;

  if(pages_[frame_alloted].GetPinCount() == 0){
    replacer_->SetEvictable(frame_alloted, true);
  }
  // std::cout << "isdirty ? " << pages_[frame_alloted].IsDirty() << std::endl;
  pages_[frame_alloted].RUnlatch();
  return true;
}

/**
  * TODO(P1): Add implementation
  *
  * @brief Flush the target page to disk.
  *
  * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
  * Unset the dirty flag of the page after flushing.
  *
  * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
  * @return false if the page could not be found in the page table, true otherwise
  */

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_alloted;
  if(page_table_->Find(page_id, frame_alloted)){
    pages_[frame_alloted].RLatch();
    page_id_t page = pages_[frame_alloted].GetPageId();
    
    disk_manager_->WritePage(page, pages_[frame_alloted].GetData());
    pages_[frame_alloted].is_dirty_ = false;
    // std::cout << "Flushing dirty page " << pages_[frame_alloted].GetPageId() << " Data: " << pages_[frame_alloted].GetData() << std::endl;
    pages_[frame_alloted].RUnlatch();
    return true;
  }

  return false; 
  
  }

void BufferPoolManagerInstance::FlushAllPgsImp() {

  for(size_t i = 0;i < pool_size_;i++){
    if(pages_[i].IsDirty()){
      FlushPgImp(pages_[i].GetPageId());
    }
  }
}

/**
  * TODO(P1): Add implementation
  *
  * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
  * page is pinned and cannot be deleted, return false immediately.
  *
  * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
  * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
  * imitate freeing the page on the disk.
  *
  * @param page_id id of page to be deleted
  * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
  */

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_allocated;
  if(!page_table_->Find(page_id, frame_allocated)){
    return true;
  }
  Page& page = pages_[frame_allocated];

  if(page.GetPinCount() > 0){
    return false; 
  }

  page_table_->Remove(page_id);

  replacer_->Remove(frame_allocated);

  free_list_.push_back(frame_allocated);

  page.ResetMemory();

  page.is_dirty_ = true;

  page.page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);

  return true;
  
  }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { 
  // std::scoped_lock<std::mutex> lock(latch_);
  return next_page_id_++; 
  }

}  // namespace bustub

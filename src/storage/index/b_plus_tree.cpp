
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  //Editing
  return root_page_id_ == INVALID_PAGE_ID; 
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  
  auto node_page_id = root_page_id_;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* leaf_target;
  while(true){
    auto node_page = buffer_pool_manager_->FetchPage(node_page_id);
    auto node = reinterpret_cast<BPlusTreePage *>(node_page->GetData());

    if(node->IsLeafPage()){
      leaf_target = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node_page->GetData());
      break;
    }
    
    node_page_id = FindChildPageId(key, node_page);
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindChildPageId(const KeyType &key, Page* node_page) -> page_id_t {
  auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node_page->GetData());
  int i;
  for(i = 1;i <= internal_node->GetSize();i++){
    if(comparator_(internal_node->KeyAt(i), key) != -1){
      break;
    }
    
  }
  if(i == internal_node->GetSize()+1){
    return internal_node->ValueAt(i-1);
  }
  if(comparator_(internal_node->KeyAt(i), key) == 0){
    return internal_node->ValueAt(i);
  }

  return internal_node->ValueAt(i-1);;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(BPlusTreePage* node, const KeyType &key, BPlusTreePage* node_extra) 
-> bool {
 if(node->GetPageId() == root_page_id_){
  Page* root = buffer_pool_manager_->NewPage(&root_page_id_);
  auto new_root = new BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>(); 

  new_root->Init(root->GetPageId(), -1, internal_max_size_);
  new_root->SetValueAt(0, node->GetPageId());
  new_root->SetValueAt(0, node_extra->GetPageId());
  
  root_page_id_ = root->GetPageId();
  return true;
 }
 
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if(parent_node->GetSize() < parent_node->GetMaxSize()){
    int i;
    for(i = 0;i <= parent_node->GetSize();i++){
      if(parent_node->ValueAt(i) == node->GetParentPageId()){
        break;
      }
    }
    int j = parent_node->GetSize();
    while(j > i){

      parent_node->SetKeyAt(j+1, parent_node->KeyAt(j));
      parent_node->SetValueAt(j+1, parent_node->ValueAt(j));
      j--;
    }
    parent_node->SetKeyAt(i+1, key);
    parent_node->SetValueAt(i+1, node_extra->GetPageId());

  }
  else{
    parent_node->SetSize(parent_node->GetSize()+1);
    int i;
    for(i = 0;i <= parent_node->GetSize();i++){
      if(parent_node->ValueAt(i) == node->GetParentPageId()){
        break;
      }
    }

    int j = parent_node->GetSize();
    while(j > i){

      parent_node->SetKeyAt(j+1, parent_node->KeyAt(j));
      parent_node->SetValueAt(j+1, parent_node->ValueAt(j));
      j--;
    }
    parent_node->SetKeyAt(i+1, key);
    parent_node->SetValueAt(i+1, node_extra->GetPageId());

    Page* root = buffer_pool_manager_->NewPage(&root_page_id_); //Upin it later
    // Create L+1
    auto *parent_extra = new BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>();

    //Initialise the new leaf value
    parent_extra->Init(root->GetPageId(), parent_node->GetParentPageId(), leaf_max_size_+1);
    int h = (parent_node->GetSize()+1)/2;
    parent_node->SetSize(h-1);
    for(int i=h+1;i <= parent_extra->GetSize();i++){
      parent_extra->SetKeyAt(i, parent_extra->KeyAt(i));
    }
    parent_extra->SetSize(parent_extra->GetSize()/2);

    InsertInParent(parent_node, parent_node->KeyAt(h), parent_extra);
    
    
    }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(const KeyType &key, const ValueType &value, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* leaf_node) 
-> bool {

  if(leaf_node->GetSize() == 0 || comparator_(leaf_node->KeyAt(leaf_node->GetSize()), key) == -1){
    leaf_node->SetKeyAt(leaf_node->GetSize()+1, key);
    leaf_node->SetValueAt(leaf_node->GetSize()+1, value);
    return true;
  }
  int i;
  for(i = 1;i <= leaf_node->GetSize();i++){
    if(comparator_(leaf_node->KeyAt(i), key) != -1){
      break;
    }
    
  }
  if(comparator_(leaf_node->KeyAt(i), key) == 0){
    return false;
  }
  int j = leaf_node->GetSize();
  while(j >= i){

    leaf_node->SetKeyAt(j+1, leaf_node->KeyAt(j));
    leaf_node->SetValueAt(j+1, leaf_node->ValueAt(j));
    j--;
  }

  leaf_node->SetKeyAt(i, key);
  leaf_node->SetValueAt(i, value);

  
  return true;

}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if(root_page_id_ == INVALID_PAGE_ID){
    //create a new page
    Page* root = buffer_pool_manager_->NewPage(&root_page_id_);
    
    //create a new leaf page
    //apply root id, and reset
    auto *root_leaf = new BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>();

    //Initialise the new leaf value
    root_leaf->Init(root->GetPageId(), -1, leaf_max_size_);
    //To DO :: 
    //add the key and value for the leaf
    

    root_page_id_ = root->GetPageId();
    
    char *root_data [[maybe_unused]] = root->GetData();
    root_data = reinterpret_cast<char *>(root_leaf);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }

  auto node_page_id = root_page_id_;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* leaf_target;
  Page* node_page;
  while(true){
    node_page = buffer_pool_manager_->FetchPage(node_page_id);
    auto node = reinterpret_cast<BPlusTreePage *>(node_page->GetData());
    

    if(node->IsLeafPage()){
      leaf_target = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node_page->GetData());
      break;
    }
    buffer_pool_manager_->UnpinPage(node_page_id, false);
    node_page_id = FindChildPageId(key, node_page);
  }

  if(leaf_target->GetSize() < leaf_target->GetMaxSize()) {
    //Add in leaf
    if(!InsertInLeaf(key, value, leaf_target)){
      return false;
    }
    //Unpin page
    
    char *root_data [[maybe_unused]] = node_page->GetData();
    root_data = reinterpret_cast<char *>(leaf_target);
    buffer_pool_manager_->UnpinPage(node_page_id, true);
    return true;
  }

  Page* root = buffer_pool_manager_->NewPage(&root_page_id_); //Upin it later
  // Create L+1
  auto *leaf_overflow = new BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>();

  //Initialise the new leaf value
  leaf_overflow->Init(root->GetPageId(), leaf_target->GetParentPageId(), leaf_max_size_+1);
  
  for(int i = 1;i <= leaf_target->GetSize();i++){
    leaf_overflow->SetKeyAt(i, leaf_target->KeyAt(i));
    leaf_overflow->SetValueAt(i, leaf_target->ValueAt(i));
  }
  if(!InsertInLeaf(key, value, leaf_overflow)){
    return false;
  }
  int h = (leaf_overflow->GetSize()+1)/2;
  leaf_target->SetSize(h);
  for(int i=h+1;i <= leaf_overflow->GetSize();i++){
    leaf_overflow->SetKeyAt(i, leaf_overflow->KeyAt(i));
  }
  leaf_overflow->SetSize(leaf_overflow->GetSize()/2);

  
  leaf_overflow->SetNextPageId(leaf_target->GetNextPageId());
  leaf_target->SetNextPageId(leaf_overflow->GetPageId());


  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

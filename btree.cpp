/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{




BTreeIndex::BTreeIndex(const std::string & relationName,
        std::string & outIndexName,
        BufMgr *bufMgrIn,
        const int attrByteOffset,
        const Datatype attrType)
{

  bufMgr = bufMgrIn;
  leafOccupancy = INTARRAYLEAFSIZE;
  nodeOccupancy = INTARRAYNONLEAFSIZE;
  scanExecuting = false;


  std::ostringstream idxStr;
  idxStr << relationName << "." << attrByteOffset;
  outIndexName = idxStr.str();


  try
  {

    // File exists here

    file = new BlobFile(outIndexName, false);
    // read meta info
    headerPageNum = file->getFirstPageNo();
    Page *headerPage;
    bufMgr->readPage(file, headerPageNum, headerPage);
    IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
    rootPageNum = meta->rootPageNo;


    if (relationName != meta->relationName || attrType != meta->attrType 
      || attrByteOffset != meta->attrByteOffset)
    {
      throw BadIndexInfoException(outIndexName);
    }

    bufMgr->unPinPage(file, headerPageNum, false);    
  }
  

 
  catch(FileNotFoundException e)
  {

    // File does not exist in this case
    
    file = new BlobFile(outIndexName, true);
    

    // allocate root and header page
    Page *headerPage;
    Page *rootPage;
    bufMgr->allocPage(file, headerPageNum, headerPage);
    bufMgr->allocPage(file, rootPageNum, rootPage);

    // Assign our metapage attributes
    IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
    meta->attrByteOffset = attrByteOffset;
    meta->attrType = attrType;
    meta->rootPageNo = rootPageNum;
    meta->rootIsLeaf = true;


    // Store value of our root status to be easily reused
    isRootLeaf = meta->rootIsLeaf;



    strncpy((char *)(&(meta->relationName)), relationName.c_str(), 20);
    meta->relationName[19] = 0;



    // initiaize root
    LeafNodeInt *root = (LeafNodeInt *)rootPage;
    root->rightSibPageNo = 0;

    bufMgr->unPinPage(file, headerPageNum, true);
    bufMgr->unPinPage(file, rootPageNum, true);

    //fill the newly created Blob File using filescan
    FileScan fileScan(relationName, bufMgr);
    RecordId rid;
    try
    {
      while(1)
      {
        fileScan.scanNext(rid);
        std::string record = fileScan.getRecord();
        insertEntry(record.c_str() + attrByteOffset, rid);
      }
    }
    catch(EndOfFileException e)
    {
      


      bufMgr->flushFile(file);
    }
  }

}




BTreeIndex::~BTreeIndex()
{
this->bufMgr->flushFile(file);
    delete this->file;
    this->file = NULL;

}





void BTreeIndex::formNewRoot(PageId firstPageInRoot, PageKeyPair<int> *newchildEntry)
{
  // create a new root 
  PageId newRootPageNum;
  Page *newRoot;
  bufMgr->allocPage(file, newRootPageNum, newRoot);
  NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;




  if (isRootLeaf == true) {
    newRootPage->level = 1;
  } else {
    newRootPage->level = 0;
  }



  newRootPage->pageNoArray[0] = firstPageInRoot;
  newRootPage->pageNoArray[1] = newchildEntry->pageNo;
  newRootPage->keyArray[0] = newchildEntry->key;

  Page *meta;
  bufMgr->readPage(file, headerPageNum, meta);
  IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
  metaPage->rootPageNo = newRootPageNum;

  metaPage->rootIsLeaf = false;

  if(isRootLeaf){isRootLeaf = false;}


  rootPageNum = newRootPageNum;

  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, newRootPageNum, true);

}



void BTreeIndex::partitionInternalNode(NonLeafNodeInt *oldNode, PageId oldPageNum, PageKeyPair<int> *&newchildEntry)
{
  // allocate a new nonleaf node
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

  int mid = nodeOccupancy/2;
  int pushupIndex = mid;
  PageKeyPair<int> pushupEntry;
  if (nodeOccupancy % 2 == 0)
  {
    pushupIndex = newchildEntry->key < oldNode->keyArray[mid] ? mid -1 : mid;
  }
  pushupEntry.set(newPageNum, oldNode->keyArray[pushupIndex]);

  mid = pushupIndex + 1;
  // move half the entries to the new node



  for(int i = mid; i < nodeOccupancy; i++)
  {
    newNode->keyArray[i-mid] = oldNode->keyArray[i];
    newNode->pageNoArray[i-mid] = oldNode->pageNoArray[i+1];
    oldNode->pageNoArray[i+1] = (PageId) 0;
    oldNode->keyArray[i+1] = 0;
  }

  newNode->level = oldNode->level;


  oldNode->keyArray[pushupIndex] = 0;
  oldNode->pageNoArray[pushupIndex] = (PageId) 0;

  insertInternalNode(newchildEntry->key < newNode->keyArray[0] ? oldNode : newNode, newchildEntry);


  newchildEntry = &pushupEntry;
  bufMgr->unPinPage(file, oldPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if the curNode is the root
  if (oldPageNum == rootPageNum)
  {
    formNewRoot(oldPageNum, newchildEntry);
  }
}



void BTreeIndex::partitionLeaf(LeafNodeInt *leaf, PageId leafPageNum, PageKeyPair<int> *&newchildEntry, const RIDKeyPair<int> dataEntry)
{
  // allocate a new leaf page
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

  int mid = leafOccupancy/2;


  if (leafOccupancy %2 == 1 && dataEntry.key > leaf->keyArray[mid])
  {
    mid = mid + 1;
  }
  // copy half the page to newLeafNode
  for(int i = mid; i < leafOccupancy; i++)
  {
    newLeafNode->keyArray[i-mid] = leaf->keyArray[i];
    newLeafNode->ridArray[i-mid] = leaf->ridArray[i];
    leaf->keyArray[i] = 0;
    leaf->ridArray[i].page_number = 0;
  }
  
  if (dataEntry.key > leaf->keyArray[mid-1])
  {
    insertLeafNode(newLeafNode, dataEntry);
  }
  else
  {
    insertLeafNode(leaf, dataEntry);
  }

  // update sibling pointer
  newLeafNode->rightSibPageNo = leaf->rightSibPageNo;
  leaf->rightSibPageNo = newPageNum;

  // the smallest key from second page as the new child entry
  newchildEntry = new PageKeyPair<int>();
  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
  newchildEntry = &newKeyPair;
  bufMgr->unPinPage(file, leafPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if curr page is root
  if (leafPageNum == rootPageNum)
  {
    formNewRoot(leafPageNum, newchildEntry);
  }
}


void BTreeIndex::searchLevel(NonLeafNodeInt *curNode, PageId &nextNodeNum, int key)
{
  int i = nodeOccupancy;
  while(i >= 0 && (curNode->pageNoArray[i] == 0))
  {
    i--;
  }
  while(i > 0 && (curNode->keyArray[i-1] >= key))
  {
    i--;
  }
  nextNodeNum = curNode->pageNoArray[i];
}



void BTreeIndex::insertLeafNode(LeafNodeInt *leaf, RIDKeyPair<int> entry)
{
  // empty leaf page
  if (leaf->ridArray[0].page_number == 0)
  {
    leaf->keyArray[0] = entry.key;
    leaf->ridArray[0] = entry.rid;    
  }
  else
  {
    int i = leafOccupancy - 1;
    // find the end
    while(i >= 0 && (leaf->ridArray[i].page_number == 0))
    {
      i--;
    }
    // shift entry
    while(i >= 0 && (leaf->keyArray[i] > entry.key))
    {
      leaf->keyArray[i+1] = leaf->keyArray[i];
      leaf->ridArray[i+1] = leaf->ridArray[i];
      i--;
    }
    // insert entry
    leaf->keyArray[i+1] = entry.key;
    leaf->ridArray[i+1] = entry.rid;
  }
}

void BTreeIndex::insertInternalNode(NonLeafNodeInt *nonleaf, PageKeyPair<int> *entry)
{
  
  int i = nodeOccupancy;
  while(i >= 0 && (nonleaf->pageNoArray[i] == 0))
  {
    i--;
  }
  while( i > 0 && (nonleaf->keyArray[i-1] > entry->key))
  {
    nonleaf->keyArray[i] = nonleaf->keyArray[i-1];
    nonleaf->pageNoArray[i+1] = nonleaf->pageNoArray[i];
    i--;
  }

  nonleaf->keyArray[i] = entry->key;
  nonleaf->pageNoArray[i+1] = entry->pageNo;
}




void BTreeIndex::insertHelper(Page *curPage, PageId curPageNum, bool nodeIsLeaf, const RIDKeyPair<int> dataEntry, PageKeyPair<int> *&newchildEntry)
{

  // nonleaf node

  if (nodeIsLeaf)
  {
    
    LeafNodeInt *leaf = (LeafNodeInt *)curPage;
    if (leaf->ridArray[leafOccupancy - 1].page_number == 0) {
      insertLeafNode(leaf, dataEntry);
      bufMgr->unPinPage(file, curPageNum, true);
      newchildEntry = nullptr;
    } else{
      partitionLeaf(leaf, curPageNum, newchildEntry, dataEntry);
    }
  }
  else {
   NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    // find the right key to traverse
    Page *nextPage;
    PageId nextNodeNum;
    searchLevel(curNode, nextNodeNum, dataEntry.key);
    bufMgr->readPage(file, nextNodeNum, nextPage);
    // NonLeafNodeInt *nextNode = (NonLeafNodeInt *)nextPage;
    nodeIsLeaf = curNode->level == 1;
    insertHelper(nextPage, nextNodeNum, nodeIsLeaf, dataEntry, newchildEntry);
    
    // no split in child, just return
    if (newchildEntry == nullptr)
    {
        // unpin current page from call stack
        bufMgr->unPinPage(file, curPageNum, false);
    }
    else
      { 
      if (curNode->pageNoArray[nodeOccupancy] == 0)
      {
        insertInternalNode(curNode, newchildEntry);
        newchildEntry = nullptr;
        bufMgr->unPinPage(file, curPageNum, true);
      }
      else
      {
        partitionInternalNode(curNode, curPageNum, newchildEntry);
      }
    }
  }
}


void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  RIDKeyPair<int> dataEntry;
  dataEntry.set(rid, *((int *)key));
  // root
  Page* root;
  // PageId rootPageNum;
  bufMgr->readPage(file, rootPageNum, root);
  PageKeyPair<int> *newchildEntry = nullptr;


  insertHelper(root, rootPageNum, isRootLeaf, dataEntry, newchildEntry);
}





void BTreeIndex::findLeaf()//need to implement with new knowledge of how key array and pageNoArray work, if i = 0 for key and lowOP < key then pageNoArray = 0, otherwise if lowOP > key[i], < key[i+1] then 
{
    bufMgr->readPage(this->file, this->rootPageNum, this->currentPageData);
    this->currentPageNum = this->rootPageNum;
    NonLeafNodeInt* current = (NonLeafNodeInt*)(currentPageData);
    int prevLevel = current->level;
    do{
        for(int i = 0; i < sizeof(current->keyArray); i++){
            if(lowOp == GT){
                if(lowValInt < current->keyArray[i]){
                    int prevPageNum = currentPageNum;
                    currentPageNum = current->pageNoArray[i];
                    bufMgr->readPage(this->file, currentPageNum, currentPageData);
                    prevLevel = current->level;
                    bufMgr->unPinPage(this->file, prevPageNum, false);
                    current = (NonLeafNodeInt*)(currentPageData);
                    break;
                }
                if(current->keyArray[i] == 0 && i > 1 && lowValInt > current->keyArray[i]){
                    bufMgr->unPinPage(this->file, currentPageNum, false);
                    throw NoSuchKeyFoundException();
                }
            }
            if(lowOp == GTE){
                if(lowValInt <= current->keyArray[i]){
                    int prevPageNum = currentPageNum;
                    currentPageNum = current->pageNoArray[i];
                    bufMgr->readPage(this->file, currentPageNum, currentPageData);
                    prevLevel = current->level;
                    bufMgr->unPinPage(this->file, prevPageNum, false);
                    current = (NonLeafNodeInt*)(currentPageData);
                    break;
                }
                if(current->keyArray[i] == 0 && i > 1 && lowValInt > current->keyArray[i]){
                    bufMgr->unPinPage(this->file, currentPageNum, false);
                    throw NoSuchKeyFoundException();
                }
            }
        
        }
    }
    while(prevLevel != 1);
    for(int i = 0; i < sizeof(current->keyArray); i++){
      if(lowOp == GT){
        if(highOp == LT){
          if(current->keyArray[i] > lowValInt && current->keyArray[i] < highValInt){
            nextEntry = i;
            break;
          }
        }
        if(highOp == LTE){
          if(current->keyArray[i] > lowValInt && current->keyArray[i] <= highValInt){
            nextEntry = i;
            break;
          }
        } 
      }
      if(lowOp == GTE){
        if(highOp == LT){
          if(current->keyArray[i] >= lowValInt && current->keyArray[i] < highValInt){
            nextEntry = i;
            break;
          }
        }
        if(highOp == LTE){
          if(current->keyArray[i] >= lowValInt && current->keyArray[i] <= highValInt){
            nextEntry = i;
            break;
        }
      }
    }
  }
  
}


void BTreeIndex::startScan(const void* lowValParm,
           const Operator lowOpParm,
           const void* highValParm,
           const Operator highOpParm)
{
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;
  if(lowOp != GT){
    if(lowOp != GTE){
      throw BadOpcodesException();
    }
  }
  if(highOp != LT){
    if(highOp != LTE){
      throw BadOpcodesException();
    }
  }
  this->lowValInt = *(int*)(lowValParm);
  this->highValInt = *(int*)(highValParm);

  
  if(this->highValInt < this->lowValInt){ //Checking that the bounds are correct
    throw BadScanrangeException();
  }
  if(scanExecuting == true){ //Checks for an Existing Scan
    endScan();
  }
  scanExecuting = true; //Sets there to be a scan going
  Page* root = NULL;
  findLeaf();
  
}


void BTreeIndex::scanNext(RecordId& outRid) 
{
 if(!scanExecuting){ //Checking that scan has been initialized
    throw ScanNotInitializedException();
  }
  LeafNodeInt* currentPage = (LeafNodeInt*)(currentPageData);
    if(lowOp == GT){
    while(currentPage->keyArray[nextEntry] <= lowValInt){
      nextEntry++;
    }
  }
  if(lowOp == GTE){
    while(currentPage->keyArray[nextEntry] < lowValInt){
      nextEntry++;
    }
  }
  if(highOp == LT){
    if(currentPage->keyArray[nextEntry] < highValInt){
      outRid = currentPage->ridArray[nextEntry];
    }
    else{
      throw IndexScanCompletedException();
    }
  }
  if(highOp == LTE){
    if(currentPage->keyArray[nextEntry] <= highValInt){
      outRid = currentPage->ridArray[nextEntry];
    }
    else{
      throw IndexScanCompletedException();
    }
  }
  if(nextEntry > 0 && (currentPage->keyArray[nextEntry+1] == 0 && currentPage->keyArray[nextEntry + 2] == 0)){ //the next entry is on the right sibling changing the global variables accordingly
    nextEntry = 0;
    int prevPageNum = currentPageNum;
    currentPageNum = currentPage->rightSibPageNo;
    bufMgr->readPage(file, currentPage->rightSibPageNo, currentPageData);
    bufMgr->unPinPage(this->file, prevPageNum, true);
    
  }
  else{
    nextEntry++;
  }
}


void BTreeIndex::endScan() 
{
  if(!scanExecuting){ 
    throw ScanNotInitializedException();
    scanExecuting = false;
    return;
  }
  bufMgr->unPinPage(file, currentPageNum, true);
  scanExecuting = false;


}

}

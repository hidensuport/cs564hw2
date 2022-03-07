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

    this -> bufMgr = bufMgrIn;
    this -> attributeType = attrType;
    this -> attrByteOffset = attrByteOffset;
    
    // update the value of node occupancy and leaf node occupancy
    // this part of code is following the variable INTARRAYLEAFSIZE and
    // INTARRAYNONLEAFSIZE from the btree.h
    switch(attrType){
        case INTEGER:
            this -> nodeOccupancy = (Page::SIZE - sizeof(int) - sizeof(PageId)) / (sizeof(int) + sizeof(PageId));
            this -> leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(int) + sizeof(RecordId));
            break;
        case DOUBLE:
            this -> nodeOccupancy = (Page::SIZE - sizeof(double) - sizeof(PageId)) / (sizeof(double) + sizeof(PageId));
            this -> leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(double) + sizeof(RecordId));
            break;
        case STRING:
            // the size of string record is provided in the instruction file
            this -> nodeOccupancy = (Page::SIZE - sizeof(char[64]) - sizeof(PageId)) / (sizeof(char[64]) + sizeof(PageId));
            this -> leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(char[64]) + sizeof(RecordId));
            break;
    }

    // initialize all the variables for the scanning:
    this -> lowValInt = -1;
    this -> highValInt = -1;
    this -> lowValDouble = -1;
    this -> highValDouble = -1;
    this -> lowValString = "";
    this -> highValString = "";
    this -> scanExecuting = false;
    // we define the initial value of nextEntry as -1, which is always
    // an invalid value for index.
    // also, we specify the case where nextEntry = -2, to present the
    // case of scan complete.
    this -> nextEntry = -1;
    this -> currentPageNum = Page::INVALID_NUMBER;
    this -> currentPageData = NULL;
    // the enum variable usually starts value from 1. Thus, -1 is an
    // invalid value for enum variable
    this -> lowOp = (Operator)-1;
    this -> highOp = (Operator)-1;
    
    // find the index file name
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    // indexName is the name of the index file
    std::string indexName = idxStr.str();
    outIndexName = indexName;
    
    // check if the index file is already existing or not
    if(File::exists(indexName)){
        BlobFile* newfile = new BlobFile(outIndexName, false);
        File * file = (File*) newfile;
        // check whether the existing file matching with our input info
        // find the meta page, which is specified to be 1
        PageId metaPageId = 1;
        Page * metaPage;
        this -> bufMgr -> readPage(file, metaPageId, metaPage);
        IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;


        // check the attribute in the meta page
        if(!(metaInfo -> relationName == relationName
           && metaInfo -> attrByteOffset == attrByteOffset
           && metaInfo -> attrType == attrType)){
            this -> bufMgr -> unPinPage(file, metaPageId, false);
            throw BadIndexInfoException("Index file exists but values in metapage not match.");
        }
        this -> headerPageNum = metaPageId;
        this -> rootPageNum = metaInfo -> rootPageNo;
        this -> file = file;
        this -> bufMgr -> unPinPage(this -> file, metaPageId, false);
        return;
    }
    
    BlobFile* newFile = new BlobFile(outIndexName, true);
    this -> file = (File *) newFile;
    // create the metadata page
    PageId metaPageId;
    Page * metaPage;
    this -> bufMgr -> allocPage(this -> file, metaPageId, metaPage);
    IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
    // write the input attributes into the metaPage
    strcpy(metaInfo -> relationName, relationName.c_str());
    metaInfo -> attrByteOffset = attrByteOffset;
    metaInfo -> attrType = attrType;
    // assign the meta page id to the private attribute
    this -> headerPageNum = metaPageId;
    // create the root page and update the attribute for root page
    // update the root page attribute in the metaPage
    PageId rootPageId;
    Page * rootPage;
    this -> bufMgr -> allocPage(this -> file, rootPageId, rootPage);
    // initialize value of the vars in the rootPage, which is firstly
    // initialized as a leaf node.
  
    ((LeafNodeInt *) rootPage) -> rightSibPageNo = Page::INVALID_NUMBER;
    this -> bufMgr -> unPinPage(this -> file, rootPageId, true);
    metaInfo -> rootPageNo = rootPageId;
    this -> rootPageNum = rootPageId;
    // the initial index page for root is actually a leaf node, since
    // this is the only node as the start.
    metaInfo -> rootIsLeaf = true;
    this -> bufMgr -> unPinPage(this -> file, metaPageId, true);
    // scan the relation and insert into the index file
    FileScan * fileScan = new FileScan(relationName, bufMgrIn);
    try
    {
        RecordId scanRid;
        while(1)
        {
            fileScan -> scanNext(scanRid);
            std::string recordStr = fileScan -> getRecord();
            const char *record = recordStr.c_str();
            // as mentioned in the instruction, the data type of key
            // in this assignment will only be integer.
            int key = *((int *)(record + attrByteOffset));
            this -> insertEntry(&key, scanRid);
        }
    }
    catch(EndOfFileException& e)
    {
        // this case means reaching the end of the relation file.
        //std::cout << "Read all records" << std::endl;
    }
    delete fileScan;

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	scanExecuting = false;
  	bufMgr->flushFile(BTreeIndex::file);
  	delete file;
	file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------




   void BTreeIndex::searchThroughLeaf(const void *key, PageId & pid, PageId currentPageId, std::vector<PageId> & searchPath){
    Page * currPage;
    this->bufMgr->readPage(this -> file, currentPageId, currPage);
    NonLeafNodeInt * currNode = (NonLeafNodeInt *) currPage;

    int targetIndex = 0;
    int slotAvailable = sizeof(currNode -> keyArray) / sizeof(currNode -> keyArray[0]);
    
    while(targetIndex < slotAvailable){
        if(*((int *) key) < currNode -> keyArray[targetIndex]){
            break;
        }
        else{
            targetIndex++;
        }
    }
    PageId updateCurrPageNum = currNode -> pageNoArray[targetIndex];


    if(currNode -> level == 1){
        this -> bufMgr -> unPinPage(this -> file, currentPageId, false);

        searchPath.push_back(currentPageId);
       
        pid = updateCurrPageNum;
    }
    else{

        this -> bufMgr -> unPinPage(this -> file, currentPageId, false);
        searchPath.push_back(currentPageId);
        this -> searchThroughLeaf(key, pid, updateCurrPageNum, searchPath);
    }
}


   void BTreeIndex::insertLeafNode(const PageId pid, const void *key, const RecordId rid, std::vector<PageId> & searchPath){
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    LeafNodeInt * currLeafPage = (LeafNodeInt*) currPage;
    // check whether there is enough space to insert into this
    // current leaf index page

    int keySlots = sizeof(currLeafPage -> keyArray) / sizeof(currLeafPage -> keyArray[0]);
    
    if(keySlots < this -> leafOccupancy){
        // the case where there is still available space in this current
        // leaf index page
        // we may need to reorder the current array unit storing in this
        // leaf page, in order to make sure the keys in this leaf page
        // is sorted.
        int i;
        for(i = 0; i < keySlots; ++i){
            // shift all the slots with key value larger than this
            // target key into the slots upper by 1.
            if(currLeafPage -> keyArray[keySlots - 1 - i] >  *((int *) key)){
                currLeafPage -> keyArray[keySlots - i] = currLeafPage -> keyArray[keySlots - 1 - i];
                currLeafPage -> ridArray[keySlots - i] = currLeafPage -> ridArray[keySlots - 1 - i];
            }
          
            else{
                break;
            }
        }
        currLeafPage -> keyArray[keySlots - i] = *((int *) key);
        currLeafPage -> ridArray[keySlots - i] = rid;
      
        this -> bufMgr -> unPinPage(this -> file, pid, true);
        return;
    }
    else{
        // this is the case where there is no more space to insert in
        // a new (key, rid) pair in this current leaf page
        // unpin the current leaf page pinned in this function
        this -> bufMgr -> unPinPage(this -> file, pid, false);
        // we need to split this leaf page up into two parts
        this -> splitLeafNode(pid, key, rid, searchPath);
    }
}




   void BTreeIndex::splitLeafNode(PageId pid, const void *key,  const RecordId rid, std::vector<PageId> & searchPath){
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    LeafNodeInt * currLeafPage = (LeafNodeInt*) currPage;
    Page * newPage;
    PageId newPageId;
    this -> bufMgr -> allocPage(this -> file, newPageId, newPage);
    LeafNodeInt * newLeafPage = (LeafNodeInt*) newPage;
    // initialize the variable in this new leaf node

    int newLeafKeySlots = sizeof(newLeafPage -> keyArray)/sizeof(newLeafPage -> keyArray[0]);

    
    newLeafPage -> rightSibPageNo = currLeafPage -> rightSibPageNo;
    currLeafPage -> rightSibPageNo = newPageId;
    // we need to split this current leaf page up into two parts,
    // by the sizes of leafOccupancy / 2 and leafOccupancy / 2 + 1
    bool newKeyInserted = false; // var to keep track whether the new
    // key has been inserted or not.
    int threshold; // # of keys to split up the non-leaf node
    
    if(this -> leafOccupancy % 2 == 0){
        threshold = this -> leafOccupancy / 2;
    }
    else{
        threshold = this -> leafOccupancy / 2 + 1;
    }
    
    for(int i= 0; i < this -> leafOccupancy; ++i){
        if(currLeafPage -> keyArray[this -> leafOccupancy - 1 - i] > *((int *)key)){
            if(newLeafKeySlots <  this -> leafOccupancy + 1 - threshold){
                // this is the case that we should still insert into
                // the new leaf node
                newLeafPage -> keyArray[this -> leafOccupancy  + 1 - threshold - 1 - i] = currLeafPage -> keyArray[this -> leafOccupancy - 1 - i];
                newLeafPage -> ridArray[this -> leafOccupancy + 1 - threshold - 1 - i] = currLeafPage -> ridArray[this -> leafOccupancy - 1 - i];
	
            }
            else{
                // the new leaf has been filled up to the specified amount
                // already
                
                // shift up the slots in this current leaf page, to make
                // space for the new key value
                currLeafPage -> keyArray[this -> leafOccupancy - i] =  currLeafPage -> keyArray[this -> leafOccupancy - 1 - i];
                currLeafPage -> ridArray[this -> leafOccupancy - i] =  currLeafPage -> ridArray[this -> leafOccupancy - 1 - i];
               
                // this is the case where all the keys in the original current leaf are actually larger than the new inserted key. Then, as i is only in range of the amount of total amount of keys in the original current leaf, we never get a chance to insert the new key value.
                if(i + 1 == this -> leafOccupancy){
                    currLeafPage -> keyArray[this -> leafOccupancy - 1 - i] = *((int*) key);
                    currLeafPage -> ridArray[this -> leafOccupancy - 1 - i] = rid;
                    newKeyInserted = true;
                }
            }
        }
        else{
            if(newKeyInserted == false){
              
                if(newLeafKeySlots <  this -> leafOccupancy + 1 - threshold){
                    newLeafPage -> keyArray[this -> leafOccupancy + 1 - threshold - 1 - i] = *((int*)key);
                    newLeafPage -> ridArray[this -> leafOccupancy + 1 - threshold - 1 - i] = rid;
                    newKeyInserted = true;
                }
                else{
                   
                    
                    // the new key has to be put in the current leaf page
                    currLeafPage -> keyArray[this -> leafOccupancy - i] =  *((int*)key);
                    currLeafPage -> ridArray[this -> leafOccupancy - i] =  rid;
                    newKeyInserted = true;
                }
            }
          
            if(newLeafKeySlots <  this -> leafOccupancy + 1 - threshold){
                newLeafPage -> keyArray[this -> leafOccupancy + 1 - threshold - 2 - i] = currLeafPage -> keyArray[this -> leafOccupancy - i - 1];
                newLeafPage -> ridArray[this -> leafOccupancy + 1 - threshold - 2 - i] = currLeafPage -> ridArray[this -> leafOccupancy - i - 1];
             
            }
            else{
                
                
                break;
            }
        }
    }
    
    int pushup = newLeafPage -> keyArray[0]; // the key value needed to push up into the upper layer non-leaf node
    this -> bufMgr -> unPinPage(this -> file, pid, true);
    this -> bufMgr -> unPinPage(this -> file, newPageId, true);
    // check whether this current page is actually a root
    if(searchPath.size() == 0){
        // case when this current page is a root. Then, we need to
        // create a new non-leaf root.
        this -> formNewRoot(&pushup, pid, newPageId, 1);
    }
    else{
        // case when this current page is not a root.
        // get the upper level non-leaf parent node
        PageId parentId = searchPath[searchPath.size() - 1];
        // delete the parentId from the searchPath, to generate the search path for the parentId
        searchPath.erase(searchPath.begin() + searchPath.size() - 1);
        this -> insertInternalNode(parentId, &pushup, newPageId, searchPath, true);
    }
}








   void BTreeIndex::formNewRoot(const void *key, const PageId leftPageId, const PageId rightPageId, int level){
    PageId rootId;
    Page * rootPage;

    this -> bufMgr -> allocPage(this -> file, rootId, rootPage);
    NonLeafNodeInt * nonLeafRootPage = (NonLeafNodeInt*) rootPage;


    nonLeafRootPage -> level = level;
    nonLeafRootPage -> keyArray[0]= *((int*) key);
    nonLeafRootPage -> pageNoArray[0] = leftPageId;
    nonLeafRootPage -> pageNoArray[1] = rightPageId;


    
    this -> bufMgr -> unPinPage(this -> file, rootId, true);

    this -> rootPageNum = rootId;
    Page * metaPage;
    this -> bufMgr -> readPage(this -> file, this -> headerPageNum, metaPage);
    IndexMetaInfo * metaInfo = (IndexMetaInfo*) metaPage;
    metaInfo -> rootPageNo = rootId;
    
    metaInfo -> rootIsLeaf = false;
    // unpin this meta page
    this -> bufMgr -> unPinPage(this -> file, this -> headerPageNum, true);
}








   void BTreeIndex::insertInternalNode(PageId pid, const void *key, const PageId leftPageId, std::vector<PageId> searchPath, bool fromLeaf){
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    NonLeafNodeInt * currNonLeafPage = (NonLeafNodeInt*) currPage;
  
    int keySlotsOccupied = sizeof(currNonLeafPage -> keyArray)/sizeof(currNonLeafPage -> keyArray[0]);
    
    if(keySlotsOccupied < this -> nodeOccupancy){
       
        int i;
       
        currNonLeafPage -> pageNoArray[keySlotsOccupied + 1] =  currNonLeafPage -> pageNoArray[keySlotsOccupied];
        for(i = 0; i <  keySlotsOccupied; i++){
          
            if( currNonLeafPage -> keyArray[keySlotsOccupied - 1 - i] >  *((int *) key)){
                 currNonLeafPage -> keyArray[keySlotsOccupied - i] =  currNonLeafPage -> keyArray[keySlotsOccupied - 1 - i];
                 currNonLeafPage -> pageNoArray[keySlotsOccupied - i] =  currNonLeafPage -> pageNoArray[keySlotsOccupied - 1 - i];
            }
         
            else{
                break;
            }
        }
        if(fromLeaf == false){
         currNonLeafPage -> keyArray[keySlotsOccupied - i] = *((int *) key);
         currNonLeafPage -> pageNoArray[keySlotsOccupied - i] = leftPageId;
        }
        else{
           
            PageId CorrectLeftPageId = currNonLeafPage -> pageNoArray[keySlotsOccupied - i + 1];
            currNonLeafPage -> pageNoArray[keySlotsOccupied + 1] = leftPageId;
            currNonLeafPage -> keyArray[keySlotsOccupied - i] = *((int *) key);
            currNonLeafPage -> pageNoArray[keySlotsOccupied - i] = CorrectLeftPageId;
        }
     
        this -> bufMgr -> unPinPage(this -> file, pid, true);
        return; 
    }
    else{
  
        this -> bufMgr -> unPinPage(this -> file, pid, false);
        this -> splitInternalNode(pid, key, leftPageId, searchPath, fromLeaf);
    }
}








   void BTreeIndex::splitInternalNode(PageId pid, const void *key,  const PageId leftPageId, std::vector<PageId> & searchPath, bool fromLeaf){
    
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    NonLeafNodeInt * currNonLeafPage = (NonLeafNodeInt*) currPage;
    Page * newPage;
    PageId newPageId;
    this -> bufMgr -> allocPage(this -> file, newPageId, newPage);
    NonLeafNodeInt * newNonLeafPage = (NonLeafNodeInt*) newPage;
    // initialize the variable in this new leaf node

    int newKeySlots = sizeof(newNonLeafPage -> keyArray)/sizeof(newNonLeafPage -> keyArray[0]);
    newNonLeafPage -> level = currNonLeafPage -> level;
    // we need to split this current non-leaf page up into two parts,
    // by the sizes of nodeOccupancy / 2 and nodeOccupancy / 2 + 1
    bool newKeyInserted = false; // var to keep track whether the new
    // key has been inserted or not.
    int pushup; // the key value needed to push up into the upper layer non-leaf node
    int threshold; // # of keys to split up the non-leaf node
    if(this -> nodeOccupancy % 2 == 0){
        threshold = this -> nodeOccupancy / 2;
    }
    else{
        threshold = this -> nodeOccupancy / 2 + 1;
    }
    for(int i= 0; i < this -> nodeOccupancy; i++){
        if( currNonLeafPage -> keyArray[i] < *((int *)key)){
            if(newKeySlots < threshold){
                newNonLeafPage -> keyArray[i] =  currNonLeafPage -> keyArray[i];
                newNonLeafPage -> pageNoArray[i] =  currNonLeafPage -> pageNoArray[i];
            }
            
            else if(newKeySlots == threshold){
                // push up this key to break up the new non-leaf Node and the current non-leaf node.
                pushup =  currNonLeafPage -> keyArray[i];
                // Even though we don't copy or store the value of this key in any non-leaf node in this level, we cannot
                // lose the pageId that this key is corresponding to.
                // We will store the pageId corresponding to this pushup key at the end of the new non-leaf node.
                newNonLeafPage -> pageNoArray[newKeySlots] =  currNonLeafPage -> pageNoArray[i];
               
            }
            else{
                // shift down the rest slots in this current non-leaf page (there have newNonLeafPage -> slotTaken + 1 slots being removed from this current non-leaf page in the front already.)
                 currNonLeafPage -> keyArray[i - newKeySlots - 1] =   currNonLeafPage -> keyArray[i];
                 currNonLeafPage -> pageNoArray[i - newKeySlots - 1] =   currNonLeafPage -> pageNoArray[i];
            }
        }
        else{
            if(newKeyInserted == false){
                // this is the first time we meet an exisitng key
                // larger than the new key value.
                // We should insert in the new key first.
                if(newKeySlots < threshold){
                    newNonLeafPage -> keyArray[i] = *((int *)key);
                    // it is remarked the right pageId of this new key
                    // has not been changed and it should still be pointing to the slots that this new key used to belong to.
                    if(fromLeaf == false){
                        newNonLeafPage -> pageNoArray[i] = leftPageId;
                    }
                    else{
                        PageId trueLeftPageId = currNonLeafPage -> pageNoArray[i];
                        currNonLeafPage -> pageNoArray[i] = leftPageId;
                        newNonLeafPage -> pageNoArray[i] = trueLeftPageId;
                    }
                    newKeyInserted = true;
                }
              
                else if(newKeySlots == threshold){
                    // push up this key to break up the new non-leaf Node and the current non-leaf node.
                    pushup = *((int *)key);
                    // Even though we don't copy or store the value of this key in any non-leaf node in this level, we cannot
                    // lose the pageId that this key is corresponding to.
                    // We will store the pageId corresponding to this pushup key at the end of the new non-leaf node.
                    if(fromLeaf == false){
                        newNonLeafPage -> pageNoArray[newKeySlots] = leftPageId;
                    }
                    else{
                        PageId trueLeftPageId = currNonLeafPage -> pageNoArray[i];
                        currNonLeafPage -> pageNoArray[i] = leftPageId;
                        newNonLeafPage -> pageNoArray[newKeySlots] = trueLeftPageId;
                    }
                    newKeyInserted = true;
                }
                // this is the case where we have to insert this new key
                // into the current non-leaf page
                else{
                    // shift down the slots in this current non-leaf page
                    // in this case, there are  currNonLeafPage -> slotTaken + 1 amount of slots having been moved away from this current non-leaf page already.
                     currNonLeafPage -> keyArray[i - newKeySlots - 1] =  *((int*)key);
                    if(fromLeaf == false){
                     currNonLeafPage -> pageNoArray[i - newKeySlots - 1] =  leftPageId;
                    }
                    else{
                        PageId trueLeftPageId = currNonLeafPage -> pageNoArray[i];
                        currNonLeafPage -> pageNoArray[i] = leftPageId;
                        currNonLeafPage -> pageNoArray[i - newKeySlots - 1] =  trueLeftPageId;
                    }
                     
                    newKeyInserted = true;
                }
            }
            // this is the part under when the new key has been
            // inserted already
            // now, we have to re-position the original i-th slot of the current non-leaf node
            if(newKeySlots < threshold){
                newNonLeafPage -> keyArray[i+1] =  currNonLeafPage -> keyArray[i];
                newNonLeafPage -> pageNoArray[i+1] =  currNonLeafPage -> pageNoArray[i];
                
            }
         
            else if(newKeySlots == threshold){
                // push up this key to break up the new non-leaf Node and the current non-leaf node.
                pushup =  currNonLeafPage -> keyArray[i];
                // Even though we don't copy or store the value of this key in any non-leaf node in this level, we cannot
                // lose the pageId that this key is corresponding to.
                // We will store the pageId corresponding to this pushup key at the end of the new non-leaf node.
                newNonLeafPage -> pageNoArray[newKeySlots] =  currNonLeafPage -> pageNoArray[i];
             
            }
            else{
                // shift down the slots in this current non-leaf page
                // It is clear that there are exact newNonLeafPage -> slotTaken amount of slots being removed
                // away from this current non-leaf page.
                 currNonLeafPage -> keyArray[i - newKeySlots] =   currNonLeafPage -> keyArray[i];
                 currNonLeafPage -> pageNoArray[i - newKeySlots] =   currNonLeafPage -> pageNoArray[i];
            }
        }
    }
    
 

    int currKeySlots = sizeof(currNonLeafPage -> keyArray)/sizeof(currNonLeafPage -> keyArray[0]);
     currNonLeafPage -> pageNoArray[currKeySlots] =  currNonLeafPage -> pageNoArray[this -> nodeOccupancy];
    
    // another speical case is when all the keys in current non-leaf node are smaller than the new key.
    // Then, based on the current code, we have not inserted the new key yet.
    if(newKeyInserted == false){
        
       
        currNonLeafPage -> pageNoArray[currKeySlots] = *((int *) key);
        if(fromLeaf == false){
            // the pageId currently at the end of the list should be on the right of this key
            PageId trueRightPage = currNonLeafPage -> pageNoArray[ currKeySlots];
            currNonLeafPage -> pageNoArray[ currKeySlots] = leftPageId;
            currNonLeafPage -> pageNoArray[ currKeySlots + 1] = trueRightPage;
        }
        else{
            currNonLeafPage -> pageNoArray[ currKeySlots + 1] = leftPageId;
        }
        newKeyInserted = true;
    }
    
    this -> bufMgr -> unPinPage(this -> file, pid, true);
    this -> bufMgr -> unPinPage(this -> file, newPageId, true);
    // check whether this current page is actually a root
    if(searchPath.size() == 0){
        // case when this current page is a root. Then, we need to
        // create a new non-leaf root.
        this -> formNewRoot(&pushup, newPageId, pid, 0);
        // since this is a non-leaf node, then the nodes above this one
        // must be at level = 0 for sure.
    }
    else{
        // case when this current page is not a root.
        // get the upper level non-leaf parent node
        PageId parentId = searchPath[searchPath.size() - 1];
        // delete the parentId from the searchPath, to generate the search path for the parentId
        searchPath.erase(searchPath.begin() + searchPath.size() - 1);
        this -> insertInternalNode(parentId, &pushup, newPageId, searchPath, false);
    }
}













void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    std::vector<PageId> searchInPath;

    Page* root;

    this -> bufMgr -> readPage(file, rootPageNum, root);

    IndexMetaInfo * metaInfo = (IndexMetaInfo*) root;

    
    
    if(metaInfo -> rootIsLeaf == true){
       
       this -> insertLeafNode(this -> rootPageNum, key, rid, searchInPath);
	   this -> bufMgr -> unPinPage(file, rootPageNum, root);
    }
    else{
       
        PageId currPageId = Page::INVALID_NUMBER;
        this -> searchThroughLeaf(key, currPageId, this -> rootPageNum, searchInPath);
        // insert the (key, rid) pair into this potential leaf node


        this -> insertLeafNode(currPageId, key, rid, searchInPath);

	this -> bufMgr -> unPinPage(file, rootPageNum, root);
    }
}








// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	if(scanExecuting == true){ //Checks for an Existing Scan
		endScan();
	}
	scanExecuting = true; //Sets there to be a scan going
	Page* root = NULL;
	bufMgr->readPage(file, rootPageNum, root); //Gets the root page from the rootPageNum
	lowValInt = *(int*)(lowValParm);
	highValInt = *(int*)(highValParm);
	if(lowOpParm == GT){ //Checking that the OP codes are correct
		lowOp = GT;
	}
	else if(lowOpParm == GTE){
		lowOp = GTE;
	}
	else{
		throw BadOpcodesException();
	}
	if(highOpParm == LT){
		highOp = LT;
	}
	else if(highOpParm == LTE){
		highOp = LTE;
	}
	else{
		throw BadOpcodesException();
	}
	if(highValInt < lowValInt){ //Checking that the bounds are correct
		throw BadScanrangeException();
	}
	NonLeafNodeInt *currentNode = (NonLeafNodeInt*)(root);
    IndexMetaInfo * metaInfo = (IndexMetaInfo*)(root);
	if(metaInfo->rootIsLeaf){ //checking the root if it is a leaf
		LeafNodeInt *currentNodeIsLeaf = (LeafNodeInt*)(root);
		if(lowOp == GT){
			int index = -1;
			for(long unsigned int i = 0; i < sizeof(currentNodeIsLeaf->keyArray); i++){
				if(currentNodeIsLeaf->keyArray[i] > *(int*)(lowValParm)){
					index = i;
					break;
				}
			}
			if(highOp == LT){
				if(index != -1 && currentNodeIsLeaf->keyArray[index] < highValInt){
					return;
				}
				else{
					throw NoSuchKeyFoundException();
				}
			}
			if(highOp == LTE){
				if(index != -1 && currentNodeIsLeaf->keyArray[index] <= highValInt){
					return;
				}
				else{
					throw NoSuchKeyFoundException();
				}
			}
		}
		if(lowOp == GTE){
			int index = -1;
			for(long unsigned int i = 0; i < sizeof(currentNodeIsLeaf->keyArray); i++){
				if(currentNodeIsLeaf->keyArray[i] >= *(int*)(lowValParm)){
					index = i;
					break;
				}
			}
			if(highOp == LT){
				if(index != -1 && currentNodeIsLeaf->keyArray[index] < highValInt){
					currentPageNum = rootPageNum;
					bufMgr->readPage(file, rootPageNum, currentPageData);
					nextEntry = index;
					return;
				}
				else{
					throw NoSuchKeyFoundException();
				}
			}
			if(highOp == LTE){
				if(index != -1 && currentNodeIsLeaf->keyArray[index] <= highValInt){
					currentPageNum = rootPageNum;
					bufMgr->readPage(file, rootPageNum, currentPageData);
					nextEntry = index;
					return;
				}
				else{
					throw NoSuchKeyFoundException();
				}
			}
		}
	}
	/*while(currentNode->level != 1){ //Going through the tree and finding the correct node that is one level above the leaf node and marking every node it passes through to unPin later
        std::cout<<currentNode;
        if(lowOp == GT){
            Page* newPage = NULL;
			for(long unsigned int i = 0; i < sizeof(currentNode->keyArray) - 1; i++){
				if(lowValInt < currentNode->keyArray[i]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
                    *currentNode = *(NonLeafNodeInt*)(newPage);
				}
				if(lowValInt > currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
                    *currentNode = *(NonLeafNodeInt*)(newPage);
				}
				if(lowValInt > currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
                    *currentNode = *(NonLeafNodeInt*)(newPage);
				}
			}
		}
		if(lowOp == GTE){
			Page* newPage = NULL;
			for(long unsigned int i = 0; i < sizeof(currentNode->keyArray) - 1; i++){
				if(lowValInt < currentNode->keyArray[i]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
					*currentNode = *(NonLeafNodeInt*)(newPage);
				}
				if(lowValInt >= currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
					*currentNode = *(NonLeafNodeInt*)(newPage);
				}
				if(lowValInt >= currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
					*currentNode = *(NonLeafNodeInt*)(newPage);
				}
			}
		}
	}
	int leafPageNum = 0;
	if(lowOp == GT){ //figuring out which is the correct leaf node based on the OPCodes
		Page* newPage = NULL;
		for(long unsigned int i = 0; i < sizeof(currentNode->keyArray) - 1; i++){
			if(lowValInt < currentNode->keyArray[i]){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
				*currentNode = *(NonLeafNodeInt*)(newPage);
			}
			if(lowValInt > currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
				*currentNode = *(NonLeafNodeInt*)(newPage);
			}
			if(lowValInt > currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
				*currentNode = *(NonLeafNodeInt*)(newPage);
			}
		}
	}
	if(lowOp == GTE){
		Page* newPage = NULL;
		for(long unsigned int i = 0; i < sizeof(currentNode->keyArray) - 1; i++){
			if(lowValInt < currentNode->keyArray[i]){
					leafPageNum = currentNode->pageNoArray[i];
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
					*currentNode = *(NonLeafNodeInt*)(newPage);
			}
			if(lowValInt >= currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
				*currentNode = *(NonLeafNodeInt*)(newPage);
			}
			if(lowValInt >= currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
				*currentNode = *(NonLeafNodeInt*)(newPage);
			}
		}
	}
    */
    int leafPageNum = 0;
    while(currentNode->level != 1){
       if(lowOp == GT){
           Page* newPage;
           if(highOp == LT){
               for(long unsigned int i = 0; i < sizeof(currentNode->keyArray); i++){
                   if(currentNode -> keyArray[i] > lowValInt && currentNode -> keyArray[i] < highValInt){
                       leafPageNum = currentNode->pageNoArray[i];
                       pagesAccessed.push_back(currentNode->pageNoArray[i]);
                       bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
                       std::cout << currentNode << " with a new page of " << newPage << std::endl;
                       currentNode = (NonLeafNodeInt*)(newPage);
                   }
               }
           }
           if(highOp == LTE){
               for(long unsigned int i = 0; i < sizeof(currentNode->keyArray); i++){
                   if(currentNode -> keyArray[i] > lowValInt && currentNode -> keyArray[i] <= highValInt){
                       leafPageNum = currentNode->pageNoArray[i];
                       pagesAccessed.push_back(currentNode->pageNoArray[i]);
                       bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
                       std::cout << currentNode << " with a new page of " << newPage << std::endl;
                       currentNode = (NonLeafNodeInt*)(newPage);
                   }
               }
           }
       }
       if(lowOp == GTE){
           Page* newPage;
           if(highOp == LT){
               for(long unsigned int i = 0; i < sizeof(currentNode->keyArray); i++){
                   if(currentNode -> keyArray[i] >= lowValInt && currentNode -> keyArray[i] < highValInt){
                       leafPageNum = currentNode->pageNoArray[i];
                       pagesAccessed.push_back(currentNode->pageNoArray[i]);
                       bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
                       std::cout << currentNode << " with a new page of " << newPage << std::endl;
                       currentNode = (NonLeafNodeInt*)(newPage);
                   }
               }
           }
           if(highOp == LTE){
               for(long unsigned int i = 0; i < sizeof(currentNode->keyArray); i++){
                   if(currentNode -> keyArray[i] >= lowValInt && currentNode -> keyArray[i] <= highValInt){
                       leafPageNum = currentNode->pageNoArray[i];
                       pagesAccessed.push_back(currentNode->pageNoArray[i]);
                       bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
                       std::cout << currentNode << " with a new page of " << newPage << std::endl;
                       currentNode = (NonLeafNodeInt*)(newPage);
                   }
               }
           }
       }
   }
	int index = -1;
	for(long unsigned int i = 0; i < sizeof(currentNode->keyArray); i++){ //Making sure that the leaf contains a valid key
		if(lowOp == GT){
			if(highOp == LT){
				if(currentNode->keyArray[i] > lowValInt && currentNode->keyArray[i] < highValInt){
					index = i;
				}
			}
			if(highOp == LTE){
				if(currentNode->keyArray[i] > lowValInt && currentNode->keyArray[i] <= highValInt){
					index = i;
				}
			}
		}
		if(lowOp == GTE){
			if(highOp == LT){
				if(currentNode->keyArray[i] >= lowValInt && currentNode->keyArray[i] < highValInt){
					index = i;
				}
			}
			if(highOp == LTE){
				if(currentNode->keyArray[i] >= lowValInt && currentNode->keyArray[i] <= highValInt){
					index = i;
				}
			}
		}
	}
	if(index != -1){
		currentPageNum = leafPageNum;
		bufMgr->readPage(file, leafPageNum, currentPageData);
		nextEntry = index;
	}
	else{
		throw NoSuchKeyFoundException();
	}
    




}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(!scanExecuting){ //Checking that scan has been initialized
		throw ScanNotInitializedException();
	}
	LeafNodeInt* currentPage = (LeafNodeInt*)(currentPageData);
	if(highOp == LT){
		if(nextEntry < highValInt){
			outRid = currentPage->ridArray[nextEntry];
		}
		else{
			throw IndexScanCompletedException();
		}
	}
	if(highOp == LTE){
		if(nextEntry <= highValInt){
			outRid = currentPage->ridArray[nextEntry];
		}
		else{
			throw IndexScanCompletedException();
		}
	}
	if(nextEntry + 1 > sizeof(currentPage->keyArray)){ //the next entry is on the right sibling changing the global variables accordingly
		nextEntry = 0;
		currentPageNum = currentPage->rightSibPageNo;
		bufMgr->readPage(file, currentPage->rightSibPageNo, currentPageData);
		pagesAccessed.push_back(currentPageNum);
	}
	else{
		nextEntry++;
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	if(!scanExecuting){	
		throw ScanNotInitializedException();
		scanExecuting = false;
		return;
	}
	for(long unsigned int i = 0; i < pagesAccessed.size(); i++){
		bufMgr->unPinPage(file, pagesAccessed[i], true);
	}
	pagesAccessed.clear();
	bufMgr->unPinPage(file, rootPageNum, true);	//unpins the root which wasn't kept track of 

}

}

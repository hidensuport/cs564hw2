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

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

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

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

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
	if(rootIsLeaf){ //checking the root if it is a leaf
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
	while(currentNode->level != 1){ //Going through the tree and finding the correct node that is one level above the leaf node and marking every node it passes through to unPin later
		if(lowOp == GT){
			Page* newPage = NULL;
			for(long unsigned int i = 0; i < sizeof(currentNode->keyArray) - 1; i++){
				if(lowValInt < currentNode->keyArray[i]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
					currentNode = (NonLeafNodeInt*)(newPage);
				}
				if(lowValInt > currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
					currentNode = (NonLeafNodeInt*)(newPage);
				}
				if(lowValInt > currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
					currentNode = (NonLeafNodeInt*)(newPage);
				}
			}
		}
		if(lowOp == GTE){
			Page* newPage = NULL;
			for(long unsigned int i = 0; i < sizeof(currentNode->keyArray) - 1; i++){
				if(lowValInt < currentNode->keyArray[i]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i], newPage);
					currentNode = (NonLeafNodeInt*)(newPage);
				}
				if(lowValInt >= currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
					currentNode = (NonLeafNodeInt*)(newPage);
				}
				if(lowValInt >= currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
					pagesAccessed.push_back(currentNode->pageNoArray[i]);
					bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
					currentNode = (NonLeafNodeInt*)(newPage);
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
				currentNode = (NonLeafNodeInt*)(newPage);
			}
			if(lowValInt > currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
				currentNode = (NonLeafNodeInt*)(newPage);
			}
			if(lowValInt > currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
				currentNode = (NonLeafNodeInt*)(newPage);
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
					currentNode = (NonLeafNodeInt*)(newPage);
			}
			if(lowValInt >= currentNode->keyArray[i] && lowValInt < currentNode->keyArray[i+1]){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[i+1], newPage);
				currentNode = (NonLeafNodeInt*)(newPage);
			}
			if(lowValInt >= currentNode->keyArray[i] && i == sizeof(currentNode->keyArray) - 1){
				leafPageNum = currentNode->pageNoArray[i];
				pagesAccessed.push_back(currentNode->pageNoArray[i]);
				bufMgr->readPage(file, currentNode->pageNoArray[sizeof(currentNode->pageNoArray)-1], newPage);
				currentNode = (NonLeafNodeInt*)(newPage);
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

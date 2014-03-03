/*
 * kewal.cc
 *
 *  Created on: Feb 27, 2014
 *      Author: Kewal
 */





#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
	if(!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{

}

RC IndexManager::createFile(const string &fileName)
{
	dbgnRBFM("<IX-------------------createFile-------------------IX>","");
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->createFile(fileName.c_str())==-1)return -1;
	FileHandle fileHandle;
	if(pfm->openFile(fileName.c_str(), fileHandle)==-1)return -1;
	void* data = malloc(PAGE_SIZE);
	fileHandle.appendPage(data);
	if(pfm->closeFile(fileHandle)==-1)return -1;
	free(data);
	dbgnRBFM("</IX------------------createFile------------------IX/>","");
	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	dbgnRBFM("<IX-------------------destroyFile-------------------IX>","");
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->destroyFile(fileName.c_str())==-1)return -1;
	dbgnRBFM("</IX------------------destroyFile------------------IX/>","");
	return 0;
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	dbgnRBFM("<IX-------------------openFile-------------------IX>","");
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->openFile(fileName.c_str(), fileHandle)==-1)return -1;
	dbgnRBFM("</IX------------------openFile------------------IX/>","");
	return 0;
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
	dbgnRBFM("<IX-------------------closeFile-------------------IX>","");
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->closeFile(fileHandle)==-1)return -1;
	dbgnRBFM("</IX------------------closeFile------------------IX/>","");
	return 0;
}

///this routine will give >0 :    if keyIndex < keyInput
//						  0  : 	if keyIndex = keyInput
//						 <0  : 	if keyIndex > keyInput
// it chekcs if arg2 > arg2, return normalised diff(irrepsective of datatype)
float IndexManager::compare(void * keyIndex,void* keyInput,AttrType type)  ////
{
	float diff;
	bool result=false;
	dbgnIXU("<IXu---------------compare---------------IXu>"," ");
	dbgnIXU("type of attribute tobe compared",type);
	switch(type)
	{
	case 0:
		diff=intVal(keyInput)-intVal(keyIndex);
		dbgnIXU("Comparing the integers here !","");
		dbgnIXU(intVal(keyInput),intVal(keyIndex));
		break;
	case 1:
		dbgnIXU("Comparing the floats here !","");
		dbgnIXU((*((float *)keyInput)),(*((float *)keyIndex)));
		diff=*((float *)keyInput)-*((float *)keyIndex);

		if(modlus(diff)<0.000001)diff=0;
		break;
	case 2:
		diff= strcmp((char *)keyInput+4,(char *)keyIndex+4);
		dbgnIXU("Comparing the strings here !","");
		dbgnIXU((char*)keyInput,(char*)keyIndex);
		break;
	}

	dbgnIXU("result of comparison",result==1);
	return(diff);
}

// Inserts a new page of INDEX TYPE and updates the pageNum to contain the Virtual Page Number of the new Page added
// Also updates the header page to contain the correct amount of freeSpace
RC IndexManager::insertIndexNode(INT32& pageNum, FileHandle fileHandle){
	dbgnRBFM("<IXu---------------insertIndexNode---------------IXu>","");
	void* data = malloc(PAGE_SIZE);
	pageType(data) = (BYTE)1;
	getFreeOffsetV(data) = (INT16)12;
	getSlotNoV(data) = (INT16)0;
	fileHandle.appendPage(data);
	pageNum = fileHandle.getNumberOfPages()-1;
	fileHandle.updateFreeSpaceInHeader(pageNum,-12);
	free(data);
	dbgnRBFM("</IXu--------------insertIndexNode--------------IXu/>","");
	return 0;
}

// Inserts a new page of LEAF TYPE and updates the pageNum to contain the Virtual Page Number of the new Page added
// Also updates the header page to contain the correct amount of freeSpace
RC IndexManager::insertLeafNode(INT32& pageNum, FileHandle fileHandle){
	dbgnRBFM("<IXu---------------insertLeafNode---------------IXu>","");
	void* data = malloc(PAGE_SIZE);
	pageType(data) = (BYTE)0;
	getFreeOffsetV(data) = (INT16)12;
	getSlotNoV(data) = (INT16)0;
	fileHandle.appendPage(data);
	pageNum = fileHandle.getNumberOfPages()-1;
	fileHandle.updateFreeSpaceInHeader(pageNum,-12);
	free(data);
	dbgnRBFM("</IXu---------------insertLeafNode--------------IXu/>","");
	return 0;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	// INITIALIZE THE ROOT
	INT32 root;
	// getRoot(fileHandle,root);
	if(root == -1) return -1;
	void* pageData = malloc(PAGE_SIZE); // This memory is freed within this function
	fileHandle.readPage(root, pageData);

	// SET SEARCH KEY
	void* tempKey;
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(key));
		searchKeyLength =  searchKeyLength + 5;
		tempKey = malloc(searchKeyLength); // This memory is freed within this function
		memcpy(tempKey,key,searchKeyLength-2);
		*((BYTE*)tempKey + (searchKeyLength-1)) = (BYTE)0;
		key = tempKey;
	}

	// FIND THE LEAF PAGE WHERE ENTRY IS STORED
	findLeafPage(fileHandle, pageData, root, key, attribute.type);

	// DELETE THAT ENTRY FROM THE LEAF PAGE
	INT16 freeSpaceIncrease = 0;
	deleteEntryInLeaf(fileHandle, attribute, key, rid, root, pageData, freeSpaceIncrease);

	// WRITE THAT PAGE BACK
	fileHandle.writePage(root, pageData);

	// UPDATE FREE SPACE IN HEADER
	fileHandle.updateFreeSpaceInHeader(root,freeSpaceIncrease);

	free(pageData);
	free(tempKey);
	return 0;
}

RC IndexManager::deleteEntryInLeaf(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid, INT32 root, void* pageData, INT16& freeSpaceIncrease){
	INT16 totalSlots = getSlotNoV(pageData);
	if(totalSlots==0)return -1;

	// Setup for Binary Search
	int start = 0;

	// Find lowest record in page which is not deleted, i.e (offset != -1)
	// No need to do same for highest record, because highest record will always EXIST ! (Nature of delete)
	INT16 startOffset = getSlotOffV(pageData,start);
	while(startOffset==-1){
		start++;
		startOffset = getSlotOffV(pageData,start);
	}

	int halfType = 0; // This variable tracks which kind of halving was done in the previous iteration so you don't end in infinite loop.
	int end = totalSlots-1; // Will Always EXIST !
	int mid = (start+end)/2;

	// BINARY SEARCH STARTS
	while(start<=end){
		// Find EXISTING mid value
		INT16 midOffset = getSlotOffV(pageData,mid);
		if(midOffset==-1){
			if(halfType==0){
				while(midOffset == -1){
					mid = mid-1;
					midOffset = getSlotOffV(pageData,mid);
				}
			}
			else{
				while(midOffset == -1){
					mid = mid+1;
					midOffset = getSlotOffV(pageData,mid);
				}
			}
		}

		// Check for equality
		if(compare((BYTE*)pageData+midOffset,key,attribute.type) == 0){
			if(mid == totalSlots-1)getSlotNoV(pageData) = getSlotNoV(pageData)-1; // Handles the case where last slot is deleted, so reduce total slots in page by 1
			else getSlotOffV(pageData,mid) = (INT16)-1;
			freeSpaceIncrease += (getSlotLenA(pageData,mid)+4);
			return 0;
		}

		// Other checks
		if(compare((BYTE*)pageData+midOffset,key,attribute.type) < 0){
			end = mid-1;
			halfType = 0;
		}
		else{
			start = mid+1;
			halfType = 1;
		}
		// Update mid
		mid = (start+end)/2;
	}
	// If reached here, returns non negative error code.
	return 1;
}

INT32 IndexManager::getIndexValueAtOffset(void* pageData, INT16 offset, AttrType type){
	INT16 inRecordOffset = 4;
	if(type == 2){
		INT32 length = *((INT32*)((BYTE*)pageData+offset));
		inRecordOffset+=length;
	}
	return *((INT32*)((BYTE*)pageData+offset+inRecordOffset));
}

RC IndexManager::scan(FileHandle &fileHandle,
		const Attribute &attribute,
		const void      *lowKey,
		const void      *highKey,
		bool			lowKeyInclusive,
		bool        	highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator)
{
	// Assign Values to ix_ScanIterator
	ix_ScanIterator.fileHandle = fileHandle;
	ix_ScanIterator.type = attribute.type;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;

	// STILL TO BE DECLARED
	/*	RID nextRid;
	INT16 totalSlotsInCurrPage;
	void* leafPage;*/
	// void *highKey; DECLARED BELOW

	// Initialize Root and read root
	INT32 root;
	// getRoot(fileHandle,root);
	if(root == -1) return -1;
	void* pageData = malloc(PAGE_SIZE); // THIS WILL BE FREED BY SCANITERATOR
	fileHandle.readPage(root, pageData);

	// Make new format for Low Key Value
	void* tempLowKey;
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(lowKey));
		searchKeyLength =  searchKeyLength + 5;
		tempLowKey = malloc(searchKeyLength); // THIS IS FREED IN THIS FUNCTION ITSELF
		memcpy(tempLowKey,lowKey,searchKeyLength-2);
		*((BYTE*)tempLowKey + (searchKeyLength-1)) = (BYTE)0;
	}

	// Make new format for High Key Value
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(highKey));
		searchKeyLength =  searchKeyLength + 5;
		ix_ScanIterator.highKey = malloc(searchKeyLength); // THIS WILL BE FREED BY SCANITERATOR !
		memcpy(ix_ScanIterator.highKey,highKey,searchKeyLength-1);
		*((BYTE*)ix_ScanIterator.highKey + (searchKeyLength-1)) = (BYTE)0;
	}

	// Return error if higKey < lowKey
	if()
		return -1;

	// Find Leaf Page where possible first record is present
	findLeafPage(fileHandle, pageData, root, tempLowKey, attribute.type);
	findLowSatisfyingEntry(fileHandle, pageData, root, tempLowKey, lowKeyInclusive, attribute.type, ix_ScanIterator.nextRid);
	ix_ScanIterator.leafPage = pageData;
	ix_ScanIterator.totalSlotsInCurrPage = getSlotNoV(pageData);
	return 0;
}

INT32 IndexManager::findLowSatisfyingEntry(FileHandle& fileHandle, void* pageData, INT32& root, void* lowKey, bool lowKeyInclusive, void* highKey, bool highKeyInclusive, AttrType type, RID& nextRid){
	INT32 nextPage = -1;
	INT16 totalSlots = getSlotNoV(pageData);
	while(totalSlots==0){
		nextPage = *((INT32*)((BYTE*)pageData+8));
		if(nextPage == -1){
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			return 0;
		}
		fileHandle.readPage(nextPage,pageData);
		totalSlots = getSlotNoV(pageData);
	}

	int start = 0;
	INT16 startOffset = getSlotOffV(pageData,start);
	while(startOffset==-1 && start < totalSlots-1){
		start++;
		startOffset = getSlotOffV(pageData,start);
	}

	if(lowKey == NULL){
		nextRid.pageNum = (INT32)nextPage;
		nextRid.slotNum = (INT16)start;
		return 0;
	}

	int halfType = 0;
	int end = totalSlots-1;
	int mid = (start+end)/2;

	while(start<=end){
		// Set mid such that it is not a deleted entry
		INT16 midOffset = getSlotOffV(pageData,mid);
		if(midOffset==-1){
			if(halfType==0){
				while(midOffset == -1){
					mid = mid-1;
					midOffset = getSlotOffV(pageData,mid);
				}
			}
			else{
				while(midOffset == -1){
					mid = mid+1;
					midOffset = getSlotOffV(pageData,mid);
				}
			}
		}
		// Compare mid with lowKey
		// First lowest record is found when midKey>=lowKey && (mid-1)Key<lowKey
		// First lowest record is (mid)Key
		if(compare((BYTE*)pageData+midOffset,lowKey,type) >= 0){
			INT16 midMinusOne = mid - 1;
			INT16 midMinusOneOffset = getSlotOffV(pageData,midMinusOne);
			while(midMinusOneOffset == -1 && midMinusOne>0){
				midMinusOne = midMinusOne-1;
				midMinusOneOffset = getSlotOffV(pageData,midMinusOne);
			}
			// This is where record will be finally found !
			if(compare((BYTE*)pageData+midMinusOneOffset,lowKey,type) < 0 || mid <= start){

				// If lowKeyInclusive is false and (mid)Key == lowKey required key is (mid+1)Key
				if(compare((BYTE*)pageData+midOffset,lowKey,type) == 0 && lowKeyInclusive == false){

				}

				// If lowKeyInclusive is true OR lowKey != (mid)Key we return mid as required record !
				else{
					// Check if our first record is not greater than highKey if highKeyInclusive == true and is not greater than equal to highKey if highKeyInclusive = false
					if(compare((BYTE*)pageData+midOffsetPlusOne,highKey,type) > 0 || (compare((BYTE*)pageData+midOffsetPlusOne,highKey,type) == 0 && highKeyInclusive == false) ){
						nextRid.pageNum = (INT32)-1;
						nextRid.slotNum = (INT16)-1;
						return 0;
					}
					nextRid.pageNum = (INT32)nextPage;
					nextRid.slotNum = (INT16)mid;
					return 0;
				}
			}
			else{
				end = mid-1;
				halfType = 0;
			}
		}
		// First lowest record is found when midKey<lowKey && (mid+1)Key>=lowKey
		// First lowest record is (mid+1)Key
		else if(compare((BYTE*)pageData+midOffset,lowKey,type) < 0){
			INT16 midPlusOne = mid + 1;
			INT16 midPlusOneOffset = getSlotOffV(pageData,midPlusOne);
			while(midPlusOneOffset == -1 && midPlusOne<end-1){
				midPlusOne = midPlusOne+1;
				midPlusOneOffset = getSlotOffV(pageData,midPlusOne);
			}
			// This is where record will be finally found !
			if(compare((BYTE*)pageData+midPlusOneOffset,lowKey,type) >= 0 || mid >= end-1){

				// If lowKeyInclusive is false and (mid+1)Key == lowKey required key is (mid+2)Key
				if(compare((BYTE*)pageData+midPlusOneOffset,lowKey,type) == 0 && lowKeyInclusive==false){
					INT16 slot = midPlusOne + 1;
					INT16 slotOffset = getSlotOffV(pageData,slot);
					while(slotOffset==-1 && slot < totalSlots-1){
						slot++;
						slotOffset = getSlotOffV(pageData,slot);
					}
					if(slotOffset==-1 && slot==totalSlots-1){
						slot = 0;
						nextPage = *((INT32*)((BYTE*)pageData+8));
						fileHandle.readPage(nextPage,pageData);
						totalSlots = getSlotNoV(pageData);
						while(totalSlots==0){
							nextPage = *((INT32*)((BYTE*)pageData+8));
							if(nextPage == -1){
								nextRid.pageNum = (INT32)-1;
								nextRid.slotNum = (INT16)-1;
								return 0;
							}
							fileHandle.readPage(nextPage,pageData);
							totalSlots = getSlotNoV(pageData);
						}
						slotOffset = getSlotOffV(pageData,slot);
						while(slotOffset==-1 && slot < totalSlots-1){
							slot++;
							slotOffset = getSlotOffV(pageData,slot);
						}
						if(slotOffset==-1 && slot==totalSlots-1){
							nextRid.pageNum = (INT32)-1;
							nextRid.slotNum = (INT16)-1;
							return 0;
						}
					}
				}

				// If lowKeyInclusive is true OR lowKey != (mid)Key we return mid as required record !
				else{
					// Check if our first record is not greater than highKey if highKeyInclusive == true and is not greater than equal to highKey if highKeyInclusive = false
					if(compare((BYTE*)pageData+midOffsetPlusOne,highKey,type) > 0 || (compare((BYTE*)pageData+midOffsetPlusOne,highKey,type) == 0 && highKeyInclusive == false) ){
						nextRid.pageNum = (INT32)-1;
						nextRid.slotNum = (INT16)-1;
						return 0;
					}
					nextRid.pageNum = (INT32)nextPage;
					nextRid.slotNum = (INT16)midPlusOne;
					return 0;
				}
			}
			else{
				start = mid+1;
				halfType = 1;
			}
		}
		mid = (start+end)/2;
	}
	return 1;
}

IX_ScanIterator::IX_ScanIterator()
{

}

IX_ScanIterator::~IX_ScanIterator()
{

}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(nextRid.pageNum == -1 && nextRid.slotNum == -1) return -1;
	rid = nextRid;
	INT16 currSlot = nextRid.slotNum;
	if(currSlot < totalSlotsInCurrPage-1){
		currSlot++;
	}
	else{
		currSlot=0;
		INT32 nextPage = *((INT32*)((BYTE*)leafPage+8));
		if(nextPage == -1){
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			return -1;
		}
		fileHandle.readPage(nextPage,leafPage);
		totalSlotsInCurrPage = getSlotNoV(leafPage);
	}

	while(getSlotOffV(leafPage,currSlot)==-1){
		if(currSlot < totalSlotsInCurrPage-1){
			currSlot++;
		}
		else{
			currSlot=0;
			INT32 nextPage = *((INT32*)((BYTE*)leafPage+8));
			if(nextPage == -1){
				nextRid.pageNum = (INT32)-1;
				nextRid.slotNum = (INT16)-1;
				return -1;
			}
			fileHandle.readPage(nextPage,leafPage);
			totalSlotsInCurrPage = getSlotNoV(leafPage);
		}
	}

	INT16 recordOffset = getSlotOffV(leafPage,currSlot);
	if((highKey==NULL) || compare((BYTE*)leafPage+recordOffset, highKey, type)>=0){
		if(compare((BYTE*)leafPage+recordOffset, highKey, type)==0 && highKeyInclusive==false){
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			return -1;
		}

		INT16 inRecordOffset = 4;
		if(type == 2){
			INT16 inRecordKeyLength = *((INT32*)((BYTE*)leafPage+recordOffset));
			inRecordOffset += inRecordKeyLength;
		}
		nextRid.pageNum = *((INT32*)((BYTE*)leafPage+recordOffset+inRecordOffset));
		nextRid.slotNum = *((INT16*)((BYTE*)leafPage+recordOffset+inRecordOffset+4));
	}
	return 0;
}

RC IX_ScanIterator::close()
{
	free(leafPage);
	free(highKey);
	return 0;
}

void IX_PrintError (RC rc)
{
	if(rc==1)
		cout<<"Entry not Deleted: Not found";
}

void IndexManager::findLeafPage(FileHandle& fileHandle, void* pageData, INT32& root, void* key, AttrType type){
	while(pageType(pageData)!=0){
		INT16 totalSlots = getSlotNoV(pageData);
		int start = 0;
		int end = totalSlots-1;
		int mid = (start+end)/2;

		// Check if keyInput is smaller than first entry
		INT16 startOffset = getSlotOffV(pageData,start);
		while(startOffset==-1 && start < totalSlots-1){
			start++;
			startOffset = getSlotOffV(pageData,start);
		}
		if((key==NULL) || compare((BYTE*)pageData+startOffset,key,type)<0){
			root = *(INT32*((BYTE*)pageData + 8));
			fileHandle.readPage(root, pageData);
			continue;
		}

		// Check if keyInput is larger than last entry
		INT16 endOffset = getSlotOffV(pageData,end);
		if(compare((BYTE*)pageData+endOffset,key,type) >= 0){
			root = getIndexValueAtOffset(pageData, endOffset, type);
			fileHandle.readPage(root, pageData);
			continue;
		}

		while(start<end){
			INT16 midOffset = getSlotOffV(pageData,mid);
			if(compare((BYTE*)pageData+midOffset,key,type) == 0){
				root = getIndexValueAtOffset(pageData, midOffset, type);
				fileHandle.readPage(root, pageData);
				break;
			}

			if(compare((BYTE*)pageData+midOffset,key,type) < 0){
				INT16 midOffsetminusone = getSlotOffV(pageData,mid-1);
				if(compare((BYTE*)pageData+midOffsetminusone,key,type) > 0){
					root = getIndexValueAtOffset(pageData, midOffsetminusone, type);
					fileHandle.readPage(root, pageData);
					break;
				}
				end = mid-1;
			}

			if(compare((BYTE*)pageData+midOffset,key,type) > 0){
				INT16 midOffsetplusone = getSlotOffV(pageData,mid+1);
				if(compare((BYTE*)pageData+midOffsetplusone,key,type) < 0){
					root = getIndexValueAtOffset(pageData, midOffsetplusone, type);
					fileHandle.readPage(root, pageData);
					break;
				}
				start = mid+1;
			}
			mid = (start+end)/2;
		}
	}
}

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
	dbgnIXfn();
	// INITIALIZE THE ROOT
	INT32 root;
	getRoot(fileHandle,root);
	if(root == -1){
		dbgnIXFnc();
		return -1;
	}
	void* pageData = malloc(PAGE_SIZE); // This memory is freed within this function
	fileHandle.readPage(root, pageData);

	// SET SEARCH KEY
	void* tempLowKey;
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(key));
		searchKeyLength =  searchKeyLength + 5;
		tempLowKey = malloc(searchKeyLength); // This memory is freed within this function
		memcpy(tempLowKey,key,searchKeyLength-1);
		*((BYTE*)tempLowKey + (searchKeyLength-1)) = (BYTE)0;
	}

	// FIND THE LEAF PAGE WHERE ENTRY IS STORED
	findLeafPage(fileHandle, pageData, root, tempLowKey, attribute.type);

	// DELETE THAT ENTRY FROM THE LEAF PAGE
	INT16 freeSpaceIncrease = 0;
	deleteEntryInLeaf(fileHandle, attribute, tempLowKey, rid, root, pageData, freeSpaceIncrease);

	// WRITE THAT PAGE BACK
	fileHandle.writePage(root, pageData);

	// UPDATE FREE SPACE IN HEADER
	fileHandle.updateFreeSpaceInHeader(root,freeSpaceIncrease);

	free(pageData);
	free(tempKey);
	dbgnIXFnc();
	return 0;
}

RC IndexManager::deleteEntryInLeaf(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid, INT32 root, void* pageData, INT16& freeSpaceIncrease){
	dbgnIXFn();
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

	int end = totalSlots-1; // Will Always EXIST !
	int mid = (start+end)/2;

	// BINARY SEARCH STARTS
	while(start<=end){
		// Find EXISTING mid value
		INT16 midOffset = getSlotOffV(pageData,mid);
		while(midOffset == -1 && mid<end){
			mid = mid+1;
			midOffset = getSlotOffV(pageData,mid);
		}

		// Check for equality
		if(compare((BYTE*)pageData+midOffset,key,attribute.type) == 0){
			// Handles the case where last slot is deleted, In this case it makes the last slot existant or reduces the total number of slots to 0
			INT16 reducedSlotsBy = 0;
			if(mid == totalSlots-1){
				while(true){
					reducedSlotsBy++;
					mid = mid-1;
					if(getSlotOffV(pageData,mid)==-1)break;
				}
				getSlotNoV(pageData) = (INT16)getSlotNoV(pageData)-reducedSlotsBy;
			}
			else getSlotOffV(pageData,mid) = (INT16)-1;

			freeSpaceIncrease += (getSlotLenA(pageData,mid)+4);
			dbgnIXFnc();
			return 0;
		}

		// Other checks
		if(compare((BYTE*)pageData+midOffset,key,attribute.type) < 0){
			end = mid-1;
		}
		else{
			start = mid+1;
		}
		// Update mid
		mid = (start+end)/2;
	}
	// If reached here, returns non negative error code.
	dbgnIXFnc();
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
	dbgnIXFn();
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
		memcpy(tempLowKey,lowKey,searchKeyLength-1);
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
	if(compare(tempLowKey,ix_ScanIterator.highKey,attribute.type)<0){
		dbgnIXFnc();
		return -1;
	}
	// Find Leaf Page where possible first record is present
	findLeafPage(fileHandle, pageData, root, tempLowKey, attribute.type);
	findLowSatisfyingEntry(fileHandle, pageData, root, tempLowKey, lowKeyInclusive, attribute.type, ix_ScanIterator.nextRid);
	dbgnIX("RID Pagenum",ix_ScanIterator.nextRid.pageNum)
	dbgnIX("RID SlotNum",ix_ScanIterator.nextRid.slotNum)
	ix_ScanIterator.leafPage = pageData;
	ix_ScanIterator.totalSlotsInCurrPage = getSlotNoV(pageData);
	dbgnIXFnc();
	return 0;
}

INT32 IndexManager::findLowSatisfyingEntry(FileHandle& fileHandle, void* pageData, INT32& root, void* lowKey, bool lowKeyInclusive, void* highKey, bool highKeyInclusive, AttrType type, RID& nextRid){
	dbgnIXFn();
	// JUST DOING A LINEAR SCAN IN THE FOUND PAGE
	INT32 nextPage = -1;
	INT16 start = 0;
	INT16 startOffset;
	do{
		INT16 totalSlots = getSlotNoV(pageData);
		while(totalSlots==0){
			nextPage = *((INT32*)((BYTE*)pageData+8));
			if(nextPage == -1){
				nextRid.pageNum = (INT32)-1;
				nextRid.slotNum = (INT16)-1;
				dbgnIX("Case 1 ", "End of linked list");
				dbgnIXFnc();
				return 0;
			}
			fileHandle.readPage(nextPage,pageData);
			totalSlots = getSlotNoV(pageData);
		}

		start = 0;
		startOffset = getSlotOffV(pageData,start);
		while(startOffset==-1 && start < totalSlots-1){
			start++;
			startOffset = getSlotOffV(pageData,start);
		}
	}
	while(compare((BYTE*)pageData+startOffset,lowKey,type) < 0 );

	if(lowKey == NULL){
		dbgnIX("Case 2", "Low key is Null");
		if(compare((BYTE*)pageData+startOffset,highKey,type) > 0 || (compare((BYTE*)pageData+startOffset,highKey,type) == 0 && highKeyInclusive == false) ){
			dbgnIX("Case 2.1","first entry found is greater(equal) than highkey");
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			dbgnIXFnc();
			return 0;
		}
		dbgnIX("Case 2.2","entry is less than highkey!");
		nextRid.pageNum = (INT32)nextPage;
		nextRid.slotNum = (INT16)start;
		dbgnIXFnc();
		return 0;
	}

	if(compare((BYTE*)pageData+startOffset,lowKey,type) == 0 && lowKeyInclusive == false){
		dbgnIX("Case 3","lowkey inclusive is false, search for next entry");
		INT16 totalSlots = getSlotNoV(pageData);
		while(totalSlots==0){
			nextPage = *((INT32*)((BYTE*)pageData+8));
			if(nextPage == -1){
				dbgnIX("case 3.1","end of linked list reached");
				nextRid.pageNum = (INT32)-1;
				nextRid.slotNum = (INT16)-1;
				dbgnIXFnc();
				return 0;
			}
			fileHandle.readPage(nextPage,pageData);
			totalSlots = getSlotNoV(pageData);
		}
		start = 0;
		startOffset = getSlotOffV(pageData,start);
		while(startOffset==-1 && start < totalSlots-1){
			start++;
			startOffset = getSlotOffV(pageData,start);
		}

		if(compare((BYTE*)pageData+startOffset,highKey,type) < 0 || (compare((BYTE*)pageData+startOffset,highKey,type) == 0 && highKeyInclusive == false) ){
			dbgnIX("Case 3.2","new next entry found is greater(equal) than highkey");
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			dbgnIXFnc();
			return 0;
		}
		dbgnIX("Case 3.3","proper entry found when lowkey = false");
		nextRid.pageNum = (INT32)nextPage;
		nextRid.slotNum = (INT16)start;
		dbgnIXFnc();
		return 0;
	}

	dbgnIX("Case 4","lowkey != null & low inclusive = true or it is false but does not matter, return 1st RID");
	if(compare((BYTE*)pageData+startOffset,highKey,type) < 0 || (compare((BYTE*)pageData+startOffset,highKey,type) == 0 && highKeyInclusive == false) ){
		dbgnIX("Case 4.1","first entry found is greater(equal) than highkey");
		nextRid.pageNum = (INT32)-1;
		nextRid.slotNum = (INT16)-1;
		dbgnIXFnc();
		return 0;
	}
	dbgnIX("Case 4.2","proper first entry found");
	nextRid.pageNum = (INT32)nextPage;
	nextRid.slotNum = (INT16)start;
	dbgnIXFnc();
	return 0;
}

IX_ScanIterator::IX_ScanIterator()
{

}

IX_ScanIterator::~IX_ScanIterator()
{

}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	dbgnIXFn();
	if(nextRid.pageNum == -1 && nextRid.slotNum == -1){
		dbgnIX("Case1","No next entry");
		dbgnIXFnc();
		return IX_EOF;
	}
	// RETURN RID
	rid = nextRid;

	// SET NEXT RID


	// SET NEXT SLOT
	INT16 currSlot = nextRid.slotNum;
	if(currSlot < totalSlotsInCurrPage-1){
		currSlot++;
	}
	else{
		currSlot=0;
		INT32 nextPage = *((INT32*)((BYTE*)leafPage+8));
		if(nextPage == -1){
			dbgnIX("Case 2","reached end of list while searching for next entry");
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			dbgnIXFnc();
			return -1;
		}
		fileHandle.readPage(nextPage,leafPage);
		totalSlotsInCurrPage = getSlotNoV(leafPage);
	}

	// SET NEXT VALID SLOT
	while(getSlotOffV(leafPage,currSlot)==-1){
		if(currSlot < totalSlotsInCurrPage-1){
			currSlot++;
		}
		else{
			currSlot=0;
			INT32 nextPage = *((INT32*)((BYTE*)leafPage+8));
			if(nextPage == -1){
				dbgnIX("Case 3","reached end of list while searching for next entry");
				nextRid.pageNum = (INT32)-1;
				nextRid.slotNum = (INT16)-1;
				dbgnIXFnc();
				return -1;
			}
			fileHandle.readPage(nextPage,leafPage);
			totalSlotsInCurrPage = getSlotNoV(leafPage);
		}
	}

	// CHECK IF NEXT VALID SLOT IS LESS THAN HIGH KEY
	INT16 recordOffset = getSlotOffV(leafPage,currSlot);

	if((highKey==NULL) || compare((BYTE*)leafPage+recordOffset, highKey, type)>=0){
		if(compare((BYTE*)leafPage+recordOffset, highKey, type)==0 && highKeyInclusive==false){
			dbgnIX("case 4","equal to higkey and highkey inclusive is false so set nextrid to -1");
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			dbgnIXFnc();
			return -1;
		}

		// VALID NEXT RID !!!!
		INT16 inRecordOffset = 4;
		if(type == 2){
			INT16 inRecordKeyLength = *((INT32*)((BYTE*)leafPage+recordOffset));
			inRecordOffset += inRecordKeyLength;
		}
		dbgnIX("case 6","Valid key formed");
		nextRid.pageNum = *((INT32*)((BYTE*)leafPage+recordOffset+inRecordOffset));
		nextRid.slotNum = *((INT16*)((BYTE*)leafPage+recordOffset+inRecordOffset+4));
		dbgnIX("Next pagenum",nextRid.pageNum);
		dbgnIX("Next SLotnum",nextRid.slotNum);
	}
	else{
		dbgnIX("case 5","crosses higkey so set nextrid to -1");
		nextRid.pageNum = (INT32)-1;
		nextRid.slotNum = (INT16)-1;
		dbgnIXFnc();
		return -1;
	}
	dbgnIXFnc();
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
	dbgnIXFn();
	if(rc==1)
		cout<<"Entry not Deleted: Not found";
	dbgnIXFnc();
}

void IndexManager::findLeafPage(FileHandle& fileHandle, void* pageData, INT32& root, void* key, AttrType type){
	dbgnIXFn();
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
				dbgnIXFnc();
				return;
			}

			if(compare((BYTE*)pageData+midOffset,key,type) < 0){
				end = mid-1;
			}

			if(compare((BYTE*)pageData+midOffset,key,type) > 0){
				start = mid+1;
			}
			mid = (start+end)/2;
		}
		if(compare((BYTE*)pageData+getSlotOffV(pageData,mid),key,type) < 0){
			root = getIndexValueAtOffset(pageData, getSlotOffV(pageData,mid-1), type);
			fileHandle.readPage(root, pageData);
			dbgnIXFnc();
			return;
		}
		root = getIndexValueAtOffset(pageData, getSlotOffV(pageData,mid), type);
		fileHandle.readPage(root, pageData);
		dbgnIXFnc();
		return;
	}
}

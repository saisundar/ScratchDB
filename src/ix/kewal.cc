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
	INT32 root;
	// getRoot(fileHandle,root);
	if(root == -1) return -1;
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(key));
		searchKeyLength =  searchKeyLength + 5;
		void* tempKey = malloc(searchKeyLength); // FREE THIS
		memcpy(tempKey,key,searchKeyLength-2);
		*((BYTE*)tempKey + (searchKeyLength-1)) = (BYTE)0;
		key = tempKey;
	}
	void* pageData = malloc(PAGE_SIZE); // FREE THIS
	fileHandle.readPage(root, pageData);
	findLeafPage(fileHandle, pageData, root, key, attribute.type);
	deleteEntryInLeaf(fileHandle, attribute, key, rid, root, pageData);
	fileHandle.writePage(root, pageData);
	free(tempkey);
	free(pageData);
	return 0;
}

RC IndexManager::deleteEntryInLeaf(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid, INT32 root, void* pageData){
	INT16 totalSlots = getSlotNoV(pageData);
	if(totalSlots==0)return -1;

	bool isFound = false;
	int start = 0;
	INT16 startOffset = getSlotOffV(pageData,start);
	while(startOffset==-1){
		start++;
		startOffset = getSlotOffV(pageData,start);
	}
	int halfType = 0;
	int end = totalSlots-1;
	int mid = (start+end)/2;
	while(start<=end){
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

		if(compare((BYTE*)pageData+midOffset,key,attribute.type) == 0){
			getSlotOffV(pageData,mid) = (INT16)-1;
			return 0;
		}
		if(compare((BYTE*)pageData+midOffset,key,attribute.type) < 0){
			end = mid-1;
			halfType = 0;
		}
		else{
			start = mid+1;
			halfType = 0;
		}
		mid = (start+end)/2;
	}
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
		tempLowKey = malloc(searchKeyLength); // FREE THIS NOW !
		memcpy(tempLowKey,lowKey,searchKeyLength-2);
		*((BYTE*)tempLowKey + (searchKeyLength-1)) = (BYTE)0;
	}

	// Make new format for High Key Value
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(highKey));
		searchKeyLength =  searchKeyLength + 5;
		ix_ScanIterator.highKey = malloc(searchKeyLength); // THIS WILL BE FREED BY SCANITERATOR !
		memcpy(ix_ScanIterator.highKey,highKey,searchKeyLength-2);
		*((BYTE*)ix_ScanIterator.highKey + (searchKeyLength-1)) = (BYTE)0;
	}

	// Find Leaf Page where possible first record is present
	findLeafPage(fileHandle, pageData, root, tempLowKey, attribute.type);
	findLowSatisfyingEntry(fileHandle, pageData, root, tempLowKey, attribute.type, ix_ScanIterator.nextRid);
	ix_ScanIterator.leafPage = pageData;
	ix_ScanIterator.totalSlotsInCurrPage = getSlotNoV(pageData);
	return 0;
}

INT32 IndexManager::findLeafPage(FileHandle& fileHandle, void* pageData, INT32& root, void* key, AttrType type, RID& nextRid){

}

IX_ScanIterator::IX_ScanIterator()
{

}

IX_ScanIterator::~IX_ScanIterator()
{
	free(leafPage);
	free(highKey);
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(nextRid.pageNum == -1 && nextRid.slotNum == -1) return -1;
	rid = nextRid;
	INT16 currSlot = nextRid.slotNum;
	if(currSlot != totalSlotsInCurrPage){
		currSlot++;
	}
	else{
		currSlot=0;
		INT32 nextPage = *((INT32*)((BYTE*)leafPage+8));
		fileHandle.readPage(nextPage,leafPage);
		totalSlotsInCurrPage = getSlotNoV(leafPage);
	}

	while(getSlotOffV(leafPage,currSlot)==-1){
		if(currSlot != totalSlotsInCurrPage){
			currSlot++;
		}
		else{
			currSlot=0;
			INT32 nextPage = *((INT32*)((BYTE*)leafPage+8));
			fileHandle.readPage(nextPage,leafPage);
			totalSlotsInCurrPage = getSlotNoV(leafPage);
		}
	}
	INT16 recordOffset = getSlotOffV(leafPage,currSlot);
	if(compare((BYTE*)leafPage+recordOffset, highKey, type)>=0){
		if(compare((BYTE*)leafPage+recordOffset, highKey, type)==0 && highKeyInclusive==false){
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
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
	return -1;
}

void IX_PrintError (RC rc)
{
}

INT32 IndexManager::findLeafPage(FileHandle& fileHandle, void* pageData, INT32& root, void* key, AttrType type){
	while(pageType(pageData)!=0){
		INT16 totalSlots = getSlotNoV(pageData);
		int start = 0;
		int end = totalSlots-1;
		int mid = (start+end)/2;

		// Check if keyInput is smaller than first entry
		INT16 startOffset = getSlotOffV(pageData,start);
		if(compare((BYTE*)pageData+startOffset,key,type)<0){
			root = *(INT32*((BYTE*)pageData + 4));
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

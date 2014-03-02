
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
	dbgnIXFn();
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->createFile(fileName.c_str())==-1)return -1;
	FileHandle fileHandle;
	if(pfm->openFile(fileName.c_str(), fileHandle)==-1)return -1;
	void* data = malloc(PAGE_SIZE);
	memcpy(data,&((INT32)(-1)),4);
	fileHandle.appendPage(data);
	if(pfm->closeFile(fileHandle)==-1)return -1;
	free(data);
	dbgnIXFn();
	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	dbgnIXFn();
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->destroyFile(fileName.c_str())==-1)return -1;
	dbgnIXFnc();
	return 0;
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	dbgnIXFn();
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->openFile(fileName.c_str(), fileHandle)==-1)return -1;
	dbgnIXFnc();
	return 0;
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
	dbgnIXFn();
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->closeFile(fileHandle)==-1)return -1;
	dbgnIXFnc();
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
	dbgnIXFn();
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
	dbgnIXFn();
	void* data = malloc(PAGE_SIZE);
	pageType(data) = (BYTE)1;
	getFreeOffsetV(data) = (INT16)12;
	getSlotNoV(data) = (INT16)0;
	fileHandle.appendPage(data);
	pageNum = fileHandle.getNumberOfPages()-1;
	fileHandle.updateFreeSpaceInHeader(pageNum,-12);
	free(data);
	dbgnIXFnc();
	return 0;
}

// Inserts a new page of LEAF TYPE and updates the pageNum to contain the Virtual Page Number of the new Page added
// Also updates the header page to contain the correct amount of freeSpace
RC IndexManager::insertLeafNode(INT32& pageNum, FileHandle fileHandle){
	dbgnIXFn();
	void* data = malloc(PAGE_SIZE);
	pageType(data) = (BYTE)0;
	getFreeOffsetV(data) = (INT16)12;
	getSlotNoV(data) = (INT16)0;
	fileHandle.appendPage(data);
	pageNum = fileHandle.getNumberOfPages()-1;
	fileHandle.updateFreeSpaceInHeader(pageNum,-12);
	free(data);
	dbgnIXFnc();
	return 0;
}

RC IndexManager::setPrevPointerIndex(FileHandle &fileHandle,void *page,INT32 virtualPgNum)
{
	dbgnIXFn();
	dbgAssert(*page==1);
	memcpy((BYTE *)page+8,&virtualPgNum,4);
	dbgnIXFnc();
}

RC IndexManager::setPrevSiblingPointerLeaf(FileHandle &fileHandle,void *page,INT32 virtualPgNum)
{
	dbgnIXFn();
	dbgAssert((BYTE *)*page==0);
	memcpy((BYTE *)page+8,&virtualPgNum,4);
	dbgnIXFnc();
}

RC IndexManager::setNextSiblingPointerLeaf(FileHandle &fileHandle,void *page,INT32 virtualPgNum)
{
	dbgnIXFn();
	dbgAssert((BYTE *)*page==0);
	memcpy((BYTE *)page+8,&virtualPgNum,4);
	dbgnIXFnc();
}
INT32 IndexManager::getPrevSiblingPointerLeaf(FileHandle &fileHandle,void *page)
{
	dbgnIXFn();
	INT32 virtualPgNum;
	dbgAssert((BYTE *)*page==0);
	memcpy(&virtualPgNum,(BYTE *)page+8,4);
	return virtualPgNum;
	dbgnIXFnc();
}

INT32 IndexManager::getNextSiblingPointerLeaf(FileHandle &fileHandle,void *page)
{
	dbgnIXFn();
	INT32 virtualPgNum;
	dbgAssert((BYTE *)*page==0);
	memcpy(&virtualPgNum,(BYTE *)page+8,4);
	return virtualPgNum;
	dbgnIXFnc();
}
RC IndexManager::updateRoot(FileHandle &fileHandle,INT32 root)
{
	dbgnIXFn();
	void* page=malloc(PAGE_SIZE);
	fileHandle.readPage(0,page);
	memcpy(page,&root,4);
	fileHandle.writePage(0,page);
	free(page);
	dbgnIXFnc();
	return 0;
}

// this routine assumes that page is a valid pointer with the actual page info, and key is a null pointer which will be allocated
// inside the routine
// returns the length of the new key, returns -1 if error or deleted.
INT32 IndexManager::getKeyAtSlot(FileHandle &fileHandle,void* page,void* key,INT16 slotNo)
{

	if(page==NULL)return -1;
	dbgnIXFn();
	INT16 keyOffset = getSlotOffV(page,slotNo);
	INT16 keyLength = getSlotLenV(page,slotNo);

	key= malloc(keyLength);
	memcpy(key,(BYTE *)page+keyOffset,keyLength);
	dbgnIXFnc();
	return keyLength;

}

RC IndexManager::insertRecurseEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid,INT32 nodeNum,	//
		void **newChildKey)
{
	void* page=malloc(PAGE_SIZE);
	dbgnIXFn();
	if(fileHandle.readPage(nodeNum,page)!=0)
		dbgnIX("Oops wrong pagenum--does not exist or some error !"," ");
	if(isleaf(page))
	{




	}
	else
	{




	}
	dbgnIXFnc();
}

//aLL in mmeory operations --- NO I/O at all here....
RC IndexManager::reOrganizePage(INT32 virtualPgNum, void* page)
{
		dbgnIXFn();
		dbgnIXU("Filename",fileHandle.fileName);

		void *newPage=malloc(PAGE_SIZE);

		INT16 freeOffset,origOffset,origLength=0,totalSlots,currSlot=0,newSlot=0;
		INT32 virtualPageNum=virtualPgNum;

		memcpy(newPage,page,PAGE_SIZE);				//copy the previous indices , isIndexByte and all other overheads
		freeOffset=12;
		totalSlots=getSlotNoV(page);
		origOffset=getFreeOffsetV(page);
		dbgnIXU("Total no of slots",totalSlots);
		dbgnIXU("Original free offset",origOffset);

		for(currSlot=0;currSlot<totalSlots;currSlot++)
		{
			origOffset=getSlotOffV(page,currSlot);
			origLength=getSlotLenV(page,currSlot);
			dbgnIXU("Current slot no",currSlot);
			dbgnIXU("Original offset",origOffset);
			dbgnIXU("Original length",origLength);
			// Empty slot
			if( origOffset == -1)
				continue;
			// Original Record
			if(origLength >= 0)
			{
				memcpy((BYTE *)newPage+freeOffset,(BYTE *)page+origOffset,origLength);   //copy the record
				memcpy(getSlotOffA(newPage,newSlot),&freeOffset,2);			 		 //copy new offset
				memcpy(getSlotLenA(newPage,newSlot),&origLength,2);
				newSlot++;
				freeOffset+=origLength;
				dbgnIXU("old slot moved to slot",newSlot);
				dbgnIXU("New freeOffset",freeOffset);
			}
		}
		memcpy(getFreeOffsetA(newPage),&freeOffset,2);
		dbgnIXU("The new free offset after reorganizing is ",getFreeOffsetV(newPage));
		memcpy(getSlotNoA(newPage),&newSlot,2);
		dbgnIXU("The new no of Slots after reorganizing is ",getSlotNoV(newPage));
		memcpy(page,newPage,PAGE_SIZE);					//copy  new page back to old page and overwrite evrything

		dbgAssert((4092-(newSlot*4)-freeOffset)==fileHandle.updateFreeSpaceInHeader(virtualPgNum,0));		//shuld be equal
		free(newPage);
		dbgnIXFnc();
		return 0;
}

RC IndexManager::splitNode(INT32 virtualPgNum,void *page,INT32 newChild,void* newChildPage,void **newChildKey)
{
	reOrganizePage(virtualPgNum,page);
	bool isLeaf=!pageType(page);
	INT16 freeOffset=getFreeOffsetV(page),totalSlots=getSlotNoV(page),prevMid=-1;
	INT16 actualfreeSpace=4092-(totalSlots*4)-freeOffset;
	INT16 middleSlot=totalSlots/2;
	INT16 midOffSet=getSlotOffV(page,middleSlot),midLength=getSlotLenV(page,middleSlot);
	INT16 ridOffset=-1;

	*newChildKey=malloc(midLength);
	memcpy(*newChildKey,getRecordAtSlot(page,middleSlot),midLength);

	if(!isLeaf)
	{
		ridOffset= (attribute.type==2)?(4+intVal(*newChildKey)):4;
		memcpy(&prevMid,(BYTE *)(*newChildKey)+ridOffset,4);
		setPrevPointerIndex(fileHandle,newChildPage,prevMid);		//{ exchange pagnums between prev f new page, and rid f middle record
		memcpy((BYTE *)(*newChildKey)+ridOffset,&newChild,4);		//}


	}
	else // its a leaf , set the doibluy linked list accordingly
	{




	}





}

//care should be taken to ensure that the page being pssed in does not have any changes, as they may be lost when reorganizing.
//DOES NOT WRITE BACK THE PAGE.. caller has to do that.
RC IndexManager::insertRecordInIndex(FileHandle &fileHandle, const Attribute &attribute,INT32 virtualPgNum, void* page,const void *key, //
		void **newChildKey  )
{

	INT16 freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,0);
	INT16 requiredSpace = (attribute.type==2)?(4+intVal(key)):4;
	INT16 freeOffset=getFreeOffsetV(page),totalSlots=getSlotNoV(page);
	INT16 actualfreeSpace=4092-(totalSlots*4)-freeOffset;
	requiredSpace+=4;			// only fr the pagenum
	dbgnIXFn();

	dbgAssert(page!=NULL);

	if(freeSpace>(requiredSpace+4))
	{
		// we should be able to insert without split
		if(actualfreeSpace<(requiredSpace+4))
		{
			reOrganizePage(virtualPgNum,page);
			freeOffset=getFreeOffsetV(page);
			totalSlots=getSlotNoV(page);
		}
		dbgAssert((4092-(totalSlots*4)-freeOffset)>=requiredSpace+4);

		for(i=totalSlots;compare(key,getRecordAtSlot(page,i-1))>0 && i >0;i--)
			memcpy(getSlotOffA(page,i),getSlotOffA(page,i-1),2);
		memcpy((BYTE*)page+freeOffset,key,requiredSpace);
		memcpy(getSlotOffA(page,i),&freeOffSet,2);
		memcpy(getSlotLenA(page,i),&requiredSpace,2);
		totalSlots++;
		memcpy(getSlotNoA(page),&totalSlots,2);
		freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,requiredSpace+4);
	}
	else
	{
		INT32 newChild;
		//split the keys across one more index page
		insertIndexNode(newChild,fileHandle);
		void* newChildPage= malloc(PAGE_SIZE),middleKey=NULL;
		fileHandle.readPage(newChild,newChildPage);
		splitNode(virtualPgNum,page,newChild,newChildPage,newChildKey); /// should update the free space in the header
		getKeyAtSlot(fileHandle,newChildPage,middleKey,0);

		dbgAssert(middleKey!=NULL);
		if(compare(key,middleKey)>0)
		insertRecordInIndex(fileHandle,attribute,newChild,newChildPage,key,0);
		else
		insertRecordInIndex(fileHandle,attribute,virtualPgNum,page,key,0);
		fileHandle.writePage(newChild,newChildPage);
	}
	dbgnIXFnc();
}

// the page passed in here is written by the caller, it is NOT written to disk here..
RC IndexManager::insertRecordInLeaf(FileHandle &fileHandle, const Attribute &attribute,INT32 virtualPgNum, void* page,const void *key,//
		void **newChildKey)
{
	INT16 freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,0);
	INT16 requiredSpace = (attribute.type==2)?(4+intVal(key)):4;
	INT16 freeOffset=getFreeOffsetV(page),totalSlots=getSlotNoV(page);
	INT16 actualfreeSpace=4092-(totalSlots*4)-freeOffset;
	requiredSpace+=4;			// only fr the pagenum
	dbgnIXFn();
	if(freeSpace>(requiredSpace+4))
	{
		// we should be able to insert without split
		if(actualfreeSpace<(requiredSpace+4))
		{
			reOrganizePage(fileHandle,virtualPgNum);
			fileHandle.readPage(virtualPgNum,page);
			freeOffset=getFreeOffsetV(page);
			totalSlots=getSlotNoV(page);
		}
		totalSlots++;
		memcpy(getSlotNoA(page),&totalSlots,2);
		for(i=totalSlots;compare(key,(BYTE*)page+getSlotOffV(page,i-1))>0 && i >0;i--)
			memcpy(getSlotOffA(page,i),getSlotOffA(page,i-1),2);
		memcpy((BYTE*)page+freeOffset,key,requiredSpace);
		memcpy(getSlotOffA(page,i),&freeOffSet,2);
		memcpy(getSlotLenA(page,i),&requiredSpace,2);
		freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,requiredSpace+4);
	}
	else
	{
		//split the keys across one more index page
		insertIndexNode(newChild,fileHandle);
		void* newChildPage= malloc(PAGE_SIZE),middleKey=NULL;
		fileHandle.readPage(newChild,newChildPage);
		splitNode(page,newChildPage); /// should update the free space in the header
		getKeyAtSlot(fileHandle,newChildPage,middleKey,0);

		dbgAssert(middleKey!=NULL);
		if(compare(key,middleKey)>0)
		insertRecordInIndex(fileHandle,attribute,newChild,newChildPage,key,0);
		else
		insertRecordInIndex(fileHandle,attribute,virtualPgNum,page,key,0);
		fileHandle.writePage(newChild,newChildPage);
	}
	dbgnIXFnc();
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	INT32 root,newRoot;
	INT16 keyLength=0;
	void *newRootPage,*middleKey,*newChildPage;
	void **newChildKey=NULL;
	newChildKey= malloc(sizeof(void*));
	*newChildKey=NULL;
	getRoot(fileHandle,root);
	dbgnIXFn();
	if(root==-1)
	{
		insertLeafNode(root, fileHandle);
		updateRoot(fileHandle,root);
	}

	insertRecurseEntry(fileHandle,attribute, key,rid,root,newChildKey);

	if(*newChildKey==NULL)
		return 0;
// if new child == some value means root has split.... make nw index node.. insert the entry in to the index node , and update root.
	middleKey=*newChildKey;
	insertIndexNode(newRoot, fileHandle);
	updateRoot(fileHandle,newRoot);
	//insert the new child entry 's first value in to the new root node.
	newRootPage= malloc(PAGE_SIZE);
	fileHandle.readPage(newRoot,newRootPage);

	//now insert this newChildKey as a record wit the newChildPage into the 0th slot in the new root.
	// Also update the "lesser than" pointer of new root to the older root.
	*newChildKey=NULL;
	insertRecordInIndex(fileHandle,attribute,newRoot,newRootPage,middleKey,newChildKey);
	dbgAssert(newChildKey==NULL);
	setPrevPointerIndex(fileHandle,newRootPage,root);
	fileHandle.writePage(newRoot,newRootPage);
	dbgnIXFnc();
	free(newRootPage);
	return 0;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}

void IX_PrintError (RC rc)
{
}

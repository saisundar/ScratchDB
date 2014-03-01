
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
	memcpy(data,&((INT32)(-1)),4);
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

RC IndexManager::updateRoot(FileHandle &fileHandle,INT32 root)
{
	void* page=malloc(PAGE_SIZE);
	fileHandle.readPage(0,page);
	memcpy(page,&root,4);
	fileHandle.writePage(0,page);
	free(page);
	return 0;

}

// this routine assumes that page is a valid pointer with the actual page info, and key is a null pointer which will be allocated
// inside the routine
// returns the length of the new key, returns -1 if error or deleted.
INT32 IndexManager::getKeyAtSlot(FileHandle &fileHandle,void* page,void* key,INT16 slotNo)
{

	if(page==NULL)return -1;

	INT16 keyOffset = getSlotOffV(page,slotNo);
	INT16 keyLength = getSlotLenV(page,slotNo);

	key= malloc(keyLength);
	memcpy(key,(BYTE *)page+keyOffset,keyLength);

	return keyLength;

}

RC IndexManager::insertRecurseEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid,INT32 nodeNum,INT32 &newchildPage)
{
	void* page=malloc(PAGE_SIZE);

	if(fileHandle.readPage(nodeNum,page)!=0)
		dbgnIX("Oops wrong pagenum--does not exist or some error !"," ");
	if(isleaf(page))
	{




	}
	else
	{




	}

}

//care should be taken to ensure that the page being pssed in does not have any changes, as they may be lost when reorganizing.
//DOES NOT WRITE BACK THE PAGE.. caller has to do that.
RC IndexManager::insertRecordInIndex(FileHandle &fileHandle, const Attribute &attribute,INT32 virtualPgNum, void* page,const void *key,INT32& newChild)
{

	INT16 freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,0);
	INT16 requiredSpace = (attribute.type==2)?(4+intVal(key)):4;
	INT16 freeOffset=getFreeOffsetV(page),totalSlots=getSlotNoV(page);
	INT16 actualfreeSpace=4092-(totalSlots*4)-freeOffset;
	requiredSpace+=4;			// only fr the pagenum

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

		for(i=totalSlots;compare(key,(BYTE*)page+getSlotOffV(page,i-1))>0 && i >0;i--)
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
		//split the keys across one more index page
		insertIndexNode(newChild,fileHandle);
		void* newChildPage= malloc(PAGE_SIZE),middleKey=NULL;
		fileHandle.readPage(newChild,newChildPage);
		splitNode(page,newChildPage); /// should update the free space in the header
		getKeyAtSlot(fileHandle,newChildPage,middleKey,0)

		dbgAssert(middleKey!=NULL);
		if(compare(key,middleKey)>0)
		insertRecordInIndex(fileHandle,attribute,newChild,newChildPage,key,0);
		else
		insertRecordInIndex(fileHandle,attribute,virtualPgNum,page,key,0);
		fileHandle.writePage(newChild,newChildPage);
	}
}

RC IndexManager::insertRecordInLeaf(FileHandle &fileHandle, const Attribute &attribute,INT32 virtualPgNum, void* page,const void *key,INT32& newChild)
{
	INT16 freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,0);
	INT16 requiredSpace = (attribute.type==2)?(4+intVal(key)):4;
	INT16 freeOffset=getFreeOffsetV(page),totalSlots=getSlotNoV(page);
	INT16 actualfreeSpace=4092-(totalSlots*4)-freeOffset;
	requiredSpace+=4;			// only fr the pagenum

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
		getKeyAtSlot(fileHandle,newChildPage,middleKey,0)

		dbgAssert(middleKey!=NULL);
		if(compare(key,middleKey)>0)
		insertRecordInIndex(fileHandle,attribute,newChild,newChildPage,key,0);
		else
		insertRecordInIndex(fileHandle,attribute,virtualPgNum,page,key,0);
		fileHandle.writePage(newChild,newChildPage);
	}
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	INT32 root,newchild=-1,newRoot,newChild=-1;
	INT16 keyLength=0;
	void *newRootPage,*middleKey,*newChildPage;
	getRoot(fileHandle,root);

	if(root==-1)
	{
		insertLeafNode(root, fileHandle);
		updateRoot(fileHandle,root);
	}

	insertRecurseEntry(fileHandle,attribute, key,rid,root,newchild);

	if(newChild==-1)
		return 0;
// if new child == some value means root has split.... make nw index node.. insert the entry in to the index node , and update root.

	insertIndexNode(newRoot, fileHandle);
	updateRoot(fileHandle,root);
	//insert the new child entry 's first value in to the new root node.
	newRoot= malloc(PAGE_SIZE);
	newChildPage=malloc(PAGE_SIZE);
	fileHandle.readPage(newRoot,newRootPage);
	fileHandle.readPage(newChild,newChildPage);
	keyLength=getKeyAtSlot(fileHandle,newChildPage,middleKey,0);

	//now insert this new key as a record wit the newChildPage into the 0th slot in the new root.
	// Also update the "lesser than" pointer of new root to the older root.

	switch(attribute.attrType)
	{
	case 0:
	case 1:

		memcpy((BYTE *)middleKey+4,&newChild,4);
		memcpy((BYTE *)newPage+getFreeOffsetV(newPage),middleKey,8);
		memcpy(getSlotOffA(newPage,0);
		break;
	case 2:

		INT32 length= intVal(middleKey);
		memcpy((BYTE *)middleKey+4+length,&newChild,4);
	}

	insertRecordInIndex(fileHandle,attribute,virtualPgNum,newRootPage,middleKey,newRoot);










	return -1;
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

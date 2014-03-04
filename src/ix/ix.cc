
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
	INT32 temp=-1;
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->createFile(fileName.c_str())==-1)return -1;
	FileHandle fileHandle;
	if(pfm->openFile(fileName.c_str(), fileHandle)==-1)return -1;
	void* data = malloc(PAGE_SIZE);
	memcpy(data,&temp,4);
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

float compare(const void * keyIndex,const void* keyInput,AttrType type)  ////
{
	float diff;
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

	dbgnIXU("result of comparison",diff);
	return(diff);
}

// Inserts a new page of INDEX TYPE and updates the pageNum to contain the Virtual Page Number of the new Page added
// Also updates the header page to contain the correct amount of freeSpace
RC IndexManager::insertIndexNode(INT32& pageNum, FileHandle &fileHandle){
	dbgnIXFn();
	void* data = malloc(PAGE_SIZE);
	pageType(data) = (BYTE)1;
	getFreeOffsetV(data) = (INT16)12;
	getSlotNoV(data) = (INT16)0;
	setPrevPointerIndex(data,-1);
	fileHandle.appendPage(data);
	dbgnIX("node created of type",(pageType(data)==1)?"Index":"Leaf");
	pageNum = fileHandle.getNumberOfPages()-1;
	dbgnIXU("new number of pages / or nodes after index insertion",pageNum);
	fileHandle.updateFreeSpaceInHeader(pageNum,-12);
	free(data);
	dbgnIXFnc();
	return 0;
}

// Inserts a new page of LEAF TYPE and updates the pageNum to contain the Virtual Page Number of the new Page added
// Also updates the header page to contain the correct amount of freeSpace
RC IndexManager::insertLeafNode(INT32& pageNum, FileHandle &fileHandle){
	dbgnIXFn();
	void* data = malloc(PAGE_SIZE);
	pageType(data) = (BYTE)0;
	getFreeOffsetV(data) = (INT16)12;
	getSlotNoV(data) = (INT16)0;
	dbgnIX("node created of type",(pageType(data)==1)?"Index":"Leaf");
	setNextSiblingPointerLeaf(data,-1);
	setPrevSiblingPointerLeaf(data,-1);
	fileHandle.appendPage(data);
	pageNum = fileHandle.getNumberOfPages()-1;
	dbgnIXU("new number of pages / or nodes after leaf insertion",pageNum);
	fileHandle.updateFreeSpaceInHeader(pageNum,-12);
	free(data);
	dbgnIXFnc();
	return 0;
}

INT32 IndexManager::getPrevPointerIndex(void *page)
{
	dbgnIXFn();
	INT32 virtualPgNum;
	dbgAssert(*(BYTE *)page==1);
	memcpy(&virtualPgNum,(BYTE *)page+8,4);
	dbgnIXU("prev pointer of index",virtualPgNum);
	dbgnIXFnc();
	return virtualPgNum;
}
RC IndexManager::setPrevPointerIndex(void *page,INT32 virtualPgNum)
{
	dbgnIXFn();
	dbgAssert(*(BYTE *)page==1);
	memcpy((BYTE *)page+8,&virtualPgNum,4);
	dbgnIXU("prev pointer of index set to ",virtualPgNum);
	dbgnIXFnc();
	return 0;
}

RC IndexManager::setPrevSiblingPointerLeaf(void *page,INT32 virtualPgNum)
{
	dbgnIXFn();
	dbgAssert(*(BYTE *)page==0);
	memcpy((BYTE *)page+8,&virtualPgNum,4);
	dbgnIXU("prev pointer of Leaf set to",virtualPgNum);
	dbgnIXFnc();
	return 0;
}

RC IndexManager::setNextSiblingPointerLeaf(void *page,INT32 virtualPgNum)
{
	dbgnIXFn();
	dbgAssert(*(BYTE *)page==0);
	memcpy((BYTE *)page+8,&virtualPgNum,4);
	dbgnIXU("Next pointer of Leaf set to ",virtualPgNum);
	dbgnIXFnc();
	return 0;
}
INT32 IndexManager::getPrevSiblingPointerLeaf(void *page)
{
	dbgnIXFn();
	INT32 virtualPgNum;
	dbgAssert(*(BYTE *)page==0);
	memcpy(&virtualPgNum,(BYTE *)page+8,4);
	dbgnIXU("prev pointer of Leaf read as",virtualPgNum);
	dbgnIXFnc();
	return virtualPgNum;

}

INT32 IndexManager::getNextSiblingPointerLeaf(void *page)
{
	dbgnIXFn();
	INT32 virtualPgNum;
	dbgAssert(*(BYTE *)page==0);
	memcpy(&virtualPgNum,(BYTE *)page+8,4);
	dbgnIXU("Next pointer of Leaf read as",virtualPgNum);
	dbgnIXFnc();
	return virtualPgNum;
}
RC IndexManager::updateRoot(FileHandle &fileHandle,INT32 root)
{
	dbgnIXFn();
	void* page=malloc(PAGE_SIZE);
	fileHandle.readPage(0,page);
	memcpy(page,&root,4);
	dbgnIXU("New value of Root has been updated as ",root);
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
	dbgnIXU("Offse of the key is",keyOffset);
	dbgnIXU("Offse of the key is",keyLength);
	dbgnIXFnc();
	return keyLength;

}

RC IndexManager::insertRecurseEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid,INT32 nodeNum,	//
		void **newChildKey)
{
	void* page=malloc(PAGE_SIZE);
	dbgnIXFn();
	dbgnIX("Node number",nodeNum);
	if(fileHandle.readPage(nodeNum,page)!=0)
		dbgnIX("Oops wrong pagenum--does not exist or some error !"," ");
	RC rc;
	INT16 totalSlots = getSlotNoV(page);
	INT16 start = 0;
	INT16 end = totalSlots-1;
	INT16 mid = (start+end)/2;
	INT32 root=-1;
	dbgnIX("node is of type",(pageType(page)==1)?"Index":"Leaf");
	if(!(pageType(page)==1))
	{
		dbgnIX("Reached a Leaf , so insert in to the leaf"," ");
		insertRecordInLeaf(fileHandle,attribute,nodeNum,page,key,newChildKey);
		fileHandle.writePage(nodeNum,page);
		dbgnIXFnc();
		return 0;
	}
	else
	{
		if(compare(getRecordAtSlot(page,start),key,attribute.type)<0)
		{
			dbgnIX("key < start==>fetching prev pointer from index","");
			root = getPrevPointerIndex(page);
		}
		else if(compare(getRecordAtSlot(page,end),key,attribute.type) >= 0)
		{
			dbgnIX("key > end==>fetching next pointer from end","");
			root = getIndexValueAtOffset(page, getSlotOffV(page,end), attribute.type);
		}
		else{
			dbgnIX("starting binary search on the slots","");
			while(start<end)
			{

				if(compare(getRecordAtSlot(page,mid),key,attribute.type) == 0)
				{
					dbgnIX("key FOUND at slot",mid);
					root = getIndexValueAtOffset(page,getSlotOffV(page,mid), attribute.type);
					dbgnIX("next level node",root);
					break;
				}
				else if(compare(getRecordAtSlot(page,mid),key,attribute.type) < 0)
					end = mid-1;
				else if(compare(getRecordAtSlot(page,mid),key,attribute.type) > 0)
					start = mid+1;
				mid = (start+end)/2;
				dbgnIX("key not found at mid, new mid=",mid);
			}
			if(root==-1)
			{
				if(compare(getRecordAtSlot(page,mid),key,attribute.type) <=0)
					root=  getIndexValueAtOffset(page,getSlotOffV(page,mid-1), attribute.type);
				else if(compare(getRecordAtSlot(page,mid),key,attribute.type) > 0)
					root = getIndexValueAtOffset(page,getSlotOffV(page,mid), attribute.type);
				dbgnIX("next level node",root);
			}
		}
		rc=insertRecurseEntry(fileHandle,attribute, key,rid,root,newChildKey);// i will need to explicitly set newcildkey to null after processing

		if(rc)
			dbgnIX("oops some error in insert recurse in index"," ");

		if(*newChildKey!=NULL)
		{
			dbgnIX("newChildkey created..handling it here","");
			void *middleKey=NULL;
			middleKey=*newChildKey;
			*newChildKey=NULL;
			insertRecordInIndex(fileHandle,attribute,nodeNum,page,middleKey,newChildKey);
			fileHandle.writePage(nodeNum,page);
			dbgnIXFnc();
			free(middleKey);
			return 0;
		}
	}
	dbgnIXFnc();
	return 0;
}

//aLL in mmeory operations --- NO I/O at all here....
RC IndexManager::reOrganizePage(FileHandle &fileHandle,INT32 virtualPgNum, void* page)
{
	dbgnIXFn();
	dbgnIXU("Filename",fileHandle.fileName);
	void *newPage=malloc(PAGE_SIZE);
	INT16 freeOffset,origOffset,origLength=0,totalSlots,currSlot=0,newSlot=0;
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

RC IndexManager::splitNode(FileHandle &fileHandle,INT32 virtualPgNum,void *page,INT32 newChild,void* newChildPage,void **newChildKey,const Attribute &attribute)
{
	dbgnIXFn();
	reOrganizePage(fileHandle,virtualPgNum,page);
	bool isLeaf=!pageType(page);
	INT16 oldfreeOffset=getFreeOffsetV(page),freeOffset,totalSlots=getSlotNoV(page),prevMid=-1,startSlot;
	INT16 middleSlot=totalSlots/2;
	INT16 midOffSet=getSlotOffV(page,middleSlot),midLength=getSlotLenV(page,middleSlot);
	INT16 ridOffset=-1,origOffset,origLength;
	INT32 next,currSlot,newSlot=0;
	INT16 freeSpaceIncNew,freeSpaceIncOrig;
	*newChildKey=malloc(midLength);
	memcpy(*newChildKey,getRecordAtSlot(page,middleSlot),midLength);

	if(!isLeaf)
	{
		dbgnIXU("Split node claled on Index ","");
		ridOffset= (attribute.type==2)?(4+intVal(*newChildKey)):4;
		memcpy(&prevMid,(BYTE *)(*newChildKey)+ridOffset,4);
		setPrevPointerIndex(newChildPage,prevMid);		//{ exchange pagnums between prev f new page, and rid f middle record
		memcpy((BYTE *)(*newChildKey)+ridOffset,&newChild,4);		//}
		startSlot=middleSlot+1;
	}
	else // its a leaf , set the doubly linked list accordingly
	{
		dbgnIXU("Split node claled on Leaf ","");
		void * nextPage=malloc(PAGE_SIZE);
		next=getNextSiblingPointerLeaf(page);

		setNextSiblingPointerLeaf(page,newChild);
		setPrevSiblingPointerLeaf(newChildPage,virtualPgNum);
		setNextSiblingPointerLeaf(newChildPage,next);
		if(next!=-1)
		{
			fileHandle.readPage(next,nextPage);
			setPrevSiblingPointerLeaf(nextPage,newChild);
			fileHandle.writePage(next,nextPage);
			free(nextPage);
		}
		memcpy((BYTE *)(*newChildKey)+ridOffset,&newChild,4);
		startSlot=middleSlot;
	}
	freeOffset=12;
	dbgnIXU("Original free offset",origOffset);
	for(currSlot=startSlot;currSlot<totalSlots;currSlot++)
	{
		origOffset=getSlotOffV(page,currSlot);
		origLength=getSlotLenV(page,currSlot);
		dbgnIXU("Current slot no",currSlot);
		dbgnIXU("Original offset",origOffset);
		dbgnIXU("Original length",origLength);
		// Empty slot
		dbgAssert( origOffset != -1)
		// Original Record
		if(origLength >= 0)
		{
			memcpy((BYTE *)newChildPage+freeOffset,(BYTE *)page+origOffset,origLength);   //copy the record
			memcpy(getSlotOffA(newChildPage,newSlot),&freeOffset,2);			 		 //copy new offset
			memcpy(getSlotLenA(newChildPage,newSlot),&origLength,2);
			newSlot++;
			freeOffset+=origLength;
			dbgnIXU("old slot moved to slot",newSlot);
			dbgnIXU("New freeOffset",freeOffset);
		}
	}
	//update the free offste and no of slots for the new page
	memcpy(getFreeOffsetA(newChildPage),&freeOffset,2);
	dbgnIXU("The new free offset in new PAge after splitting is ",getFreeOffsetV(newChildPage));
	memcpy(getSlotNoA(newChildPage),&newSlot,2);
	dbgnIXU("The new no of Slots in new PAge after splitting is ",getSlotNoV(newChildPage));

	//update free space for new page
	freeSpaceIncNew=freeOffset-12+newSlot*4;
	fileHandle.updateFreeSpaceInHeader(newChild,-freeSpaceIncNew);

	//update the free offste and no of slots for the original page
	memcpy(getFreeOffsetA(page),&midOffSet,2);
	dbgnIXU("The new free offset in new PAge after splitting is ",getFreeOffsetV(page));
	memcpy(getSlotNoA(page),&middleSlot,2);
	dbgnIXU("The new no of Slots in new PAge after splitting is ",getSlotNoV(page));

	//update free space for orig page
	freeSpaceIncOrig=oldfreeOffset-midOffSet+(totalSlots-middleSlot)*4;
	fileHandle.updateFreeSpaceInHeader(virtualPgNum,freeSpaceIncOrig);

	dbgnIXFnc();
	return 0;
}

//care should be taken to ensure that the page being pssed in does not have any changes, as they may be lost when reorganizing.
//DOES NOT WRITE BACK THE PAGE.. caller has to do that.
RC IndexManager::insertRecordInIndex(FileHandle &fileHandle, const Attribute &attribute,INT32 virtualPgNum, void* page,const void *key, //
		void **newChildKey  )
{

	INT16 freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,0);
	INT16 requiredSpace = (attribute.type==2)?(4+intVal(key)):4;
	INT16 freeOffset=getFreeOffsetV(page),totalSlots=getSlotNoV(page);
	INT16 actualfreeSpace=4092-(totalSlots*4)-freeOffset,i;
	requiredSpace+=4;			// only fr the pagenum
	dbgnIXFn();

	dbgAssert(page!=NULL);

	if(freeSpace>(requiredSpace+4))
	{
		dbgnIX("Key can be accomodated in the same inde without split","");
		// we should be able to insert without split
		if(actualfreeSpace<(requiredSpace+4))
		{
			dbgnIX("Need a Reorganize however","");
			reOrganizePage(fileHandle,virtualPgNum,page);
			freeOffset=getFreeOffsetV(page);
			totalSlots=getSlotNoV(page);
		}
		dbgAssert((4092-(totalSlots*4)-freeOffset)>=requiredSpace+4);

		for(i=totalSlots;compare(key,getRecordAtSlot(page,i-1),attribute.type)>0 && i >0;i--)
			memcpy(getSlotOffA(page,i),getSlotOffA(page,i-1),2);
		dbgnIX("inserting record in slot",i);
		memcpy((BYTE*)page+freeOffset,key,requiredSpace);
		memcpy(getSlotOffA(page,i),&freeOffset,2);
		memcpy(getSlotLenA(page,i),&requiredSpace,2);
		totalSlots++;
		memcpy(getSlotNoA(page),&totalSlots,2);
		freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,requiredSpace+4);
	}
	else
	{
		INT32 newChild;
		dbgnIX("Key accomodation mandates a SPLIT","");
		//split the keys across one more index page
		insertIndexNode(newChild,fileHandle);
		void* newChildPage= malloc(PAGE_SIZE),*middleKey=NULL;
		fileHandle.readPage(newChild,newChildPage);
		splitNode(fileHandle, virtualPgNum, page, newChild, newChildPage, newChildKey, attribute); /// should update the free space in the header
		middleKey=*newChildKey;

		dbgAssert(middleKey!=NULL);
		if(compare(key,middleKey,attribute.type)>0)
			insertRecordInIndex(fileHandle,attribute,newChild,newChildPage,key,0);
		else
			insertRecordInIndex(fileHandle,attribute,virtualPgNum,page,key,0);
		fileHandle.writePage(newChild,newChildPage);
	}
	dbgnIXFnc();
	return 0;
}

// the page passed in here is written by the caller, it is NOT written to disk here..
RC IndexManager::insertRecordInLeaf(FileHandle &fileHandle, const Attribute &attribute,INT32 virtualPgNum, void* page,const void *key, //
		void **newChildKey  )
{
	dbgnIXFn();
	INT16 freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,0);
	INT16 requiredSpace = (attribute.type==2)?(4+intVal(key)):4;
	INT16 freeOffset=getFreeOffsetV(page),totalSlots=getSlotNoV(page);
	INT16 actualfreeSpace=4092-(totalSlots*4)-freeOffset,i;
	requiredSpace+=6;			// only fr the pagenum


	dbgAssert(page!=NULL);

	dbgnIX("free spacein the page",freeSpace);
	dbgnIX(" space required for th record",requiredSpace);
	if(freeSpace>(requiredSpace+4))
	{
		dbgnIX("Key can be accomodated in the same Leaf without split","");
		// we should be able to insert without split
		if(actualfreeSpace<(requiredSpace+4))
		{
			dbgnIX("Need a Reorganize however","");
			reOrganizePage(fileHandle,virtualPgNum,page);
			freeOffset=getFreeOffsetV(page);
			totalSlots=getSlotNoV(page);
		}
		dbgAssert((4092-(totalSlots*4)-freeOffset)>=requiredSpace+4);

		for(i=totalSlots;compare(key,getRecordAtSlot(page,i-1),attribute.type)>0 && i >0;i--)
			memcpy(getSlotOffA(page,i),getSlotOffA(page,i-1),2);
		dbgnIX("inserting record in slot",i);
		memcpy((BYTE*)page+freeOffset,key,requiredSpace);
		memcpy(getSlotOffA(page,i),&freeOffset,2);
		memcpy(getSlotLenA(page,i),&requiredSpace,2);
		totalSlots++;
		memcpy(getSlotNoA(page),&totalSlots,2);
		freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,requiredSpace+4);
	}
	else
	{
		INT32 newChild;
		dbgnIX("Key accomodation mandates a SPLIT","");
		//split the keys across one more index page
		insertIndexNode(newChild,fileHandle);
		void* newChildPage= malloc(PAGE_SIZE),*middleKey=NULL;
		fileHandle.readPage(newChild,newChildPage);
		splitNode(fileHandle,virtualPgNum,page,newChild,newChildPage,newChildKey,attribute); /// should update the free space in the header
		middleKey=*newChildKey;

		dbgAssert(middleKey!=NULL);
		if(compare(key,middleKey,attribute.type)>0)
			insertRecordInLeaf(fileHandle,attribute,newChild,newChildPage,key,0);
		else
			insertRecordInLeaf(fileHandle,attribute,virtualPgNum,page,key,0);
		fileHandle.writePage(newChild,newChildPage);
	}
	dbgnIXFnc();
	return 0;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	INT32 root,newRoot,strLen;
	INT32 searchKeyLength;
	void *newRootPage,*middleKey;
	void **newChildKey=NULL;
	INT32 pagNum=rid.pageNum;
	INT16 slotNo=rid.slotNum;
	void* tempKey;

	newChildKey= (void **)malloc(sizeof(void*));
	*newChildKey=NULL;
	dbgnIX("inserting entry","");
	getRoot(fileHandle,root);
	dbgnIXFn();
	if(root==-1)
	{
		dbgnIX("Cross your fingers , here begins the insertions!!!!!!!! Adding Root","");
		insertLeafNode(root, fileHandle);
		updateRoot(fileHandle,root);
	}

	if(attribute.type==2)
	{
		strLen=(*(INT32*)(key));
		strLen++;
		searchKeyLength = strLen+4;
		tempKey = malloc(searchKeyLength+6);
		memcpy(tempKey,key,searchKeyLength-1);
		memcpy(tempKey,&strLen,4);
		*((BYTE*)tempKey + (searchKeyLength-1)) = (BYTE)0;
		memcpy((BYTE*)tempKey +searchKeyLength,&pagNum,4);
		memcpy((BYTE*)tempKey +searchKeyLength+4,&slotNo,2);

	}
	else
	{
		searchKeyLength=4;
		tempKey = malloc(searchKeyLength+6);
		memcpy(tempKey,key,searchKeyLength);
		memcpy((BYTE*)tempKey +searchKeyLength,&pagNum,4);
		memcpy((BYTE*)tempKey +searchKeyLength+4,&slotNo,2);
	}

	insertRecurseEntry(fileHandle,attribute, tempKey,rid,root,newChildKey);

	if(*newChildKey==NULL)
	{

		dbgnIX("new child key is not found, so no split occurred","");
		return 0;
	}
	// if new childKey == some value means root has split.... make nw index node.. insert the entry in to the index node , and update root.
	middleKey=*newChildKey;
	dbgnIX("new child key IS FOUND, so split occurred","");
	dbgnIX("we are levelling up BOYS!!!!!!Tree becomes one level taller :)","");
	insertIndexNode(newRoot, fileHandle);
	updateRoot(fileHandle,newRoot);
	//insert the new child entry 's first value in to the new root node.
	newRootPage= malloc(PAGE_SIZE);
	fileHandle.readPage(newRoot,newRootPage);

	//now insert this newChildKey as a record wit the newChildPage into the 0th slot in the new root.
	// Also update the "lesser than" pointer of new root to the older root.
	*newChildKey=NULL;
	insertRecordInIndex(fileHandle,attribute,newRoot,newRootPage,middleKey,newChildKey);
	dbgAssert(*newChildKey==NULL);
	setPrevPointerIndex(newRootPage,root);
	fileHandle.writePage(newRoot,newRootPage);
	dbgnIXFnc();
	free(newRootPage);
	free(middleKey);
	free(newChildKey);
	free(tempKey);
	return 0;
}
RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	dbgnIXFn();
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
		INT32 newKeyLength = searchKeyLength + 1;
		tempLowKey = malloc(searchKeyLength+5); // THIS IS FREED IN THIS FUNCTION ITSELF
		BYTE* temp = (BYTE*)tempLowKey;
		memcpy(temp,& newKeyLength, 4);
		memcpy(temp + 4,key,searchKeyLength);
		*((BYTE*)tempLowKey + (searchKeyLength+4)) = (BYTE)0;
	}

	// FIND THE LEAF PAGE WHERE ENTRY IS STORED
	findLeafPage(fileHandle, pageData, root, tempLowKey, attribute.type);

	// DELETE THAT ENTRY FROM THE LEAF PAGE
	INT16 freeSpaceIncrease = 0;
	RC rc = deleteEntryInLeaf(fileHandle, attribute, tempLowKey, rid, root, pageData, freeSpaceIncrease);
	if(rc==1){
		free(pageData);
		free(tempLowKey);
		dbgnIXFnc();
		return 1;
	}
	// WRITE THAT PAGE BACK
	fileHandle.writePage(root, pageData);

	// UPDATE FREE SPACE IN HEADER
	fileHandle.updateFreeSpaceInHeader(root,freeSpaceIncrease);

	free(pageData);
	free(tempLowKey);
	dbgnIXFnc();
	return 0;
}

RC IndexManager::deleteEntryInLeaf(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid, INT32 root, void* pageData, INT16& freeSpaceIncrease){
	dbgnIXFn();
	INT16 totalSlots = getSlotNoV(pageData);
	if(totalSlots==0){
		dbgnIX("Zero slots in this page","return not found ");
		return 1;
	}

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
			dbgnIX("Slot to be deleted found",mid);
			// Handles the case where last slot is deleted, In this case it makes the last slot existant or reduces the total number of slots to 0
			INT16 reducedSlotsBy = 0;
			if(mid == totalSlots-1){
				while(true){
					reducedSlotsBy++;
					mid = mid-1;
					if(mid<0)break;
					if(getSlotOffV(pageData,mid)!=-1)break;
				}
				getSlotNoV(pageData) = (INT16)getSlotNoV(pageData)-reducedSlotsBy;
			}
			else getSlotOffV(pageData,mid) = (INT16)-1;
			dbgnIX("Slots reduce by",reducedSlotsBy);
			freeSpaceIncrease = (getSlotLenV(pageData,mid)+4);
			dbgnIX("Free Space Increases by",freeSpaceIncrease);
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
	dbgnIX("Entry not found","");
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
	if(fileHandle.stream==0){
		dbgnIX("File does not exist","");
		return -1;
	}
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
	getRoot(fileHandle,root);
	if(root == -1) return -1;
	void* pageData = malloc(PAGE_SIZE); // THIS WILL BE FREED BY SCANITERATOR
	fileHandle.readPage(root, pageData);

	// Make new format for Low Key Value
	void* tempLowKey;
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(lowKey));
		INT32 newKeyLength = searchKeyLength + 1;
		tempLowKey = malloc(searchKeyLength+5); // THIS IS FREED IN THIS FUNCTION ITSELF
		BYTE* temp = (BYTE*)tempLowKey;
		memcpy(temp,& newKeyLength, 4);
		memcpy(temp + 4,lowKey,searchKeyLength);
		*((BYTE*)tempLowKey + (searchKeyLength+4)) = (BYTE)0;
	}

	// Make new format for High Key Value
	if(attribute.type==2){
		int searchKeyLength = (*(INT32*)(highKey));
		INT32 newKeyLength = searchKeyLength + 1;
		ix_ScanIterator.highKey = malloc(searchKeyLength+5); // THIS WILL BE FREED BY SCANITERATOR !
		BYTE* temp = (BYTE*)ix_ScanIterator.highKey;
		memcpy(temp,& newKeyLength, 4);
		memcpy(temp + 4,highKey,searchKeyLength);
		*((BYTE*)ix_ScanIterator.highKey + (searchKeyLength+4)) = (BYTE)0;
	}

	if(highKey!=NULL && lowKey !=NULL){
		// Return error if higKey < lowKey
		float compare = compare(tempLowKey,ix_ScanIterator.highKey,attribute.type);
		dbgnIX("Comparing high and low key","");
		if(compare<0){
			dbgnIX("High key is less than low key","");
			dbgnIXFnc();
			return -1;
		}
	}

	// Find Leaf Page where possible first record is present
	findLeafPage(fileHandle, pageData, root, tempLowKey, attribute.type);
	findLowSatisfyingEntry(fileHandle, pageData, root, tempLowKey, lowKeyInclusive, ix_ScanIterator.highKey, highKeyInclusive, attribute.type, ix_ScanIterator.nextRid);
	dbgnIX("RID Pagenum",ix_ScanIterator.nextRid.pageNum)
	dbgnIX("RID SlotNum",ix_ScanIterator.nextRid.slotNum)
	ix_ScanIterator.leafPage = pageData;
	ix_ScanIterator.totalSlotsInCurrPage = getSlotNoV(pageData);
	dbgnIXFnc();
	return 0;
}

INT32 IndexManager::findLowSatisfyingEntry(FileHandle& fileHandle, void* pageData, INT32& root, void* lowKey, bool lowKeyInclusive, void* highKey, bool highKeyInclusive,//
		AttrType type, RID& nextRid){
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
	fileHandle.stream=0;
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

	if( (highKey==NULL) || (compare((const void *)((BYTE*)leafPage+recordOffset),(const void *) highKey, type) >= 0)){
		if((compare((const void *)((BYTE*)leafPage+recordOffset),(const void *) highKey, type)==0) && highKeyInclusive==false){
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
	fileHandle.stream=0;
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
			root = *((INT32*)((BYTE*)pageData + 8));
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

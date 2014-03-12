
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
	if(pfm->createFile(fileName.c_str())==-1)
	{
		dbgnIX("create file failed from pfm","");
		return -1;
	}
	FileHandle fileHandle;
	if(pfm->openFile(fileName.c_str(), fileHandle)==-1){
		dbgnIX("open file failed from pfm","");
		return -1;
	}
	void* data = malloc(PAGE_SIZE);
	memcpy(data,&temp,4);
	fileHandle.appendPage(data);
	if(pfm->closeFile(fileHandle)==-1){
		dbgnIX("close file failed from pfm","");
		return -1;
	}
	free(data);
	dbgnIXFnc();
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

bool compRID(const void * keyIndex,const void* keyInput,AttrType type)  ////
{
	dbgnIXFn();
	dbgnIXU("type of attribute tobe compared",type);
	INT32 diff=-1;
	switch(type)
	{
	case 0:
	case 1:
		diff=memcmp((BYTE*)keyIndex+4,(BYTE*)keyInput+4,6);
		break;
	case 2:
		INT32 len=intVal(keyIndex);
		diff=memcmp((BYTE*)keyIndex+4+len,(BYTE*)keyInput+4+len,6);
		break;
	}

	dbgnIXU("result of comparison",diff);
	if(diff==0)
		return true;
	return false;

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
		dbgnIXU("inputKey","keyFrom Disk");
		dbgnIXU(intVal(keyInput),intVal(keyIndex));
		break;
	case 1:
		dbgnIXU("Comparing the floats here !","");
		dbgnIXU("inputKey","keyFrom Disk");
		dbgnIXU((*((float *)keyInput)),(*((float *)keyIndex)));
		diff=*((float *)keyInput)-*((float *)keyIndex);

		if(modlus(diff)<0.000001)diff=0;
		break;
	case 2:
		diff= strcmp((char *)keyInput+4,(char *)keyIndex+4);
		dbgnIXU("inputKey","keyFrom Disk");
		dbgnIXU(intVal(keyInput),intVal(keyIndex));
		dbgnIXU((char*)keyInput+4,(char*)keyIndex+4);
		break;
	}

	dbgnIXU("result of comparison",diff);
	dbgnIXFnc();
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
	memcpy((BYTE *)page+4,&virtualPgNum,4);
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
	memcpy(&virtualPgNum,(BYTE *)page+4,4);
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
	{
		dbgnIX("Oops wrong pagenum--does not exist or some error !"," ");
		free(page);
		return -1;
	}
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
		dbgnIX("value to be isnerted",intVal(key));
		rc=insertRecordInLeaf(fileHandle,attribute,nodeNum,page,key,newChildKey);
		if(rc==RECORD_EXISTS_ALREADY)
		{
			dbgnIX("record already present..","");
			free (page);
			dbgnIXFnc();
			return rc;
		}
		fileHandle.writePage(nodeNum,page);
		free (page);
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
				if(compare(getRecordAtSlot(page,mid),key,attribute.type) <0)
					root=  getIndexValueAtOffset(page,getSlotOffV(page,mid-1), attribute.type);
				else 
					root = getIndexValueAtOffset(page,getSlotOffV(page,mid), attribute.type);
				dbgnIX("next level node",root);
			}
		}
		rc=insertRecurseEntry(fileHandle,attribute, key,rid,root,newChildKey);// i will need to explicitly set newcildkey to null after processing

		if(rc)
		{
			if(rc==RECORD_EXISTS_ALREADY)
			{
				dbgnIX("record already present..","");
			}
			else
			{
				dbgnIX("newChildkey created..handling it here","");
			}
			free(page);
			dbgnIXFnc();
			return rc;
		}

		if(*newChildKey!=NULL)
		{
			dbgnIX("newChildkey created..handling it here","");
			void *middleKey=NULL;
			middleKey=*newChildKey;
			*newChildKey=NULL;
			insertRecordInIndex(fileHandle,attribute,nodeNum,page,middleKey,newChildKey);
			fileHandle.writePage(nodeNum,page);

			free(page);
			free(middleKey);
			dbgnIXFnc();
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
	INT16 freeOffset,origOffset,origLength=0,totalSlots,currSlot=0,newSlot=0,nonEmpty=0;
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
		if( origOffset == (INT16)(-1))
			continue;
		nonEmpty++;
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
	dbgAssert(nonEmpty==newSlot);
	dbgnIXU("the total number of slot s originally was",totalSlots);

	//everything after this is just for the logging purposes
	freeOffset=(4092-(newSlot*4)-freeOffset);
	dbgnIXU("new free space agt the end of the file ",freeOffset);
	origOffset=fileHandle.updateFreeSpaceInHeader(virtualPgNum,0);
	dbgnIXU("free space in header is ",origOffset);
	dbgAssert(freeOffset==origOffset);		//shuld be equal
	free(newPage);
	dbgnIXFnc();
	return 0;
}

INT16 IndexManager::firstBatchDupSlot(INT32 virtualPgNum,void *page,const Attribute &attribute)
{

	INT16 freeOffset,totalSlots=getSlotNoV(page),prevMid=-1,currSlot=-1,prevOff,currOff;
	INT16 middleSlot=totalSlots/2;
	dbgnIXFn();

	for(currSlot=middleSlot;currSlot>0;currSlot--)
	{
		prevOff=getSlotOffV(page,(currSlot-1));
		currOff=getSlotOffV(page,currSlot);
		if(compare((BYTE *)page+prevOff,(BYTE *)page+currOff,attribute.type)!=0)
			break;
	}

	if(currSlot==0)
	{
		for(currSlot=middleSlot;currSlot<totalSlots-1;currSlot++)
		{
			prevOff=getSlotOffV(page,currSlot);
			currOff=getSlotOffV(page,(currSlot+1));
			if(compare((BYTE *)page+prevOff,(BYTE *)page+currOff,attribute.type)!=0)
				break;
		}
	}

	dbgnIXU("value of the middle slot being returned is ",currSlot);
	dbgnIXFnc();
	return currSlot;
}

RC IndexManager::splitNode(FileHandle &fileHandle,INT32 virtualPgNum,void *page,INT32 newChild,void* newChildPage,void **newChildKey,const Attribute &attribute)
{
	dbgnIXFn();
	reOrganizePage(fileHandle,virtualPgNum,page);
	bool isLeaf=!pageType(page);
	INT16 oldfreeOffset=getFreeOffsetV(page),freeOffset,totalSlots=getSlotNoV(page),prevMid=-1,startSlot;
	INT16 middleSlot=firstBatchDupSlot(virtualPgNum,page,attribute);
	INT16 midOffSet=getSlotOffV(page,middleSlot),midLength=getSlotLenV(page,middleSlot);
	INT16 ridOffset=-1,origOffset,origLength;
	INT32 next,currSlot,newSlot=0;
	INT16 freeSpaceIncNew,freeSpaceIncOrig;
	*newChildKey=malloc(midLength);
	memcpy(*newChildKey,getRecordAtSlot(page,middleSlot),midLength);
	ridOffset= (attribute.type==2)?(4+intVal(*newChildKey)):4;
	if(!isLeaf)
	{
		dbgnIXU("Split node claled on Index ","");
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
		memcpy((BYTE *)(*newChildKey)+ridOffset,&newChild,4);
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
		{
			memcpy(getSlotOffA(page,i),getSlotOffA(page,i-1),2);
			memcpy(getSlotLenA(page,i),getSlotLenA(page,i-1),2);
		}
		dbgnIX("inserting record in slot",i);
		memcpy((BYTE*)page+freeOffset,key,requiredSpace);
		memcpy(getSlotOffA(page,i),&freeOffset,2);
		memcpy(getSlotLenA(page,i),&requiredSpace,2);
		totalSlots++;
		freeOffset+=requiredSpace;
		memcpy(getFreeOffsetA(page),&freeOffset,2);
		memcpy(getSlotNoA(page),&totalSlots,2);
		freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,-(requiredSpace+4));
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
		free(newChildPage);
	}
	dbgnIXFnc();
	return 0;
}

INT16 returnLeftMid(void *page,INT16 mid,INT16 start)
{
	dbgnIXFn();
	for(;mid>=start;mid--)
	{
		if(getSlotOffV(page,mid)==-1)
			continue;
		break;
	}

	if(mid<start)return (INT16)(-1);

	dbgnIX("left most non zero offfset mid=",mid);
	dbgnIXFnc();
	return mid;

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
	bool found=false;

	dbgAssert(page!=NULL);
	dbgnIX("value to be isnerted",intVal(key));
	dbgnIX("free space in the page",freeSpace);
	dbgnIX(" space required for the record",requiredSpace);

	//do a search for the record first...only if it does not exist,insert...

	INT16 start=0,end=totalSlots-1,mid=(start+end)/2,midOff,currSlot;
	float comp;

	for(mid=(start+end)/2;start<end;mid=(start+end)/2)
	{
		mid=returnLeftMid(page,mid,start);
		if(mid==-1){
			start=mid+1;
			continue;
		}
		midOff=getSlotOffV(page,mid);
		comp=compare((const void*)key,(const void*)((BYTE *)page+midOff),attribute.type);
		if(comp==0)
		{
			end=mid;
		}
		else if(comp>0) //mid >key
		{
			start=mid+1;
		}
		else //comp<0 mid<key
		{
			end=mid-1;
		}
	}
	midOff=getSlotOffV(page,mid);

	if(compare((const void*)key,(const void*)((BYTE *)page+midOff),attribute.type)==0)
	{
		dbgnIX("record with same key is already present at slot",mid);
		dbgnIX("now checking whether the same record has also been inserted","");
		found=false;
		for(currSlot=mid;currSlot<totalSlots;currSlot++)
		{
			midOff=getSlotOffV(page,currSlot);
			if(midOff==-1)continue;
			if(compare((const void*)key,(const void*)((BYTE *)page+midOff),attribute.type)==0)
			{
				if(compRID((const void*)key,(const void*)((BYTE *)page+midOff),attribute.type))
				{
					found=true;
					break;
				}
			}
			else
				break;
		}
	}
	if(found)
	{
		dbgnIX("the exact key with same RID is found..","return error");
		return 2;
	}

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
		dbgnIX("Total number of slots",totalSlots);
		INT16 slotOff=-1;
		void *addr=NULL;
		//handle deleted slots here
		for(i=totalSlots;i>0;i--)
		{
			if(getSlotOffV(page,i-1)==-1)
			{
				memcpy(getSlotOffA(page,i),getSlotOffA(page,i-1),2);
				memcpy(getSlotLenA(page,i),getSlotLenA(page,i-1),2);
				continue;
			}
			slotOff=getSlotOffV(page,i-1);
			dbgnIX("Slot offset",slotOff);
			addr=(BYTE *)page+slotOff;
			if(compare(key,addr,attribute.type)<=0)
				break;
			memcpy(getSlotOffA(page,i),getSlotOffA(page,i-1),2);
			memcpy(getSlotLenA(page,i),getSlotLenA(page,i-1),2);
		}
		memcpy((BYTE*)page+freeOffset,key,requiredSpace);
		INT32 pageNum;INT16 slot;
		memcpy(&pageNum,(BYTE*)page+freeOffset+4,4);
		memcpy(&slot,(BYTE*)page+freeOffset+8,2);
		dbgnIX("RID page of record inserted in leaf",pageNum);
		dbgnIX("RID slot of record inserted in leaf",slot);
		memcpy(getSlotOffA(page,i),&freeOffset,2);
		memcpy(getSlotLenA(page,i),&requiredSpace,2);
		freeOffset+=requiredSpace;
		memcpy(getFreeOffsetA(page),&freeOffset,2);
		totalSlots++;
		memcpy(getSlotNoA(page),&totalSlots,2);
		freeSpace=fileHandle.updateFreeSpaceInHeader(virtualPgNum,-(requiredSpace+4));
	}
	else
	{
		INT32 newChild;
		dbgnIX("Key accomodation mandates a SPLIT","");
		//split the keys across one more index page
		insertLeafNode(newChild,fileHandle);
		void* newChildPage= malloc(PAGE_SIZE),*middleKey=NULL;
		fileHandle.readPage(newChild,newChildPage);
		splitNode(fileHandle,virtualPgNum,page,newChild,newChildPage,newChildKey,attribute); /// should update the free space in the header
		middleKey=*newChildKey;

		dbgAssert(middleKey!=NULL);
		if(compare(key,middleKey,attribute.type)<=0)
			insertRecordInLeaf(fileHandle,attribute,newChild,newChildPage,key,0);
		else
			insertRecordInLeaf(fileHandle,attribute,virtualPgNum,page,key,0);
		fileHandle.writePage(newChild,newChildPage);
		free(newChildPage);
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
	RC rc;

	newChildKey= (void **)malloc(sizeof(void*));
	*newChildKey=NULL;
	dbgnIX("inserting entry","");
	dbgnIXFn();
	getRoot(fileHandle,root);
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

	rc=insertRecurseEntry(fileHandle,attribute, tempKey,rid,root,newChildKey);
	if(rc==RECORD_EXISTS_ALREADY)
	{
		dbgnIX("record exists already..","");
		dbgnIXFnc();
		free(newChildKey);
		free(tempKey);
		return rc;
	}

	if(*newChildKey==NULL)
	{

		dbgnIX("new child key is not found, so no split occurred","");
		free(tempKey);
		free(newChildKey);
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
	insertRecordInIndex(fileHandle,attribute,newRoot,newRootPage,middleKey,newChildKey);
	setPrevPointerIndex(newRootPage,root);
	fileHandle.writePage(newRoot,newRootPage);

	dbgnIXFnc();
	free(newRootPage);
	free(*newChildKey);
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
	if(root == -1 ||key==NULL){
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
	else
	{
		tempLowKey=malloc(4);
		memcpy(tempLowKey,key,4);

	}
	if(attribute.type!=2)
	{
		dbgnIX("Looking for elemnt .",intVal(key));
	}
	else{
		dbgnIX("Looking for elemnt with string wih Length=",intVal(key));
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
	dbgnIX("Binary searching through the array...","");
	// Find lowest record in page which is not deleted, i.e (offset != -1)
	// No need to do same for highest record, because highest record will always EXIST ! (Nature of delete)
	INT16 startOffset = getSlotOffV(pageData,start);
	while(startOffset==-1 && start<totalSlots){
		start++;
		startOffset = getSlotOffV(pageData,start);
	}

	int end = totalSlots-1; // Will Always EXIST !
	int mid = (start+end)/2;
	if(attribute.type!=2)
	{
		dbgnIX("Looking for elemnt .",intVal(key));
	}
	else{
		dbgnIX("Looking for elemnt with string wih Length=",intVal(key));
	}

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
				freeSpaceIncrease = (getSlotLenV(pageData,(totalSlots-1))+4);
			}
			else{
				getSlotOffV(pageData,mid) = (INT16)-1;
				freeSpaceIncrease = (getSlotLenV(pageData,mid)+4);
			}
			dbgnIX("Slots reduce by",reducedSlotsBy);
			dbgnIX("Free Space Increases by",freeSpaceIncrease);
			dbgnIX("slot offset after deletion",getSlotOffV(pageData,mid));
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
	if(ix_ScanIterator.type != 2){
		ix_ScanIterator.nextKey = malloc(4);
		ix_ScanIterator.copyLength = 4;
	}

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
	tempLowKey = NULL;
	if(attribute.type==2 && lowKey!=NULL){
		int searchKeyLength = (*(INT32*)(lowKey));
		INT32 newKeyLength = searchKeyLength + 1;
		tempLowKey = malloc(searchKeyLength+5); // THIS IS FREED IN THIS FUNCTION ITSELF
		BYTE* temp = (BYTE*)tempLowKey;
		memcpy(temp,& newKeyLength, 4);
		memcpy(temp + 4,lowKey+4,searchKeyLength);
		*((BYTE*)tempLowKey + (searchKeyLength+4)) = (BYTE)0;
	}
	else if(lowKey!=NULL)
	{
		tempLowKey=malloc(4);
		memcpy(tempLowKey,lowKey,4);
	}

	// Make new format for High Key Value
	ix_ScanIterator.highKey = NULL;
	if(attribute.type==2 && highKey != NULL){
		int searchKeyLength = (*(INT32*)(highKey));
		INT32 newKeyLength = searchKeyLength + 1;
		ix_ScanIterator.highKey = malloc(searchKeyLength+5); // THIS WILL BE FREED BY SCANITERATOR !
		BYTE* temp = (BYTE*)ix_ScanIterator.highKey;
		memcpy(temp,& newKeyLength, 4);
		memcpy(temp + 4,highKey+4,searchKeyLength);
		*((BYTE*)ix_ScanIterator.highKey + (searchKeyLength+4)) = (BYTE)0;
	}
	else if(highKey!=NULL)
	{
		ix_ScanIterator.highKey=malloc(4);
		memcpy(ix_ScanIterator.highKey,highKey,4);
	}

	if(highKey!=NULL && lowKey !=NULL){
		// Return error if higKey < lowKey
		float comp = compare(tempLowKey,ix_ScanIterator.highKey,attribute.type);
		dbgnIX("Comparing high and low key","");
		if(comp<0){
			dbgnIX("High key is less than low key","");
			dbgnIXFnc();
			return -1;
		}
	}

	// Find Leaf Page where possible first record is present
	findLeafPage(fileHandle, pageData, root, tempLowKey, attribute.type);
	findLowSatisfyingEntry(fileHandle, pageData, root, tempLowKey, lowKeyInclusive, ix_ScanIterator.highKey, highKeyInclusive, attribute.type, ix_ScanIterator.nextRid, ix_ScanIterator);
	dbgnIX("RID Pagenum",ix_ScanIterator.nextRid.pageNum)
	dbgnIX("RID SlotNum",ix_ScanIterator.nextRid.slotNum)
	ix_ScanIterator.leafPage = pageData;
	ix_ScanIterator.totalSlotsInCurrPage = getSlotNoV(pageData);
	free(tempLowKey); /// I ADDED THIS
	dbgnIXFnc();
	return 0;
}

INT32 IndexManager::findLowSatisfyingEntry(FileHandle& fileHandle, void* pageData, INT32& root, void* lowKey, bool lowKeyInclusive, void* highKey, bool highKeyInclusive,//
		AttrType type, RID& nextRid, IX_ScanIterator& ix_scaniterator){
	dbgnIXFn();
	// JUST DOING A LINEAR SCAN IN THE FOUND PAGE
	INT32 nextPage = -1;
	INT16 start = 0;
	INT16 startOffset;

	if(lowKey == NULL){
		dbgnIX("Case 2", "Low key is Null");
		INT16 totalSlots = getSlotNoV(pageData);
		while(totalSlots==0){
			start = 0;
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

		startOffset = getSlotOffV(pageData,start);
		while(startOffset==-1 && start < totalSlots){
			start++;
			startOffset = getSlotOffV(pageData,start);
		}
		//now we are at a nonzeroslot page, and at a slot which has non zero offset..


		if((highKey!=NULL && compare((BYTE*)pageData+startOffset,highKey,type) < 0) || //
				(highKey !=NULL && compare((BYTE*)pageData+startOffset,highKey,type) == 0 && highKeyInclusive == false) ){
			dbgnIX("Case 2.1","first entry found is greater(equal) than highkey");
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			dbgnIXFnc();
			return 0;
		}

		dbgnIX("Case 2.2","entry is less than highkey!");
		INT16 inRecordOffset = 4;
		INT16 recordOffset = getSlotOffV(pageData,start);
		INT32 outputLength = 0;
		if(type == 2){
			INT16 inRecordKeyLength = *((INT32*)((BYTE*)pageData+recordOffset));
			inRecordOffset += inRecordKeyLength;
			INT32 outputLength = inRecordOffset-1;
			ix_scaniterator.nextKey = malloc(outputLength); // will be freed in get next entry
			ix_scaniterator.copyLength = outputLength;
		}
		dbgnIX("case 6","Valid key formed");
		ix_scaniterator.storedSlot = start;
		nextRid.pageNum = *((INT32*)((BYTE*)pageData+recordOffset+inRecordOffset));
		nextRid.slotNum = *((INT16*)((BYTE*)pageData+recordOffset+inRecordOffset+4));
		if(outputLength !=0)
			memcpy(ix_scaniterator.nextKey,(BYTE*)pageData+recordOffset,outputLength);
		memcpy(ix_scaniterator.nextKey,&outputLength,4);
		dbgnIX("First satisfying Pagenum",nextRid.pageNum);
		dbgnIX("First satisfying Slotnum",nextRid.slotNum);
		dbgnIXFnc();
		return 0;
	}

	// nonzero page,non zeeo slot, and low key-matching slot may or not not be the start slot..
	INT16 totalSlots = getSlotNoV(pageData);
	start=-1;
	do
	{
		start++;
		startOffset = getSlotOffV(pageData,start);

		while(totalSlots==0 || totalSlots==start){
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
			start = 0;
			startOffset = getSlotOffV(pageData,start);
		}

		while(startOffset==-1 && start < totalSlots){
			start++;
			startOffset = getSlotOffV(pageData,start);
		}

	}while(compare((BYTE*)pageData+startOffset,lowKey,type) > 0 );


	//nonzero page, non zero slot, and guaranteed to be greater than or equal to  lowkey

	if(compare((BYTE*)pageData+startOffset,lowKey,type) == 0 && lowKeyInclusive == false){
		start = start+1;
		if(start==totalSlots){
			nextPage = *((INT32*)((BYTE*)pageData+8));
			if(nextPage == -1){
				dbgnIX("case 3.1","end of linked list reached");
				nextRid.pageNum = (INT32)-1;
				nextRid.slotNum = (INT16)-1;
				dbgnIXFnc();
				return 0;
			}
			fileHandle.readPage(nextPage,pageData);
		}
		dbgnIX("Case 3","lowkey inclusive is false, search for next entry");
		INT16 totalSlots = getSlotNoV(pageData);
		while(totalSlots==0){
			start = 0;
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

		startOffset = getSlotOffV(pageData,start);
		while(startOffset==-1 && start < totalSlots){
			start++;
			startOffset = getSlotOffV(pageData,start);
		}
		//nonzero page, non zero slot, and guaranteed to be greater than
		if((highKey!=NULL && compare((BYTE*)pageData+startOffset,highKey,type) < 0) ||//
				(highKey !=NULL && compare((BYTE*)pageData+startOffset,highKey,type) == 0 && highKeyInclusive == false) ){
			dbgnIX("Case 3.2","new next entry found is greater(equal) than highkey");
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			dbgnIXFnc();
			return 0;
		}

		dbgnIX("Case 3.3","proper entry found when lowkey = false");
		INT16 inRecordOffset = 4;
		INT16 recordOffset = getSlotOffV(pageData,start);
		INT32 outputLength = 0;
		if(type == 2)
		{
			INT16 inRecordKeyLength = *((INT32*)((BYTE*)pageData+recordOffset));
			inRecordOffset += inRecordKeyLength;
			INT32 outputLength = inRecordOffset-1;
			ix_scaniterator.nextKey = malloc(outputLength); // will be freed in get next entry
			ix_scaniterator.copyLength = outputLength;
		}
		dbgnIX("case 6","Valid key formed");
		ix_scaniterator.storedSlot = start;
		nextRid.pageNum = *((INT32*)((BYTE*)pageData+recordOffset+inRecordOffset));
		nextRid.slotNum = *((INT16*)((BYTE*)pageData+recordOffset+inRecordOffset+4));
		if(outputLength !=0)
			memcpy(ix_scaniterator.nextKey,(BYTE*)pageData+recordOffset,outputLength);
		memcpy(ix_scaniterator.nextKey,&outputLength,4);
		dbgnIX("First satisfying Pagenum",nextRid.pageNum);
		dbgnIX("First satisfying SlotNum",nextRid.slotNum);
		dbgnIXFnc();
		return 0;
	}

	dbgnIX("Case 4","lowkey != null & low inclusive = true or it is false but does not matter, return 1st RID");
	if((highKey!=NULL && compare((BYTE*)pageData+startOffset,highKey,type) < 0) || //
			(highKey !=NULL && compare((BYTE*)pageData+startOffset,highKey,type) == 0 && highKeyInclusive == false) ){
		dbgnIX("Case 4.1","first entry found is greater(equal) than highkey");
		nextRid.pageNum = (INT32)-1;
		nextRid.slotNum = (INT16)-1;
		dbgnIXFnc();
		return 0;
	}
	dbgnIX("Case 4.2","proper first entry found");
	INT16 inRecordOffset = 4;
	INT16 recordOffset = getSlotOffV(pageData,start);
	INT32 outputLength = 0;
	if(type == 2)
	{
		INT16 inRecordKeyLength = *((INT32*)((BYTE*)pageData+recordOffset));
		inRecordOffset += inRecordKeyLength;
		INT32 outputLength = inRecordOffset-1;
		ix_scaniterator.nextKey = malloc(outputLength); // will be freed in get next entry
		ix_scaniterator.copyLength = outputLength;
	}
	ix_scaniterator.storedSlot = start;
	nextRid.pageNum = *((INT32*)((BYTE*)pageData+recordOffset+inRecordOffset));
	nextRid.slotNum = *((INT16*)((BYTE*)pageData+recordOffset+inRecordOffset+4));
	if(outputLength !=0)
		memcpy(ix_scaniterator.nextKey,(BYTE*)pageData+recordOffset,outputLength);
	memcpy(ix_scaniterator.nextKey,&outputLength,4);
	dbgnIX("First satisfying Pagenum",nextRid.pageNum);
	dbgnIX("First satisfying SlotNum",nextRid.slotNum);
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
	memcpy(key,nextKey,copyLength);
	if(type == 2)
	{
		free(nextKey);
	}


	// SET NEXT RID

	INT16 currSlot = storedSlot;


	// SET NEXT VALID SLOT
	do
	{
		if(currSlot < totalSlotsInCurrPage){
			dbgnIX("slot offset =-1","incrementing");
			dbgnIX("slot value ==",currSlot);
			dbgnIX("offset ===",getSlotOffV(leafPage,currSlot))
			currSlot++;
		}
		else{
			currSlot=0;
			dbgnIX("next page","incrementing");
			INT32 nextPage = *((INT32*)((BYTE*)leafPage+8));
			if(nextPage == -1){
				dbgnIX("Case 3","reached end of list while searching for next entry");
				nextRid.pageNum = (INT32)-1;
				nextRid.slotNum = (INT16)-1;
				dbgnIXFnc();
				return 0;
			}
			fileHandle.readPage(nextPage,leafPage);
			totalSlotsInCurrPage = getSlotNoV(leafPage);
			dbgnIX("total no of slots",totalSlotsInCurrPage);
		}
	}while(currSlot==totalSlotsInCurrPage||getSlotOffV(leafPage,currSlot)==-1);

	// CHECK IF NEXT VALID SLOT IS LESS THAN HIGH KEY
	INT16 recordOffset = getSlotOffV(leafPage,currSlot);

	if( (highKey==NULL) || (compare(((BYTE*)leafPage+recordOffset),highKey, type) >= 0)){
		if(highKey!=NULL && (compare(((BYTE*)leafPage+recordOffset),highKey, type)==0) && highKeyInclusive==false){
			dbgnIX("case 4","equal to higkey and highkey inclusive is false so set nextrid to -1");
			nextRid.pageNum = (INT32)-1;
			nextRid.slotNum = (INT16)-1;
			dbgnIXFnc();
			return 0;
		}
		// VALID NEXT RID !!!!
		INT16 inRecordOffset = 4;
		INT32 outputLength = 4;
		INT16 nextKeyLength;
		if(type == 2){
			INT16 inRecordKeyLength = *((INT32*)((BYTE*)leafPage+recordOffset));
			inRecordOffset += inRecordKeyLength;
			nextKeyLength = inRecordKeyLength-1;
			outputLength = inRecordOffset-1;
			nextKey = malloc(outputLength); // will be freed when get next entry is called again
			copyLength = outputLength;
		}
		dbgnIX("case 6","Valid key formed");
		storedSlot = currSlot;
		dbgnIX("storedSlot",storedSlot);
		nextRid.pageNum = *((INT32*)((BYTE*)leafPage+recordOffset+inRecordOffset));
		nextRid.slotNum = *((INT16*)((BYTE*)leafPage+recordOffset+inRecordOffset+4));
		memcpy(nextKey,(BYTE*)leafPage+recordOffset,outputLength);
		if(type == 2)
			memcpy(nextKey,&nextKeyLength,4);
		dbgnIX("Next pagenum",nextRid.pageNum);
		dbgnIX("Next SLotnum",nextRid.slotNum);
	}
	else{
		dbgnIX("case 5","crosses higkey so set nextrid to -1");
		nextRid.pageNum = (INT32)-1;
		nextRid.slotNum = (INT16)-1;
		dbgnIXFnc();
		return 0;
	}
	dbgnIXFnc();
	return 0;
}

RC IX_ScanIterator::close()
{
	free(leafPage);
	free(highKey);
	fileHandle.stream=0;
	if(type != 2){
		free(nextKey);
	}
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
			dbgnIX("either key is null or start>key","");
			root = *((INT32*)((BYTE*)pageData + 8));
			dbgnIX("new vallue of root",root);
			fileHandle.readPage(root, pageData);
			continue;
		}

		// Check if keyInput is larger than last entry
		INT16 endOffset = getSlotOffV(pageData,end);
		if(compare((BYTE*)pageData+endOffset,key,type) >= 0){
			dbgnIX("start>key","");
			root = getIndexValueAtOffset(pageData, endOffset, type);
			dbgnIX("new vallue of root",root);
			fileHandle.readPage(root, pageData);
			continue;
		}
		dbgnIX("Binary search starts.....","");;
		while(start<end){
			INT16 midOffset = getSlotOffV(pageData,mid);
			if(compare((BYTE*)pageData+midOffset,key,type) == 0){
				dbgnIX("equal case FOUND....","");;
				root = getIndexValueAtOffset(pageData, midOffset, type);
				fileHandle.readPage(root, pageData);
				dbgnIX("new vallue of root",root);
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
		dbgnIX("Binary search ends.....","");
		dbgnIX("value of mid",mid);
		if(compare((BYTE*)pageData+getSlotOffV(pageData,mid),key,type) < 0){
			dbgnIX("key is greater than final converegd node===> take the > pointer down","");;
			root = getIndexValueAtOffset(pageData, getSlotOffV(pageData,mid-1), type);
			dbgnIX("new vallue of root",root);
			fileHandle.readPage(root, pageData);
			dbgnIXFnc();
			return;
		}
		dbgnIX("key is lesser than or equal to final converegd node===> take the < pointer down","");;
		root = getIndexValueAtOffset(pageData, getSlotOffV(pageData,mid), type);
		dbgnIX("new vallue of root",root);
		fileHandle.readPage(root, pageData);
		dbgnIXFnc();
		return;
	}
	dbgnIXFnc();
}

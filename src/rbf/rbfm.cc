#include<cstdlib>
// #include <unistd.h>
// #include <sys/types.h>
#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
	if(!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	_rbf_manager=NULL;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	PagedFileManager *pfm = PagedFileManager::instance();
	if( pfm->createFile(fileName.c_str())!=0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->destroyFile(fileName.c_str())!=0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->openFile(fileName.c_str(),fileHandle)!=0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	if(pfm->closeFile(fileHandle)!=0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	void *page=malloc(PAGE_SIZE),*newPage=malloc(PAGE_SIZE); // to be freed when I exit
	dbgn1("this=================================================================== ","Reorganize called");
	dbgn1("Filename",fileHandle.fileName);
	INT16 length;

	INT16 freeOffset,origOffset,origLength=0,slotNo,freeSpace,slot=0;
	INT32 virtualPageNum=pageNumber,offset=virtualPageNum%681;

	fileHandle.readPage(virtualPageNum,page);
	fileHandle.readPage(virtualPageNum,newPage);
	freeOffset=0;
	slotNo=getSlotNoV(newPage);
	freeSpace=4092-(slotNo*4)-freeOffset;

	for(slot=0;slot<slotNo;slot++)
	{
		origOffset=getSlotOffV(page,slot);origLength=getslotLenV(page,slot);
		if( origOffset == -1)   //means that the slot is empty.
			continue;

		if(origLength>=0)
		{
			memcpy(newPage+freeOffset,page+origOffset,origLength);   //copy the record
			memcpy(getSlotOffA(newPage,slot),&freeOffset,2);			 //copy the new offset int othe slot
			//memcpy(getslotLenA(newPage,i),&origLength,2);			 //copy the new length int othe slot ---- redundant as length will alredy be there in the slot info
			freeOffset+=origLength;
		}
		else
		{
			//tombstone case-whee ineed to copy the first six bytes alone from the record
			memcpy(newPage+freeOffset,page+origOffset,6);   /// copying only 6 bytes as its a tombstone....
			memcpy(getSlotOffA(newPage,slot),&freeOffset,2);
			origLength=-6;
			memcpy(getSlotLenA(newPage,slot),&origLength,2);	//update length of slot to -6 ????? required or not ?
			freeOffset+=TOMBSIZE;								/// incrementing by 6 bytes..
		}
	}
	memcpy(getFreeOffsetA(newPage),&freeOffset,2);
	dbgn1("the new free offset after reorganizing is ",freeOffset);

	fileHandle.writePage(virtualPageNum,newPage);
	free(page);
	free(newPage);
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	void *modRecord=NULL,*headerPage,*page=malloc(PAGE_SIZE); // to be freed when I exit
	dbgn("this=================================================================== ","insertRecord");
	dbgn("Filename",fileHandle.fileName);
	INT32 headerPageActualNumber;
	INT16 length;
	modRecord = modifyRecordForInsert(recordDescriptor,					//
													  data,					//
													  length);
	INT16 totalLength=length;
	INT32 virtualPageNum=findFirstFreePage(fileHandle,length+4,headerPageActualNumber);
    dbgn("virtualPageNum",virtualPageNum);
	dbgn("headerPageActualNumber",headerPageActualNumber);

	INT16 freeOffset,slotNo,freeSpace;
	INT32 offset=virtualPageNum%681,i;
	headerPage=malloc(PAGE_SIZE);
	bool slotReused=false;

	//reading of the data page..
	fileHandle.readPage(virtualPageNum,page);
	freeOffset=getFreeOffsetV(page);slotNo=getSlotNoV(page);
	freeSpace=4092-(slotNo*4)-freeOffset;

	for(i=0;i<slotNo;i++)
	{
		if(getSlotOffV(page,i)==-1)
		{
			slotReused=true;
			break;
		}
	}

	if(!slotReused){slotNo++;totalLength=length+4;}

	if(freeSpace<totalLength)
		{
			reorganizePage(fileHandle,recordDescriptor,virtualPageNum);
			fileHandle.readPage(virtualPageNum,page);
			freeOffset=getFreeOffsetV(page);slotNo=getSlotNoV(page);
			freeSpace=4092-(slotNo*4)-freeOffset;
		}

	rid.slotNum=i;rid.pageNum=virtualPageNum;	dbgn("**************RID pgno",rid.pageNum);dbgn("*****************RID slotNo",rid.slotNum);	//update RID
	memcpy(getSlotOffA(page,i),&freeOffset,2);  //update offset for slot
	memcpy(getslotLenA(page,i),&length,2);		//update length for slot
	dbgn("freeOffset",freeOffset);
	dbgn("length",length);
	memcpy((BYTE *)page+freeOffset,modRecord,length);		//write the record
	freeOffset=freeOffset+length;
	memcpy(getFreeOffsetA(page),&freeOffset,2);
	memcpy(getSlotNoA(page),&slotNo,2);

	fileHandle.writePage(virtualPageNum,page);//written data page

	//write the header page free space now

	fseek(fileHandle.stream,headerPageActualNumber*PAGE_SIZE,SEEK_SET);
	fread(headerPage, 1, PAGE_SIZE, fileHandle.stream);
	freeSpace=*(INT16 *)((BYTE *)headerPage+4+((offset+1)*PES)); // here handle the case where tombstone is left--update has to take care.
	dbgn("free space in page",freeSpace);
	freeSpace=freeSpace-totalLength;
	dbgn("free space in page",freeSpace);
	memcpy((BYTE *)headerPage+4+((offset+1)*PES),&freeSpace,2);
	fseek(fileHandle.stream,headerPageActualNumber*PAGE_SIZE,SEEK_SET);
	fwrite(headerPage, 1, PAGE_SIZE, fileHandle.stream);
	free(headerPage);
	free(page);
	free(modRecord);

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	//	fetch the actual page. read the record. convert into application format.return record.
	INT32 virtualPageNum=rid.pageNum,slotNo=rid.slotNum;
	RC rc;
	dbgn("this ","readRecord			=============================================================");
	dbgn("Filename",fileHandle.fileName);
	void *page=malloc(PAGE_SIZE),*modRecord;
	rc=fileHandle.readPage(virtualPageNum,page);
	if(rc)return -1;
	INT32 totalSlotNo=*(INT16 *)((BYTE *)page+4092);

	if(slotNo>=totalSlotNo||slotNo<0)return -1;

	INT16 offset=*(INT16 *)((BYTE *)page+4088-(slotNo*4));
	INT16 length=*(INT16 *)((BYTE *)page+4090-(slotNo*4));

	if(length<0)								//tombstone
	{

		RID tempId;
		dbgn1("tombstne record =========================== in read","   ");
		memcpy(&tempId.pageNum,((INT32 *)page+offset),4);
		memcpy(&tempId.slotNum,((INT16 *)page+offset+4),2);
		dbgn1("tombstone points to RID page number",tempId.pageNum);
		dbgn1("tombstone points to RID slot number",tempId.slotNum);
		RC rc=readRecord(fileHandle,recordDescriptor,tempId,data);
		free(page);
		free(modRecord);
		return RC;

	}
	modRecord=malloc(length);
	memcpy(modRecord,(BYTE *)page+offset,length);

	modifyRecordForRead(recordDescriptor,data,modRecord);
	free(page);
	free(modRecord);

	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	//	with record descriptor decode the given record and print it.this has to decode application format of record.

	BYTE * printData = (BYTE*)data;
	INT32 num = 0;FLOAT num1;
	dbgn("this ","printRecord");

	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	dbgn("num of attributes",recordDescriptor.size());
	while(it != recordDescriptor.end())
	{
		switch(it->type){
		case 0:
			cout<<it->name<<"\t";
			num = *((INT32 *)printData);
			if(isNull(num))
				cout<<"NULL\n";
			else
				cout<<num<<"\n";
			printData = printData+4;

			break;

		case 1:
			cout<<it->name<<"\t";
			num1 = *((FLOAT *)printData);
			num = *((INT32 *)printData);
			if(isNull(num))
				cout<<"NULL\n";
			else
				cout<<num1<<"\n";
			printData = printData+4;

			break;

		case 2:
			cout<<it->name<<"\t";
			num = *((INT32 *)printData);
			if(isNull(num))
			{
				cout<<"NULL\n";
			printData = printData+4;
			}
			else
			{
			printData = printData+4;
			for(int i=0;i<num;i++){
				cout<<*((char*)printData);
				printData = printData+1;
			}
			cout<<"\n";
			}
			break;

		default:
			break;

		}
		++it;
	}

	return 0;
}

INT32 RecordBasedFileManager::findFirstFreePage(FileHandle &fileHandle, INT16  requiredSpace, INT32  &headerPageNumber){
	bool isPageFound = false;
	int noOfPages = fileHandle.getNumberOfPages();
	int curr = 10;									//Current Seek Position
	int pagesTraversedInCurrentHeader = 0;			//Variable to maintain count of virtual pages traversed in a header file
	INT16 freeSpace = 0;
	int header = 0;									//Keep track of header file
	INT32 nextHeaderPageNo = 0;						//Chain to next header page
	INT32 finalHeader;

	while(noOfPages--){
		pagesTraversedInCurrentHeader++;
		fseek(fileHandle.stream,curr,SEEK_SET);		//Set position to freeSpace field in "Page Entries"
		fread(&freeSpace, 1, 2, fileHandle.stream);	//Start reading freeSpace field in "Page Entries"
		dbgn("freeSpace",freeSpace);
		dbgn("requiredSpace",requiredSpace);

		if(freeSpace>=requiredSpace){				//If freeSpace is sufficient, end search
			isPageFound=true;
			break;
		}
		curr+=6;									//if not, go to next entry
		if(pagesTraversedInCurrentHeader==681 && noOfPages!=0){		//If all pages traversed in the current header, move to next header
			header++;
			pagesTraversedInCurrentHeader=0;
			nextHeaderPageNo = fileHandle.getNextHeaderPage(nextHeaderPageNo);
			curr = nextHeaderPageNo*PAGE_SIZE+10;
		}
	}

	if(!isPageFound){
		void* data = malloc(PAGE_SIZE);
		*((INT32*)data+1023)=0;
		fileHandle.appendPage(data);
		free(data);
		finalHeader=fileHandle.getNextHeaderPage(nextHeaderPageNo);
		nextHeaderPageNo = (finalHeader==0)?nextHeaderPageNo:finalHeader;
		pagesTraversedInCurrentHeader++;
	}

	headerPageNumber = nextHeaderPageNo;
	return (header*681)+pagesTraversedInCurrentHeader-1;
}

RC RecordBasedFileManager::modifyRecordForRead(const vector<Attribute> &recordDescriptor,const void* data, const void* modRecord){
	BYTE* storedDataPointer = (BYTE*)modRecord;
	BYTE* dataPointer = (BYTE*)data;
	INT32 noOfFields = *((INT16*)storedDataPointer),len;
	storedDataPointer = storedDataPointer+(noOfFields*2)+2;		//Pointing to start of data stream.
	INT16 offsetPointer = 0;
	offsetPointer+=2;
	INT16 prevOffset=(noOfFields*2)+2,presOffset=0;
	INT32 num=1346458179;

	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	while(noOfFields--)
	{
		switch(it->type){
		case 0:
			presOffset=*(INT16*)((BYTE *)modRecord+offsetPointer);
			memcpy((void*)dataPointer,(void*)storedDataPointer,4);
			dataPointer+=4;
			storedDataPointer+=4;
			offsetPointer+=2;
			prevOffset=presOffset;
			break;

		case 1:
			presOffset=*(INT16 *)((BYTE *)modRecord+offsetPointer);
			memcpy((void*)dataPointer,(void*)storedDataPointer,4);
			dataPointer+=4;
			storedDataPointer+=4;
			offsetPointer+=2;
			prevOffset=presOffset;
			break;

		case 2:
			presOffset=*(INT16*)((BYTE *)modRecord+offsetPointer);
			len=presOffset-prevOffset;
			memcpy((void*)dataPointer,&len,4);
			dataPointer+=4;
			memcpy((void*)dataPointer,(void*)storedDataPointer,len);
			dataPointer+=len;
			storedDataPointer+=len;
			offsetPointer+=2;
			prevOffset=presOffset;
			break;

		default:
			break;

		}
		++it;
	}

	if(noOfFields!=recordDescriptor.size())
		{
			int numOfOffenders=recordDescriptor.size()-noOfFields;

			dbgn1(" appending CRAP to the dsk record"," quite literally");
			for(i=0;i<numOfOffenders;i++)
				memcpy((void*)dataPointer+(i*4),&num,4);   //quite literally appending the string "CRAP" into the data record.

		}
	return 0;
}

// WARNING: Its the responsibility of the caller to free the memory block assigned in this function
void* RecordBasedFileManager::modifyRecordForInsert(const vector<Attribute> &recordDescriptor,const void *data,INT16  &length)
{
	    void *modRecord=NULL;
	    BYTE * iterData = (BYTE*)data;
		INT16 numberAttr=recordDescriptor.size();
		INT16 dataOffset=0,offOffset=0;
		length=(numberAttr*2)+2;INT32 num=0;
		std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
		for(;it != recordDescriptor.end();it++)
		{
			switch(it->type){
			case 0:
				length=length+4;
				iterData=iterData+4;
				break;

			case 1:
				length=length+4;
				iterData=iterData+4;
				break;

			case 2:
				num = *((INT32 *)iterData);
				length=length+num;
				iterData=iterData+4+num;
				break;

			default:
				break;

			}
		}
		length=max(length,TOMBSIZE);					//// in order to inflate the record for  minimum of 6 bytes to accomodate tombstone
        modRecord=malloc(length);

        memcpy(modRecord,&numberAttr,2);
        dataOffset=(numberAttr*2)+2;
        offOffset=2;
        iterData = (BYTE*)data;
        for(it = recordDescriptor.begin();it != recordDescriptor.end();it++)
        {
        	switch(it->type){

        	case 0:
                memcpy((BYTE *)modRecord+dataOffset,iterData,4);
                iterData+=4;
        		dataOffset=dataOffset+4;
        		memcpy((BYTE *)modRecord+offOffset,&dataOffset,2);
        		offOffset+=2;
           		break;

        	case 1:
        		memcpy((BYTE *)modRecord+dataOffset,iterData,4);
        		iterData+=4;
        		dataOffset=dataOffset+4;
        		memcpy((BYTE *)modRecord+offOffset,&dataOffset,2);
        		offOffset+=2;
        		break;

        	case 2:
        		num = *((INT32 *)iterData);
           		iterData=iterData+4;
        		memcpy((BYTE *)modRecord+dataOffset,iterData,num);
        		dataOffset=dataOffset+num;
        		iterData+=num;
        		memcpy((BYTE *)modRecord+offOffset,&dataOffset,2);
        		offOffset+=2;
        		break;

        	default:
        		break;

        	}
        }
		return modRecord;
}

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	if(fileHandle.stream==0)return -1;
	if(fileHandle.mode==0)return -1;
	// truncate(fileHandle.fileName.c_str(),PAGE_SIZE);
	INT32 zeros = 0;
	fseek(fileHandle.stream,0,SEEK_SET);
	fwrite(&zeros,4,1,fileHandle.stream);
	fseek(fileHandle.stream,4092,SEEK_SET);
	fwrite(&zeros,4,1,fileHandle.stream);
return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	if(fileHandle.stream==0)return -1;
	if(fileHandle.mode==0)return -1;

	void * pageData = malloc(PAGE_SIZE);
	if(fileHandle.readPage(rid.pageNum,pageData)==-1)return-1;

	INT16 slotNo=rid.slotNum;
	INT16 totalSlotsInPage = *(INT16 *)((BYTE *)pageData+4092);

	if(slotNo>=totalSlotsInPage||slotNo<0)return -1;
	INT16 slotOffset = 4088-slotNo*4;
	INT16 recordOffset = *((INT16*)((BYTE*)pageData + slotOffset));
	//If record is already deleted return error
	if(recordOffset == -1)return -1;
	//Delete record by setting offset to -1
	*((INT16*)((BYTE*)pageData + slotOffset)) = -1;
	//Variable to store increase in freeSpace to update in header page
	INT32 increaseFreeSpace = *((INT32*)((BYTE*)pageData + slotOffset + 2));

	// Update FreeSpace Pointer if required
	if(((BYTE*)pageData + recordOffset + increaseFreeSpace)==((BYTE *)pageData+4094)){
		*((INT16 *)((BYTE *)pageData+4094)) = recordOffset;
	}

	// Update Number of slots if required
	if(slotNo == totalSlotsInPage-1){
		totalSlotsInPage = totalSlotsInPage-1;
		*((INT16 *)((BYTE *)pageData+4092)) = totalSlotsInPage;
		increaseFreeSpace+=4;
	}
	if(fileHandle.writePage(rid.pageNum,pageData)==-1)return -1;

	//Update FreeSpace in Header Page
	fileHandle.updateFreeSpaceInHeader(rid.pageNum, increaseFreeSpace);
	free(pageData);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	if(fileHandle.stream==0)return -1;
	if(fileHandle.mode==0)return -1;

	void * pageData = malloc(PAGE_SIZE);
	if(fileHandle.readPage(rid.pageNum,pageData)==-1)return-1;

	INT16 slotNo = rid.slotNum;
	INT16 totalSlotsInPage = *((INT16 *)((BYTE *)pageData+4092));
	INT16 freeSpaceIncrease = 0;
	if(slotNo>=totalSlotsInPage||slotNo<0)return -1;
	INT16 slotOffset = 4088-slotNo*4;
	INT16 recordOffset = *((INT16*)((BYTE*)pageData + slotOffset));
	if(recordOffset == -1)return -1;

	BYTE* recordLocation = (BYTE*)pageData + recordOffset;
	INT16 oldLength = *((INT16*)((BYTE*)pageData + slotOffset + 2));
	INT16 newLength;

	// Read the new record
	void* newRecord = modifyRecordForInsert(recordDescriptor,data,newLength);

	// if new record length is smaller than old record length
	if(newLength<=oldLength){
		memcpy(recordLocation,newRecord,newLength);
		// set new length in slot
		*((INT16*)((BYTE*)pageData + slotOffset + 2)) = newLength;
		freeSpaceIncrease = oldLength - newLength;
	}

	// if length is not smaller then record may still fit on same page
	else{
		INT16 oldFreespace = fileHandle.updateFreeSpaceInHeader(rid.pageNum, 0);

		// if it fits on the same page
		if(oldFreespace >= newLength){
			INT16 freeSpaceBlockSize = getFreeSpaceBlockSize(fileHandle, rid.pageNum);
			if(freeSpaceBlockSize==-1)return -1;
			// If it can be directly appended in free space block
			INT16 freeSpaceBlockPointer = 0;
			if(freeSpaceBlockSize>newLength){
				freeSpaceBlockPointer = *((INT16 *)((BYTE *)pageData+4094));
				memcpy(pageData+freeSpaceBlockPointer,newRecord,newLength);
			}
			// If it cannot be directly appended in free space block
			else{
				deleteRecord(fileHandle,recordDescriptor,rid);
				reorganizePage(fileHandle,recordDescriptor,rid.pageNum);
				// Read page again since you modified the data
				if(fileHandle.readPage(rid.pageNum,pageData)==-1)return-1;
				freeSpaceBlockPointer = *((INT16 *)((BYTE *)pageData+4094));
				memcpy(pageData+freeSpaceBlockPointer,newRecord,newLength);
			}
			freeSpaceIncrease = oldLength - newLength;
			// Update slot (record offset and record length)
			*((INT16*)((BYTE*)pageData + slotOffset)) = freeSpaceBlockPointer;
			*((INT16*)((BYTE*)pageData + slotOffset + 2)) = newLength;

			// Update free space block pointer
			*((INT16 *)((BYTE *)pageData+4094)) = freeSpaceBlockPointer + newLength;
		}
		// If it does not fit on same Page
		else{
			RID newRid;
			insertRecord(fileHandle,recordDescriptor,data,newRid);
			INT32 tombstonePageNum = newRid.pageNum;
			INT16 tombstoneSlotNum = newRid.slotNum;
			// insert new RID as tomb stone
			*((INT32*)((BYTE*)pageData + recordOffset)) = tombstonePageNum;
			*((INT16*)((BYTE*)pageData + recordOffset+4)) = tombstoneSlotNum;
			// Set length = -1 to indicate tomb stone
			*((INT16*)((BYTE*)pageData + slotOffset + 2)) = -1;
			freeSpaceIncrease = oldLength - 6;
		}

	}

	fileHandle.updateFreeSpaceInHeader(rid.pageNum, increaseFreeSpace);
	if(fileHandle.writePage(rid.pageNum,pageData)==-1)return-1;
	free(pageData);
	free(newRecord);
return 0;
}

INT16 RecordBasedFileManager::getFreeSpaceBlockSize(FileHandle &fileHandle, PageNum pageNum)
{
	void * pageData = malloc(PAGE_SIZE);
	if(fileHandle.readPage(pageNum,pageData)==-1)return-1;
	INT16 totalSlotsInPage = *((INT16 *)((BYTE *)pageData+4092));
	INT16 freeSpacePointer = *((INT16 *)((BYTE *)pageData+4092));
	return 4092 - (totalSlotsInPage*4) - freeSpacePointer;
}


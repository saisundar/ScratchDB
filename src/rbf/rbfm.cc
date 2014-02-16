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
	INT16 freeOffset,origOffset,origLength=0,slotNo,slot=0;
	INT32 virtualPageNum=pageNumber;

	fileHandle.readPage(virtualPageNum,page);
	fileHandle.readPage(virtualPageNum,newPage);
	freeOffset=0;
	slotNo=getSlotNoV(newPage);origOffset=getFreeOffsetV(newPage);
	dbgn("total no of slots",slotNo);
	dbgn("original free offset",origOffset);

	for(slot=0;slot<slotNo;slot++)
	{
		origOffset=getSlotOffV(page,slot);origLength=getSlotLenV(page,slot);
		dbgn("slot no",slot);
		dbgn("orig offset",origOffset);
		dbgn("orig length",origLength);
		if( origOffset == -1)   //means that the slot is empty.
			continue;

		if(origLength>=0)
		{
			memcpy((BYTE *)newPage+freeOffset,(BYTE *)page+origOffset,origLength);   //copy the record
			memcpy(getSlotOffA(newPage,slot),&freeOffset,2);			 //copy the new offset int othe slot
			//memcpy(getslotLenA(newPage,i),&origLength,2);			 //copy the new length int othe slot ---- redundant as length will alredy be there in the slot info
			freeOffset+=origLength;
			dbgn1(" moving slot",slot);
			dbgn1("new freeoffset",freeOffset);
		}
		else
		{
			//tombstone case-where ineed to copy the first six bytes alone from the record
			memcpy((BYTE *)newPage+freeOffset,(BYTE *)page+origOffset,6);   /// copying only 6 bytes as its a tombstone....
			memcpy(getSlotOffA(newPage,slot),&freeOffset,2);
			origLength=-1;
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

	if(freeSpace<totalLength)
	{
		reorganizePage(fileHandle,recordDescriptor,virtualPageNum);
		fileHandle.readPage(virtualPageNum,page);
		freeOffset=getFreeOffsetV(page);slotNo=getSlotNoV(page);
		freeSpace=4092-(slotNo*4)-freeOffset;
	}

	if(!slotReused){slotNo++;totalLength=length+4;}

	rid.slotNum=i;rid.pageNum=virtualPageNum;	dbgn("**************RID pgno",rid.pageNum);dbgn("*****************RID slotNo",rid.slotNum);	//update RID
	memcpy(getSlotOffA(page,i),&freeOffset,2);  //update offset for slot
	memcpy(getSlotLenA(page,i),&length,2);		//update length for slot
	dbgn("freeOffset",freeOffset);
	dbgn("length",length);
	memcpy((BYTE *)page+freeOffset,modRecord,length);		//write the record
	freeOffset=freeOffset+length;
	memcpy(getFreeOffsetA(page),&freeOffset,2);
	memcpy(getSlotNoA(page),&slotNo,2);

	fileHandle.writePage(virtualPageNum,page);//written data page

	//write the header page free space now
	headerPage=malloc(PAGE_SIZE);
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
	dbgn1("record page requestd RID Page==",rid.pageNum);
	dbgn1("record page requestd RID Slot==",rid.slotNum);
	void *page=malloc(PAGE_SIZE),*modRecord;
	rc=fileHandle.readPage(virtualPageNum,page);
	if(rc==-1)return -1;
	INT32 totalSlotNo=*(INT16 *)((BYTE *)page+4092);

	dbgn("total slots in the page",totalSlotNo);
	if(slotNo>=totalSlotNo||slotNo<0)return -1;

	INT16 offset=*(INT16 *)((BYTE *)page+4088-(slotNo*4));
	INT16 length=*(INT16 *)((BYTE *)page+4090-(slotNo*4));

	if(offset==-1)return -1;

	if(length<0)								//tombstone
	{

		RID tempId;
		INT32 temp;
		dbgn1("tombstne record =========================== in read","   ");
		memcpy(&temp,((BYTE *)page+offset),4);
		tempId.pageNum=temp;
		memcpy(&temp,((BYTE *)page+offset+4),2);
		tempId.slotNum=temp;
		dbgn1("tombstone points to RID page number",tempId.pageNum);
		dbgn1("tombstone points to RID slot number",tempId.slotNum);
		RC rc=readRecord(fileHandle,recordDescriptor,tempId,data);
		free(page);
		return rc;

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
	INT32 noOfFields = *((INT16*)storedDataPointer),tempNo=noOfFields,len;
	storedDataPointer = storedDataPointer+(noOfFields*2)+2;		//Pointing to start of data stream.
	INT16 offsetPointer = 0;
	offsetPointer+=2;
	INT16 prevOffset=(noOfFields*2)+2,presOffset=0;
	INT32 num=1346458179;

	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	while(tempNo-- && it!=recordDescriptor.end())
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

//	if(noOfFields!=recordDescriptor.size())
//	{
//		int numOfOffenders=recordDescriptor.size()-noOfFields;
//
//		dbgn1(" appending CRAP to the dsk record"," quite literally");
//		for(int i=0;i<numOfOffenders;i++)
//			memcpy(dataPointer+(i*4),&num,4);   //quite literally appending the string "CRAP" into the data record.
//
//	}
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
	dbgn("<----------------------------In modifyRecordForInsert------------------------->","");

	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	for(;it != recordDescriptor.end();it++)
	{
		dbgn("type",it->type);
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
	length= maxim(length,TOMBSIZE);					//// in order to inflate the record for  minimum of 6 bytes to accomodate tombstone
	dbgn("length of modified record",length);
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
	dbgn1("<----------------------------In Delete All Records------------------------->","");
	PagedFileManager *pfm = PagedFileManager::instance();
	const char* fileName = fileHandle.fileName.c_str();
	pfm->closeFile(fileHandle);
	dbgn1("fileHandle ","closed");
	pfm->destroyFile(fileName);
	dbgn1(fileName," destroyed");
	pfm->createFile(fileName);
	dbgn1(fileName," created again");
	pfm->openFile(fileName,fileHandle);
	dbgn1("fileHandle"," associated again");
	/*	if(fileHandle.stream==0)return -1;
	if(fileHandle.mode==0)return -1;
	// truncate(fileHandle.fileName.c_str(),PAGE_SIZE);
	INT32 zeros = 0;
	fseek(fileHandle.stream,0,SEEK_SET);
	fwrite(&zeros,4,1,fileHandle.stream);
	fseek(fileHandle.stream,4092,SEEK_SET);
	fwrite(&zeros,4,1,fileHandle.stream);*/
	return 0;
}
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	dbgn1("<----------------------------In Delete Record------------------------->","");
	if(fileHandle.stream==0)
		{
		dbgn1("stream -======0","oopsies");
		return -1;
		}
//	if(fileHandle.mode==0)
//		{
//		dbgn1("mode -======0","oopsies");
//		return -1;
//		}

	dbgn1("requested Slot number: ",rid.slotNum);
	dbgn1("requested Page number: ",rid.pageNum);

	void * pageData = malloc(PAGE_SIZE);
	if(fileHandle.readPage(rid.pageNum,pageData)==-1)return-1;

	INT16 slotNo=rid.slotNum;
	INT16 totalSlotsInPage = *(INT16 *)((BYTE *)pageData+4092);
	dbgn1("Current Slot number: ",rid.slotNum);
	dbgn1("Total number of slots in page: ",totalSlotsInPage);

	if(slotNo>=totalSlotsInPage||slotNo<0)return -1;
	INT16 slotOffset = 4088-slotNo*4;
	INT16 recordOffset = *((INT16*)((BYTE*)pageData + slotOffset));
	INT16 recordLength = *((INT16*)((BYTE*)pageData + slotOffset + 2));
	dbgn1("Record Offset",recordOffset);
	dbgn1("Record Length",recordLength);

	//If record is already deleted return error
	if(recordOffset == -1)return -1;

	//IF record has tomb stone
	if(recordLength <0){
		dbgn1("<----Handling Tomb Stone------>"," delete record then update again" );
		RID newRid;
		newRid.pageNum = (unsigned)*((INT32 *)((BYTE *)pageData+recordOffset));
		newRid.slotNum = (unsigned)*((INT16 *)((BYTE *)pageData+recordOffset+4));
		deleteRecord(fileHandle,recordDescriptor,newRid);
	}

	//Delete record by setting offset to -1
	*((INT16*)((BYTE*)pageData + slotOffset)) = -1;

	//Variable to store increase in freeSpace to update in header page
	INT16 increaseFreeSpace = (recordLength == -1)? 6 : recordLength;

	// Update FreeSpace Pointer if required
	if((recordOffset + increaseFreeSpace)==*((INT16 *)((BYTE *)pageData+4094))){
		*((INT16 *)((BYTE *)pageData+4094)) = recordOffset;
	}

	// Update Number of slots if required
	if(slotNo == totalSlotsInPage-1){
		dbgn1("Last Slot has been deleted ", "");
		totalSlotsInPage = totalSlotsInPage-1;
		*((INT16 *)((BYTE *)pageData+4092)) = totalSlotsInPage;
		increaseFreeSpace+=4;
	}
	if(fileHandle.writePage(rid.pageNum,pageData)==-1)return -1;
	dbgn1("Free Space increases by: ", increaseFreeSpace);
	//Update FreeSpace in Header Page
	fileHandle.updateFreeSpaceInHeader(rid.pageNum, increaseFreeSpace);
	free(pageData);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	dbgn1("<----------------------------In Update Record------------------------->","");
	if(fileHandle.stream==0)return -1;

	void * pageData = malloc(PAGE_SIZE);
	if(fileHandle.readPage(rid.pageNum,pageData)==-1)return-1;

	INT16 slotNo = rid.slotNum;

	dbgn1("update requested fir Record slotNum",rid.slotNum);
	dbgn1("update requested fir Record pgnum",rid.pageNum);

	INT16 totalSlotsInPage = getSlotNoV(pageData);
	dbgn1("Current Slot number: ",rid.slotNum);
	dbgn1("Total number of slots in page",totalSlotsInPage);

	INT16 freeSpaceIncrease = 0;
	if(slotNo>=totalSlotsInPage||slotNo<0)return -1;
	INT16 slotOffset 	= 4088-slotNo*4;
	INT16 recordOffset  = getSlotOffV(pageData,slotNo);
	BYTE* recordLocation= (BYTE*)pageData + recordOffset;
	INT16 recordLength  =  getSlotLenV(pageData,slotNo);
	dbgn1("old Record Offset",recordOffset);
	dbgn1("old Record Length",recordLength);

	//IF record is deleted
	if(recordOffset == -1)return -1;

	//IF record has tomb stone
	if(recordLength <0){
		dbgn1("Case 0 : "," delete record then update again" );
		RID newRid;
		newRid.pageNum = (unsigned)*((INT32 *)((BYTE *)pageData+recordOffset));
		newRid.slotNum = (unsigned)*((INT16 *)((BYTE *)pageData+recordOffset+4));
		deleteRecord(fileHandle,recordDescriptor,newRid);
		getSlotLenV(pageData,slotNo)= 6;
		fileHandle.writePage(rid.pageNum,pageData);
		updateRecord(fileHandle,recordDescriptor,data,rid);
		return 0;
	}

	INT16 oldLength = getSlotLenV(pageData,slotNo);
	INT16 newLength;

	// Read the new record
	void* newRecord = modifyRecordForInsert(recordDescriptor,data,newLength);

	dbgn1("Old Length",oldLength);
	dbgn1("New Length",newLength);

	// if new record length is smaller than old record length
	if(newLength<=oldLength){
		dbgn1("Case 1: ","Old Length > New Length");
		memcpy(recordLocation,newRecord,newLength);
		// set new length in slot
		getSlotLenV(pageData,slotNo) = newLength;
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
				dbgn1("Case 2.1: ","Old Length < New Length && Free Space Block can accommodate new record");
				freeSpaceBlockPointer = getFreeOffsetV(pageData);
				memcpy((BYTE *)pageData+freeSpaceBlockPointer,newRecord,newLength);
				freeSpaceIncrease = oldLength - newLength;
			}
			// If it cannot be directly appended in free space block
			else{
				dbgn1("Case 2.2: ","Old Length < New Length && Free Space Block canNOT accommodate new record");
				deleteRecord(fileHandle,recordDescriptor,rid);
				reorganizePage(fileHandle,recordDescriptor,rid.pageNum);
				// Read page again since you modified the data
				if(fileHandle.readPage(rid.pageNum,pageData)==-1)return-1;
				freeSpaceBlockPointer = getFreeOffsetV(pageData);
				memcpy((BYTE *)pageData+freeSpaceBlockPointer,newRecord,newLength);
				freeSpaceIncrease = - newLength;
				if(rid.slotNum == totalSlotsInPage-1)
				{
					freeSpaceIncrease-=4;
					getSlotNoV(pageData) = totalSlotsInPage;
				}
			}
			// Update slot (record offset and record length)
			getSlotOffV(pageData,slotNo) = freeSpaceBlockPointer;
			getSlotLenV(pageData,slotNo) = newLength;

			dbgn1("New Record Offset",getSlotOffV(pageData,slotNo));
			dbgn1("New Record Length",getSlotLenV(pageData,slotNo));

			// Update free space block pointer
			getFreeOffsetV(pageData) = freeSpaceBlockPointer + newLength;
			dbgn1("Free Space Block Pointer", getFreeOffsetV(pageData));
		}
		// If it does not fit on same Page
		else{
			dbgn1("Case 3: ","Cannot fit on same page");
			RID newRid;
			insertRecord(fileHandle,recordDescriptor,data,newRid);
			INT32 tombstonePageNum = newRid.pageNum;
			INT16 tombstoneSlotNum = newRid.slotNum;
			dbgn1("newRID Page Number: ",tombstonePageNum);
			dbgn1("newRID Slot Number: ",tombstoneSlotNum);

			// insert new RID as tomb stone
//			*((INT32*)((BYTE*)pageData + recordOffset)) = tombstonePageNum;
//			*((INT16*)((BYTE*)pageData + recordOffset+4)) = tombstoneSlotNum;
			memcpy((BYTE*)pageData + recordOffset,&tombstonePageNum,4);
			memcpy((BYTE*)pageData + recordOffset+4,&tombstoneSlotNum,2);

			// Set length = -1 to indicate tomb stone
			getSlotLenV(pageData,slotNo) = -1;
			freeSpaceIncrease = oldLength - 6;
		}

	}
	dbgn1("Free Space increases by: ", freeSpaceIncrease);
	if(fileHandle.writePage(rid.pageNum,pageData)==-1)return-1;				//always call writepage before updating freespaceheader to acuire write ermission for the handle..
	fileHandle.updateFreeSpaceInHeader(rid.pageNum, freeSpaceIncrease);


	dbgn1("UPDATE is doneeeeeeeeeeeeeeeeeeeeeeeeeee","");
	free(pageData);
	free(newRecord);
	return 0;
}
INT16 RecordBasedFileManager::getFreeSpaceBlockSize(FileHandle &fileHandle, PageNum pageNum)
{
	dbgn1("<----------------------------In Get Free SPace Block Size------------------------->","");
	void * pageData = malloc(PAGE_SIZE);
	if(fileHandle.readPage(pageNum,pageData)==-1)return-1;
	INT16 totalSlotsInPage = *((INT16 *)((BYTE *)pageData+4092));
	dbgn1("Total Slots: ",totalSlotsInPage);
	INT16 freeSpacePointer = *((INT16 *)((BYTE *)pageData+4094));
	dbgn1("Free Space Pointer: ",freeSpacePointer);
	free(pageData);
	dbgn1("Free Space: ",4092 - (totalSlotsInPage*4) - freeSpacePointer);
	return 4092 - (totalSlotsInPage*4) - freeSpacePointer;
}


RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	void *recData=malloc(PAGE_SIZE);
	readRecord(fileHandle,recordDescriptor, rid, recData);

	BYTE * printData = (BYTE*)recData;
	INT32 num = 0;
	bool found=false;
	dbgn1("this ","readAttribute");

	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	dbgn1("num of attributes",recordDescriptor.size());
	while(it != recordDescriptor.end() && !found)
	{
		switch(it->type){
		case 0:
			dbgn1(" attribute iterated name",it->name);
			dbgn1("desired atribute name",attributeName);
			if((it->name).compare(attributeName)==0)
			{
				memcpy(data,printData,4);
				found=true;
				dbgn1("attribute found","integer");
			}
			printData = printData+4;
			break;

		case 1:
			dbgn1(" attribute iterated name",it->name);
			dbgn1("desired atribute name",attributeName);
			if((it->name).compare(attributeName)==0)
			{
				memcpy(data,printData,4);
				found=true;
				dbgn1("attribute found","float");
			}
			printData = printData+4;

			break;

		case 2:
			dbgn1(" attribute iterated name",it->name);
			dbgn1("desired atribute name",attributeName);
			num = *((INT32 *)printData);
			if((it->name).compare(attributeName)==0)
			{
				if(isNull(num))
					memcpy(data,printData,4);
				else
					memcpy(data,printData,4+num);
				found=true;
				dbgn1("attribute found","string");
			}
			printData = printData+4+num;
			break;

		default:
			break;

		}
		++it;
	}
	free(recData);
	return 0;
}


//just invokes the set values which initilaises all the freaking variables in the scan iterator object
RC RecordBasedFileManager::scan(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor,const string &conditionAttribute,const CompOp compOp,              // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator)
{
	return(rbfm_ScanIterator.setValues(fileHandle,
			recordDescriptor,
			conditionAttribute.c_str(),
			compOp,
			value,
			attributeNames));

}

// returns false if attribute is not found in the given record descriptors.
bool RBFM_ScanIterator::isValidAttr(string condAttr,const vector<Attribute> &recordDescriptor){
	INT32 i;
	dbgn1("enter RBFM_ScanIterator::isValidAttr","");
	for(i=0;i<recordDescriptor.size();i++)
		if(recordDescriptor[i].length!=0 && recordDescriptor[i].name.compare(condAttr)==0)
		{
			attrNum=i;
			attrLength=recordDescriptor[i].length;
			valueP=malloc(attrLength);
			type=recordDescriptor[i].type;
			dbgn1("attirbute found.number",i);
			isValid=true;
			break;
		}
	return(i<recordDescriptor.size());
}
RC RBFM_ScanIterator::setValues(FileHandle &fileHandle,							//
		const vector<Attribute> &recordDescriptor,				//
		const char *conditionAttribute,							//
		const CompOp compOp,              						// comparision type such as "<" and "="
		const void *value,                    					// used in the comparison
		const vector<string> &attributeNames){

	dbgn1("enter RBFM_ScanIterator::setValues","");
	if(((!strcmp(conditionAttribute,"")) && (compOp==NO_OP) && (value== NULL))||compOp==NO_OP)
	{unconditional=true;isValid=true;	dbgn1("unconditionala scan about to begin......","");}
	else if(fileHandle.stream==0||!isValidAttr(conditionAttribute,recordDescriptor))
	{
		isValid=false;
		return 1;
	}

	condAttr=conditionAttribute;
	recDesc=recordDescriptor;
	oper=compOp;
	if(value==NULL)
	{
		free(valueP);
		valueP=NULL;
	}
	else
	{
		switch(type)
		{
		case 0:
		case 1:
			memcpy(valueP,value,attrLength);
			break;
		case 2:
			INT32 length = *((INT32*)value);
			memcpy(valueP,(BYTE*)value+4,attrLength); // copy the string alone if the atribute is a string
			*((BYTE*)valueP + length)=0;
			break;
		}

	}

	attrNames=attributeNames;
	currHandle=fileHandle;
	recDesc=recordDescriptor;
	curHeaderPage=malloc(PAGE_SIZE);
	curDataPage=malloc(PAGE_SIZE);
	fseek(fileHandle.stream,0,SEEK_SET);
	fread(curHeaderPage, PAGE_SIZE, 1,fileHandle.stream);
	numOfPages=*((INT32 *)curHeaderPage);
	fileHandle.readPage(0,curDataPage);
	numOfSlots=getSlotNoV(curDataPage);
	currRid.pageNum=0;
	currRid.slotNum=0;

	return(isValid=(numOfPages>0));

}

RC RBFM_ScanIterator::getNextDataPage()
{
	INT32 actualPageNum;
	bool found=false;
	while(!found)
	{
	currRid.pageNum++;

	if(currRid.pageNum==numOfPages)
	{
		currRid.slotNum=numOfSlots;
		return 0;
	}
	if(currRid.pageNum%681==0)
	{
		headerPageNum=currHandle.getNextHeaderPage(headerPageNum);
		fseek(currHandle.stream,headerPageNum*PAGE_SIZE,SEEK_SET);
		fread(curHeaderPage, PAGE_SIZE, 1, currHandle.stream);
	}
	actualPageNum=*((INT32 *)((BYTE *)curHeaderPage+((currRid.pageNum%681+1)*PES)));
	fseek(currHandle.stream,actualPageNum*PAGE_SIZE,SEEK_SET);
	fread(curDataPage, PAGE_SIZE, 1, currHandle.stream);
	currRid.slotNum=0;
	numOfSlots=getSlotNoV(curDataPage);
	dbgn1("number of slots in current data page",numOfSlots);
	if(numOfSlots!=0)found=true;
	}

	return 0;
}

RC RBFM_ScanIterator::incrementRID(){

	if(currRid.slotNum<numOfSlots-1)
		currRid.slotNum++;
	else if(currRid.slotNum==numOfSlots-1)
		getNextDataPage();

	dbgn1("new RID page number",currRid.pageNum);
	dbgn1("new RID slot number",currRid.slotNum);

	return 0;
}

bool RBFM_ScanIterator::returnRes(int diff)
{
	if(diff==0)
		return((oper==EQ_OP)||(oper==LE_OP)||(oper==GE_OP));
	if(diff<0)
		return((oper==LT_OP)||(oper==LE_OP)||(oper==NE_OP));
	return((oper==GT_OP)||(oper==GE_OP)||(oper==NE_OP));
}

bool RBFM_ScanIterator::evaluateCondition(void * temp)
{
	int diff;
	bool result=false;

	switch(type)
	{
	case 0:
		diff=intVal(temp)-intVal(valueP);
		break;
	case 1:
		if(*((float *)temp)>*((float *)valueP))
			diff=1;
		else if(*((float *)temp)==*((float *)valueP))
			diff=0;
		else
			diff=-1;
		break;
	case 2:
		diff= strcmp((char *)temp,(char *)valueP);
		dbgn1("Comparing the strings here !","");
		dbgn1((char*)temp,(char*)valueP);
		break;
	}
	result=returnRes(diff);
	dbgn("result of comparison",result==1);
	return(result);
}

RC RBFM_ScanIterator::getAttributeGroup(void * data,void *temp)
{
	BYTE * printData = (BYTE*)temp;
	BYTE * tempData = (BYTE *)data;
	INT32 num = 0;
	dbgn1("this ","getAttributeGroup");

	std::vector<Attribute>::const_iterator it = recDesc.begin();
	std::vector<string>::const_iterator st = attrNames.begin();
	dbgn("num of attributes",recDesc.size());

	dbgn("recDesc length",recDesc.size());
	dbgn("recDesc length",attrNames.size());

	for(it = recDesc.begin();(it != recDesc.end() && st!=attrNames.end());it++)
	{
		if(it->length==0)continue;

		switch(it->type){
		case 0:
			if(it->name.compare(*st)==0)
			{
				memcpy(tempData,printData,4);
				tempData+=4;
				dbgn1("int attribute found",*st);
				st++;
			}
			printData = printData+4;
			break;

		case 1:
			if(it->name.compare(*st)==0)
			{
				memcpy(tempData,printData,4);
				tempData+=4;
				dbgn1("float attribute found",*st);
				st++;
			}
			printData = printData+4;
			break;

		case 2:
			num = *((INT32 *)printData);
			if((it->name).compare(*st)==0)
			{
				if(isNull(num))
					{memcpy(tempData,printData,4);tempData+=4;}
				else
					{memcpy(tempData,printData,4+num);tempData+=4+num;}
				dbgn1("string attribute found",*st);
				st++;
			}
			printData = printData+4+num;
			break;

		default:
			break;

		}
	}
	return 0;

}
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	bool found=false;
	INT16 offset=0,len=0,numOfAttr=-1,startOff,endOff,attrLen;
	void * modRecord=NULL,*temp=NULL,*tempAttr=NULL;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	dbgn1("this ","getNextRecord======================================");
	dbgn1("total num of pages=",numOfPages);
	for(;!found;incrementRID())
	{
		if((currRid.pageNum==numOfPages && currRid.slotNum==numOfSlots)||!isValid)
		{
			dbgn1("end of scan....","");
			dbgn1("scan ",(unconditional)?"unconditional":"conditional");
			return RBFM_EOF;
		}

		offset=getSlotOffV(curDataPage,currRid.slotNum);
		len=getSlotLenV(curDataPage,currRid.slotNum);

		if(offset<0|| len<0){
			dbgn1("slot offset",offset);
			dbgn1("slot length",len);
			dbgn1("offset less than 0","or length lessert thn 0");
			continue;
		}

		modRecord=malloc(len);
		memcpy(modRecord,(BYTE*)curDataPage+offset,len);
		numOfAttr=*(INT16 *)modRecord;
		temp=malloc(len*2);		//apprxiamtaley allocating..

		if(!unconditional){								// means check for attiute condition

			if(attrNum>=numOfAttr)						//means null records at the end-===> added column but not updated===>record may be ignored.
			{	dbgn1(" desc attribute number is"," greater than disk attributes==>null");

			// see if the given value itself is null in that case this record matches....
			continue;
			}

			if(attrNum==0)
			{
				startOff=2*(numOfAttr+1);
				endOff=*(INT16 *)((BYTE *)modRecord+2);
			}
			else
			{
				startOff=*((INT16 *)((BYTE *)modRecord+2*(attrNum)));
				endOff=*((INT16 *)((BYTE *)modRecord+2*(attrNum+1)));
			}

			attrLen=endOff-startOff;
			tempAttr=malloc(attrLen+1);

			memcpy(tempAttr,(BYTE*)modRecord+startOff,attrLen);
			*((char *)tempAttr+attrLen)=0;

			if(!evaluateCondition(tempAttr))
			{
				//means the record did not satisfy the criteron.
				//move to next record

				free(modRecord);free(tempAttr);free(temp);continue;

			}

		}
		found=true;
		rbfm->modifyRecordForRead(recDesc,temp,modRecord);

		getAttributeGroup(data,temp);
		free(modRecord);
		free(tempAttr);
		free(temp);
		rid=currRid;

	}

	//wont come here at all
	return 0;
}


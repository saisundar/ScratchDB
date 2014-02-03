#include<cstdlib>
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
	freeOffset=*(INT16 *)((BYTE *)page+4094);slotNo=*(INT16 *)((BYTE *)page+4092);
	freeSpace=4092-(slotNo*4)-freeOffset;

	if(freeSpace<length)
	{
		///reorganize the damn thing....
	}

	for(i=0;i<slotNo;i++)
	{
		if(*(INT16 *)((BYTE *)page+4088-(i*4))==-1)
		{
			slotReused=true;
			break;
		}
	}

	if(!slotReused){slotNo++;totalLength=length+4;}

	if(freeSpace<totalLength)
		{
			///reorganize the damn thing....
		}

	rid.slotNum=i;rid.pageNum=virtualPageNum;	dbgn("**************RID pgno",rid.pageNum);dbgn("*****************RID slotNo",rid.slotNum);	//update RID
	memcpy((BYTE *)page+4088-(i*4),&freeOffset,2);  //update offset for slot
	memcpy((BYTE *)page+4090-(i*4),&length,2);		//update length for slot
	dbgn("freeOffset",freeOffset);
	dbgn("length",length);
	memcpy((BYTE *)page+freeOffset,modRecord,length);		//write the record
	freeOffset=freeOffset+length;
	memcpy((BYTE *)page+4094,&freeOffset,2);
	memcpy((BYTE *)page+4092,&slotNo,2);

	fileHandle.writePage(virtualPageNum,page);//written data page

	//write the header page free space now

	fseek(fileHandle.stream,headerPageActualNumber*PAGE_SIZE,SEEK_SET);
	fread(headerPage, 1, PAGE_SIZE, fileHandle.stream);
	freeSpace=*(INT16 *)((BYTE *)headerPage+4+((offset+1)*PES));
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
	dbgn("this ","readRecord");
	dbgn("Filename",fileHandle.fileName);
	void *page=malloc(PAGE_SIZE),*modRecord;
	rc=fileHandle.readPage(virtualPageNum,page);
	if(rc)return -1;
	INT32 totalSlotNo=*(INT16 *)((BYTE *)page+4092);

	if(slotNo>totalSlotNo||slotNo<0)return -1;

	INT16 offset=*(INT16 *)((BYTE *)page+4088-(slotNo*4));
	INT16 length=*(INT16 *)((BYTE *)page+4090-(slotNo*4));
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
			printData = printData+4;
			cout<<num<<"\n";
			break;

		case 1:
			cout<<it->name<<"\t";
			num1 = *((FLOAT *)printData);
			printData = printData+4;
			cout<<num1<<"\n";
			break;

		case 2:
			cout<<it->name<<"\t";
			num = *((INT32 *)printData);
			printData = printData+4;
			for(int i=0;i<num;i++){
				cout<<*((char*)printData);
				printData = printData+1;
			}
			cout<<"\n";
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

	if(noOfFields!=recordDescriptor.size())
	{
		dbgn("ERROR due to mismatch in no of attributes of record descriptor and disk record"," ");
		dbgn("disk attri",noOfFields);
		dbgn("Record descriptor",recordDescriptor.size());
	}

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

	return 0;
}

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


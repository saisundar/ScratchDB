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

	void *modRecord,*headerPage,*page=malloc(PAGE_SIZE); // to be freed when I exit
	INT32 length = modifyRecordForInsert(recordDescriptor,data,modRecord);INT32 headerPageActualNumber;
	INT32 totalLength=length;
	INT32 virtualPageNum=findFirstFreePage(fileHandle,length,headerPageActualNumber);
	INT16 freeOffset,slotNo,freeSpace;
	INT32 offset=virtualPageNum%681,i;
	headerPage=malloc(PAGE_SIZE);
	bool slotReused=false;
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
	rid.slotNum=i;rid.pageNum=virtualPageNum;	dbgn("RID pgno slotno",rid.pageNum+" "+rid.slotNum);	//update RID
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
	freeSpace=freeSpace-totalLength;
	memcpy((BYTE *)headerPage+4+((offset+1)*PES),&freeSpace,2);
	fseek(fileHandle.stream,headerPageActualNumber*PAGE_SIZE,SEEK_SET);
	fwrite(headerPage, 1, PAGE_SIZE, fileHandle.stream);

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

//	fetch the actual page. read the record. convert into application format.return record.
	INT32 virtualPageNum=rid.pageNum,slotNo=rid.slotNum;
	RC rc;
	void *page=malloc(PAGE_SIZE),*modRecord;
	rc=fileHandle.readPage(virtualPageNum,page);
	if(rc)return -1;
	INT32 totalSlotNo=*(INT16 *)((BYTE *)page+4092);

	if(slotNo>totalSlotNo)return -1;

	INT16 offset=*(INT16 *)((BYTE *)page+4088-(slotNo*4));
	INT16 length=*(INT16 *)((BYTE *)page+4090-(slotNo*4));
    modRecord=malloc(length);
	memcpy(modRecord,(BYTE *)page+offset,length);

	modRecordForRead(recordDescriptor,data,modRecord);

	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	//	with record descriptor decode the given record and print it.this has to decode application format of record.

	BYTE * printData = (BYTE*)data;
	INT32 num = 0;
	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
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
			num = *((INT32 *)printData);
			printData = printData+4;
			cout<<num<<"\n";
			break;

		case 2:
			cout<<it->name<<"\t";
			num = *((INT32 *)printData);
			printData = printData+4;
			for(int i=0;i<num;i++){
				cout<<*((char*)printData);
				printData = printData+1;
			}
			cout<<"\t";
			break;

		default:
			break;

		}
		++it;
	}

	return -1;
}

INT32 findFirstFreePage(FileHandle fileHandle, INT32 requiredSpace, INT32  &headerPageNumber){
	bool isPageFound = false;
	int noOfPages = fileHandle.getNumberOfPages();
	int curr = 10;									//Current Seek Position
	int pagesTraversedInCurrentHeader = 0;			//Variable to maintain count of virtual pages traversed in a header file
	INT16 freeSpace = 0;
	int header = 1;									//Keep track of header file
	INT32 nextHeaderPageNo = 0;						//Chain to next header page

	while(noOfPages--){
		pagesTraversedInCurrentHeader++;
		fseek(fileHandle.stream,curr,SEEK_SET);		//Set position to freeSpace field in "Page Entries"
		fread(&freeSpace, 1, 2, fileHandle.stream);	//Start reading freeSpace field in "Page Entries"
		if(freeSpace>=requiredSpace){				//If freeSpace is sufficient, end search
			isPageFound=true;
			break;
		}
		curr+=6;									//if not, go to next entry
		if(pagesTraversedInCurrentHeader==681 && noOfPages!=1){		//If all pages traversed in the current header, move to next header
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
		nextHeaderPageNo = fileHandle.getNextHeaderPage(nextHeaderPageNo);
	}

	headerPageNumber = nextHeaderPageNo;
	return (header*681)+pagesTraversedInCurrentHeader-1;
}

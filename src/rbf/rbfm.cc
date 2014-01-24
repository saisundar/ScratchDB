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
	INT32 length = modifyRecordForInsert(recordDescriptor,data,modRecord),headerPageActualNumber;
	INT32 totalLength=length;
	INT32 virtualPageNum=getFreePageInfo(fileHandle,length,headerPageActualNumber);
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
	memcpy(page+freeOffset,modRecord,length);		//write the record
	freeOffset=freeOffset+length;
	memcpy((BYTE *)page+4094,&freeOffset,2);
	memcpy((BYTE *)page+4092,&slotNo,2);

	fileHandle.writePage(virtualPageNum,page);//written data page

	//write the header page free space now

	fseek(fileHandle.stream,headerPageActualNumber*PAGE_SIZE,SEEK_SET);
	fread(headerPage, 1, PAGE_SIZE, stream);
	freeSpace=*(INT16 *)((BYTE *)headerPage+4+((offset+1)*PES));
	freeSpace=freeSpace-totalLength;
	memcpy((BYTE *)headerPage+4+((offset+1)*PES),&freeSpace,2);
	fseek(fileHandle.stream,headerPageActualNumber*PAGE_SIZE,SEEK_SET);
	fwrite(headerPage, 1, PAGE_SIZE, fileHandle.stream);

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

//	fetch the actual page. read the record. convert into application format.return record.
	INT32 virtualPageNum=rid.pageNum,slotNo=rid.slotNum,i;
	RC rc;
	void *page=malloc(PAGE_SIZE),*modRecord;
	rc=fileHandle.readPage(virtualPageNum,page);
	if(rc)return -1;
	INT32 totalSlotNo=*(INT16 *)((BYTE *)page+4092);

	if(slotNo>totalSlotNo)return -1;

	INT16 offset=*(INT16 *)((BYTE *)page+4088-(slotNo*4));
	INT16 length=*(INT16 *)((BYTE *)page+4090-(slotNo*4));
    modRecord=malloc(length);
	memcpy(modRecord,page+offset,length);

	modRecordForRead(recordDescriptor,data,modRecord);

	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
//	with record descriptor decode the given record and print it.this has to decode application format of record.

	INT32 pos=0;
	for(Attribute a:recordDescriptor)
	{
		switch(a.type){
		case 0:
			cout<<a.name<<"\t";
			INT32 num=*(INT32 *)pos;pos=pos+4;
			cout<<num<<"\n";
		case 1:
			cout<<a.name<<"\t";
			INT32 num=*(INT32 *)pos;pos=pos+4;
			cout<<num<<"\n";
		case 3:
			INT32 len=*(INT32 *)pos;

		}
	}

	return -1;
}

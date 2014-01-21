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
	void *data = malloc(PAGE_SIZE);
	void *data1 = malloc(PAGE_SIZE);
	INT32 i;
	FileHandle fileHandle;
	if( pfm->createFile(fileName.c_str())!=0)goto error_rbfcreate;

	if(pfm->openFile(fileName.c_str(),fileHandle)!=0)goto error_rbfcreate;

	for(i=0;i<6;i++)
		*((BYTE *)data+i) = 1; 							// for the overall no of pages

	for(i=4092;i<4096;i++)
		*((BYTE *)data+i) = 0; 						// for the next_page information
	fileHandle.appendPage(data);

	dbgn("page number is",fileHandle.getNumberOfPages());

	fileHandle.readPage(0,data1);
	dbgn("next page number",getNextHeaderPage((BYTE *)data1));

	if(pfm->closeFile(fileHandle)!=0)goto error_rbfcreate;

	free(data);
	free(data1);
	return 0;

	error_rbfcreate:
	free(data);
	free(data1);
	return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	PagedFileManager *pfm = PagedFileManager::instance();
	pfm->destroyFile(fileName.c_str());
	return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	pfm->openFile(fileName.c_str(),fileHandle);
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	pfm->closeFile(fileHandle);
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	return -1;
}

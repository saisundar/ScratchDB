
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
		diff= strcmp((char *)keyInput,(char *)keyIndex);
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

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	INT32 root;
	getRoot(fileHandle,root);

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

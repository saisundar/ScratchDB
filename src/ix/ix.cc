
#include "ix.h"

#define pageType(data) (*((BYTE*)data))

# ifdef debugIX
# define dbgnIX(str1,str2) cout<<"\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnIX(str1,str2) (void)0;
#endif

# ifdef debugIXU
# define dbgnIXU(str1,str2) cout<<"\t\t\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnIXU(str1,str2) (void)0;
#endif

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

// Inserts a new page of INDEX TYPE and updates the pageNum to contain the Virtual Page Number of the new Page added
// Also updates the header page to contain the correct amount of freeSpace
RC IndexManager::insertIndexNode(INT32& pageNum, FileHandle fileHandle){
	void* data = malloc(PAGE_SIZE);
	pageType(data) = 1;
	getFreeOffsetV(data) = 8;
	getSlotNoV(data) = 0;
	fileHandle.appendPage(data);
	pageNum = fileHandle.getNumberOfPages();
	fileHandle.updateFreeSpaceInHeader(pageNum,-8);
	return 0;
}

// Inserts a new page of LEAF TYPE and updates the pageNum to contain the Virtual Page Number of the new Page added
// Also updates the header page to contain the correct amount of freeSpace
RC IndexManager::insertLeafNode(INT32& pageNum, FileHandle fileHandle){

	void* data = malloc(PAGE_SIZE);
	pageType(data) = 0;
	getFreeOffsetV(data) = 16;
	getSlotNoV(data) = 0;
	fileHandle.appendPage(data);
	pageNum = fileHandle.getNumberOfPages();
	fileHandle.updateFreeSpaceInHeader(pageNum,-16);
	return 0;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
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

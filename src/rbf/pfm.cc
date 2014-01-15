#include "pfm.h"
#include<stdio.h>
#include<iostream>
using namespace std;

bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
	//do nothing
}


PagedFileManager::~PagedFileManager()
{
	//do nothing
}


RC PagedFileManager::createFile(const char *fileName)
{
	//Use stat to query os. And then create.after creation update exists table
	dbgn("we are","here ");
	if(FileExists(fileName))
		return -1;

	FILE *file;
	file=fopen(fileName,"wb");
    dbgn(2,"here ");
	fclose(file);

	// EXISTS tO BE ADDED??????
    return 0;
}


RC PagedFileManager::destroyFile(const char *fileName)
{
	//Delete that file from the disk . Also remove exists entry.invike "remove"

	if(!FileExists(fileName))

    return -1;

	remove(fileName);

	return 0;

}


RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
//	//If entry exists , then open in read mode.
//	If entry does not exist then returnerror.
//   we  have steram attributein handle.
//


   return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
//	Ii need to dealloc stream dealloc mapping.
    return -1;
}


FileHandle::FileHandle()
{
	//set stream to null
}


FileHandle::~FileHandle()
{

}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
//	This method reads the page into the memory block pointed by data. The page should exist. Note the page number starts from 0.
//
//	----
//	See if pageunum eceeds the numofpages. If so errro.
//
//	Read pagesize data using fread.

    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{

//	This method writes the data into a page specified by the pageNum. The page should exist. Note the page number starts from 0.
//
//	Similar to fread.
//
//	Refer example for writing.

    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    return -1;
}



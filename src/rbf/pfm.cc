#include "pfm.h"
#include<cstdlib>
using namespace std;

// Check if a file exists
bool FileExists(string fileName)
{
	struct stat stFileInfo;

	if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
	else return false;
}
INT32 getNextHeaderPage(BYTE *headerStart)
{

	 INT32 nxt=*(INT32 *)(headerStart+4092);

	 return nxt;

}
PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
	if(!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

// The PagedFileManager does nothing.
PagedFileManager::PagedFileManager()
{

}

// The PagedFileDestructor does nothing
PagedFileManager::~PagedFileManager()
{

}

// <createFile> tells the OS to CREATE a file.
// if the fileName exists the function returns an error code '-1' else it CREATES the file.
// ARGS:
// fileName: const char* (c - string)
RC PagedFileManager::createFile(const char *fileName)
{
	dbgn("this ","createFile");
	dbgn("Filename",fileName);
	if(FileExists(fileName))
		return -1;

	FILE *file;
	file = fopen(fileName,"wb");
	fclose(file);

	return 0;
}

// <destroyFile> tells the OS to DESTROY a file.
// if the fileName does not exist the function returns an error code '1' else it DESTROYS the file.
// ARGS:
// fileName: const char* (c - string)
RC PagedFileManager::destroyFile(const char *fileName)
{
	dbgn("this ","destroyFile");
	dbgn("Filename",fileName);
	if(!FileExists(fileName)|| (files.find(fileName)!=files.end() && files.find(fileName)->second!=0))
		return -1;
	dbgn("ref count",files.find(fileName)->second);
	remove(fileName);
	files.erase(files.find(fileName));
	return 0;
}


RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{

	//	//If entry exists , then open in read mode.
	//	If entry does not exist then returnerror.
	//   we  have steram attributein handle.
	dbgn("this ","openFile");
	dbgn("Filename",fileName);
	if(!FileExists(fileName))
		return -1;
	if(fileHandle.stream)
		return -1;
	fileHandle.fileName = fileName;
	fileHandle.stream = fopen( fileName ,"rb");
	fileHandle.mode = false;
	if(files.find(fileName)==files.end())
		files.insert(std::pair<string,INT32>(fileName,0));
	files[fileName]++;
	dbgn("ref count_open_file",files[fileName]);
	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	//	Ii need to dealloc stream
	//  ????? Check if file is open
	dbgn("this ","closeFile");
	dbgn("Filename",fileHandle.fileName);
	if(fileHandle.stream==0)
		return -1;

	fclose(fileHandle.stream);

	if(fileHandle.mode)
		files[fileHandle.fileName] = -1*files[fileHandle.fileName];
	files[fileHandle.fileName]--;
	fileHandle.stream = 0;
	return 0;
}


FileHandle::FileHandle()
{
	stream = 0;
	mode = false;
}


FileHandle::~FileHandle()
{
	//no freeing required
	if(stream!=0)
		fclose(stream);
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	//	This method reads the page into the memory block pointed by data.
	//  The page should exist. Note the page number starts from 0.
	//	See if pageunum eceeds the numofpages. If so errro.
	//	Read pagesize data using fread.
	if(pageNum>=getNumberOfPages())
		return -1;
	fseek(stream,pageNum*PAGE_SIZE,SEEK_SET);
//	if(fileHandle.mode)
//			files[fileHandle.fileName] = -1*files[fileHandle.fileName]; ?????????
	fread(data, 1, PAGE_SIZE, stream);
	return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{

	//	This method writes the data into a page specified by the pageNum. The page should exist. Note the page number starts from 0.
	//	Similar to fread.
	//	Refer example for writing.
	PagedFileManager *pfm = PagedFileManager::instance();
	if((pfm->files[fileName]<0 && !mode)||pageNum>=getNumberOfPages())
		return -1;
	dbgn("this ","writePage");
	dbgn("page num",pageNum);
	if(!mode)
	{
    	freopen(fileName.c_str(),"r+b",stream);
       	(pfm->files[fileName]) = -1*(pfm->files[fileName]);
       	mode=true;
	}
	fseek(stream,pageNum*PAGE_SIZE,SEEK_SET);
	fwrite(data, 1, PAGE_SIZE, stream);
	fflush(stream);
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
	PagedFileManager *pfm = PagedFileManager::instance();
		if(pfm->files[fileName]<0 && !mode)
			return -1;
        if(!mode)
		{
        	freopen(fileName.c_str(),"r+b",stream);
        	(pfm->files[fileName]) = -1*(pfm->files[fileName]);
        	mode=true;
		}

		fseek(stream,0,SEEK_END);
		fwrite(data, 1, PAGE_SIZE, stream);
		fflush(stream);
		return 0;

}


unsigned FileHandle::getNumberOfPages()
{
	if(stream==0)
	{ cout<<" NO STREAM PRESENT!!!";
	return 0;
	}
	void *data=malloc(PAGE_SIZE);

	readPage(0,data);
    INT32 pgn=*(INT32 *)data;
    free(data);
	return(pgn);


}



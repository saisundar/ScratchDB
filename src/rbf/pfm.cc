#include "pfm.h"
#include <cstdlib>
using namespace std;

// Check if a file exists
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

// The PagedFileManager does nothing.
PagedFileManager::PagedFileManager()
{

}

// The PagedFileDestructor does nothing
PagedFileManager::~PagedFileManager()
{

}

INT32 PagedFileManager::insertHeader(FILE* fileStream)
{
	void *data = malloc(PAGE_SIZE);
	for(int i=0;i<PAGE_SIZE;i++)
		*((BYTE *)data+i) = 0;
	fseek(fileStream,0,SEEK_END);
	INT32 end = ftell(fileStream);
	fwrite(data,1,PAGE_SIZE,fileStream);
	free(data);
	return end/PAGE_SIZE;
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
	insertHeader(file);
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
	if(files.find(fileName)!=files.end())dbgn("ref count",files.find(fileName)->second);
	remove(fileName);
	if(files.find(fileName)!=files.end())
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

// <closeFile> Closes the stream associated with the FileHandle object passed as an argument
// It checks for a stream associated with the fileHandle, if none exist it returns an error
// if it exists, it closes that stream
// This function also updates the count of open streams of a file(read/write)
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
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

//Initializes the stream and mode
FileHandle::FileHandle()
{
	stream = 0;
	mode = false;
}

//Make sure stream is deallocated
FileHandle::~FileHandle()
{
	//no freeing required
	if(stream!=0)
		fclose(stream);
}

INT32 FileHandle::getNextHeaderPage(int pageNum)
{
	INT32 pgn;
	fseek(stream,(pageNum*PAGE_SIZE)+4092,SEEK_SET);
	fread(&pgn, 4, 1, stream);
	return (pgn);
}

INT32 FileHandle::getHeaderPageNum(INT32 pageNum)
{
	if(pageNum>=getNumberOfPages()) return -1;

	INT32 headPageNum=(pageNum)/681;INT32 tempPgNum=0;
	for(int i=0;i<headPageNum;i++)
		tempPgNum=getNextHeaderPage(tempPgNum);
	return tempPgNum;
}
INT32 FileHandle::translatePageNum(INT32 pageNum)
{
	if(pageNum>=getNumberOfPages()) return -1;
	INT32 headPageNum=getHeaderPageNum(pageNum);
	INT32 offset=pageNum%681;
	if(headPageNum==-1)return -1;
	INT32 actualPgNum;
	fseek(stream,(headPageNum*PAGE_SIZE)+((offset+1)*PES),SEEK_SET);
	fread(&actualPgNum, 4, 1, stream);
	return actualPgNum;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
	//	This method reads the page into the memory block pointed by data.
	//  The page should exist. Note the page number starts from 0.
	//	See if pageunum eceeds the numofpages. If so errro.
	//	Read pagesize data using fread.
	if(pageNum>=getNumberOfPages())
		return -1;
	INT32 actualPgNum=translatePageNum(pageNum);
	dbgn("this ","readPage");
	dbgn("virtual page num",pageNum);
	dbgn("actual page num",actualPgNum);
	if(actualPgNum==-1)return -1;
	fseek(stream,actualPgNum*PAGE_SIZE,SEEK_SET);
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
	INT32 actualPgNum=translatePageNum(pageNum);
	dbgn("this ","readPage");
	dbgn("virtual page num",pageNum);
	dbgn("actual page num",actualPgNum);
	if(actualPgNum==-1)return -1;
	if(!mode)
	{
    	freopen(fileName.c_str(),"r+b",stream);
       	(pfm->files[fileName]) = -1*(pfm->files[fileName]);
       	mode=true;
	}
	fseek(stream,actualPgNum*PAGE_SIZE,SEEK_SET);
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

	INT32 entry = ftell(stream);
	entry = entry/PAGE_SIZE;

	fwrite(data, 1, PAGE_SIZE, stream);
	fflush(stream);

	// Increase Page count in Header File
	void* pgCntStream = malloc(4);
	fseek(stream,0,SEEK_SET);
	fread(pgCntStream, 1, 4, stream);
	INT32 pageCount = *((INT32 *)pgCntStream);
	pageCount++;
	free(pgCntStream);
	pgCntStream = (void*)&pageCount;
	fseek(stream,0,SEEK_SET);
	fwrite(pgCntStream, 1, 4, stream);

	// Make an entry for the page which was appended
	// Check if current header has sufficient space
	int inHeaderPosition = pageCount%681;
	INT32  currentHeaderPage = getHeaderPageNum(pageCount-1);
	// Sufficient space is not available, So insert new header page
	if(inHeaderPosition==0){

		PagedFileManager *pfm = PagedFileManager::instance();
		INT32 newHeaderPage = pfm->insertHeader(stream);

		// Link the new header page
		fseek(stream,(currentHeaderPage*PAGE_SIZE)+4092,SEEK_SET);
		fwrite((void*)&newHeaderPage,1,4,stream);
		currentHeaderPage = newHeaderPage;
	}

	fseek(stream,currentHeaderPage*PAGE_SIZE + PES*(inHeaderPosition+1),SEEK_SET);
	fwrite((void*)&entry,1,4,stream);
	return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	if(stream==0)
	{ cout<<" NO STREAM PRESENT!!!";
	return 0;
	}
	INT32 pgn;
	fseek(stream,0,SEEK_SET);
	fread(&pgn, 4, 1, stream);
	return(pgn);
}



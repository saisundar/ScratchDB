#include "pfm.h"
#include <cstdlib>
using namespace std;

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
	_pf_manager=NULL;
}

//	<insertHeader> is used to insert a new HEADER PAGE into the heap file
//	It creates a standard header file(with proper initializations) and adds it to the end of the stream
//	IMPORTANT: It returns the ACTUAL PAGE NUMBER of the header block newly inserted.
//	IMPORTANT: It is the job of the function calling <insertHeader> to link
//		the new HEADER PAGE to the previous one
//	CALLED BY:
//	<createFile>: the first header is created at when the file is created.
//	<appendPage>: the appendPage functions adds pages and hence knows when to create a new HEADER PAGE

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
// if the fileName does not exist the function returns an error code '-1' else it DESTROYS the file.
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

//	<openFile> open a file which is already created by the program and associate it with a fileHandle
//	Returns an error if file does not exist OR the fileHandle is already associated to a file
//  This function ALWAYS assigns read privileges ONLY while opening the file
//	It also updates the map::<fileName, fileHandle Count> if a new file is opened.

RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
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

//  TAKES ACTUAL PAGE NUMBER of HEADER as input
//	RETURNS ACTUAL PAGE NUMBER of next HEADER by reading last 4 bytes
INT32 FileHandle::getNextHeaderPage(INT32 pageNum)
{
	INT32 pgn;
	fseek(stream,(pageNum*PAGE_SIZE)+4092,SEEK_SET);
	fread(&pgn, 4, 1, stream);
	return (pgn);
}

//	RETURNS ACTUAL PAGE NUMBER OF HEADER PAGE where the record for VIRTUAL PAGE NUMBER pageNum is stored
INT32 FileHandle::getHeaderPageNum(INT32 pageNum)
{
	if(pageNum>=getNumberOfPages()) return -1;

	INT32 headPageNum=(pageNum)/681;INT32 tempPgNum=0;
	dbgn("this ","get header pagnum");
		dbgn("vrtl pg num ",pageNum);

	for(int i=0;i<headPageNum;i++)
		tempPgNum=getNextHeaderPage(tempPgNum);
	dbgn("headr pg num ",tempPgNum);
	return tempPgNum;
}

//	<translatePageNum> Takes VIRTUAL PAGE NUMBER AS INPUT
//	reads the Header pages and	RETURNS ACTUAL PAGE NUMBER
INT32 FileHandle::translatePageNum(INT32 pageNum)
{
	if(pageNum>=getNumberOfPages()) return -1;
	INT32 headPageNum=getHeaderPageNum(pageNum);
	dbgn("this ","tranlsate pagnum");
	dbgn("vrtl pg num ",pageNum);
	dbgn("headr pg num ",headPageNum);
	INT32 offset=pageNum%681;
	if(headPageNum==-1)return -1;
	INT32 actualPgNum;
	fseek(stream,(headPageNum*PAGE_SIZE)+((offset+1)*PES),SEEK_SET);
	fread(&actualPgNum, 1, 4, stream);
	dbgn("actual pg num ",actualPgNum);
	return actualPgNum;
}

//	<readPage> allows reading at Page Level from the heap file
//	Parameters: pageNum : VIRTUAL Page Number
//  			data	: void* pointer to block of data where it is to be read
//	Checks	: pageNum should exist(STARTS from 0)
//  ??? If a FileHandle is assigned Write privileges and is currently Reading data,
//     should its write privileges be revoked and hence freed for other users ?
RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(pageNum>=getNumberOfPages())
		return -1;
	INT32 actualPgNum=translatePageNum(pageNum);
	dbgn("this ","readPage");
	dbgn("virtual page num",pageNum);
	dbgn("actual page num",actualPgNum);
	if(actualPgNum==-1)return -1;
	fseek(stream,actualPgNum*PAGE_SIZE,SEEK_SET);
	fread(data, 1, PAGE_SIZE, stream);
	return 0;
}

//	<readPage> allows writing at Page Level into the heap file, it also ensures that only one fileHandle
//			can write into the file
//	Parameters: pageNum : VIRTUAL Page Number
//  			data	: void* pointer to block of data which contains what is to be written
//	Checks	: pageNum should exist(STARTS from 0)
//			: IF there are fileHandles currently writing into the heap file AND it is not the current
//				fileHandle, return error
//			: IF the current file does not have write privileges and no other fileHandles are writing
//				into the heap file, ASSIGN current fileHandle write privileges

RC FileHandle::writePage(PageNum pageNum, const void *data)
{

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

	INT32 actualPageNo = ftell(stream);
	actualPageNo = actualPageNo/PAGE_SIZE;
	dbgn("page no of New page added:",actualPageNo);
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
	INT32 virtualPageNo = pageCount-1;
	int inHeaderPosition = (virtualPageNo)%681;
	INT32  currentHeaderPage = getHeaderPageNum(virtualPageNo);

	// Sufficient space is not available, So insert new header page
	if(virtualPageNo >680 && inHeaderPosition==0){

		PagedFileManager *pfm = PagedFileManager::instance();
		INT32 newHeaderPage = pfm->insertHeader(stream);
		currentHeaderPage = getHeaderPageNum(virtualPageNo-1);
		// Link the new header page
		fseek(stream,(currentHeaderPage*PAGE_SIZE)+4092,SEEK_SET);
		fwrite((void*)&newHeaderPage,1,4,stream);
		currentHeaderPage = newHeaderPage;
	}
	//Write the new entry for the page in the directory.
	fseek(stream,currentHeaderPage*PAGE_SIZE + PES*(inHeaderPosition+1),SEEK_SET);
	fwrite((void*)&actualPageNo,1,4,stream);

	//Update the free space in the directory for that page to 4092 Bytes
	fseek(stream,currentHeaderPage*PAGE_SIZE + PES*(inHeaderPosition+1) + 4,SEEK_SET);
	INT16 freeSpace = 4092;
	fwrite((void*)&freeSpace,1,2,stream);
	dbgn("Appendpage: actual page no of newly appended page",translatePageNum(virtualPageNo));
	dbgn("number of pages after insertion",getNumberOfPages());
	return 0;

}

//	<getNumberOfPages> is used to get the number of VIRTUAL PAGES in a heap file
//		This is stored in the first 4 BYTES  of the heap file. this function simply reads
//		these 4 bytes into an integer and returns the integer.
//		It returns an error if no stream is assigned to the fileHandle
unsigned FileHandle::getNumberOfPages()
{
	if(stream==0)
	{ cout<<" NO STREAM PRESENT!!!";
	return 0;
	}
	INT32 pgn=-1;
	fseek(stream,0,SEEK_SET);
	fread(&pgn, 4, 1, stream);
	return(pgn);
}

// <updateFreeSpaceInHeader> updates the free space in the header page by the amount specified in the parameter

INT16 FileHandle::updateFreeSpaceInHeader(PageNum pageNum, INT16 increaseBy){
	//Update FreeSpace in Header Page
	INT32 headerPageNumber = getHeaderPageNum(pageNum);
	void* headerPageData = 0;
	fread(headerPageData,1,PAGE_SIZE,stream);
	int i=0;
	for(i=10;i<4096;i+=6){
		if((*((INT32*)((BYTE*)headerPageData + i)))==pageNum){
			break;
		}
	}
	INT16 oldFreespace = *((INT32*)((BYTE*)headerPageData + i + 4));
	*((INT32*)((BYTE*)headerPageData + i + 4)) = oldFreespace + increaseBy;
	free(headerPageData);
	return oldFreespace + increaseBy;
}

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
	dbgnPFMU("<PFMu----insertHeader----PFMu>","");
	void *data = malloc(PAGE_SIZE);
	for(int i=0;i<PAGE_SIZE;i++)
		*((BYTE *)data+i) = 0;
	fseek(fileStream,0,SEEK_END);
	INT32 end = ftell(fileStream);
	fwrite(data,1,PAGE_SIZE,fileStream);
	free(data);
	dbgnPFMU("</PFMu---insertHeader---PFMu/>","");
	return end/PAGE_SIZE;
}

// <createFile> tells the OS to CREATE a file.
// if the fileName exists the function returns an error code '-1' else it CREATES the file.
// ARGS:
// fileName: const char* (c - string)
RC PagedFileManager::createFile(const char *fileName)
{
	dbgnPFM("<PFM---------createFile---------PFM>","");
	dbgnPFM("Filename",fileName);
	if(FileExists(fileName))
	{
		dbgnPFM(fileName,"already exists");
		dbgnPFM("</PFM--------createFile--------PFM/>","");
		return -1;
	}

	FILE *file;
	file = fopen(fileName,"wb");
	insertHeader(file);
	fclose(file);
	dbgnPFM("</PFM--------createFile--------PFM/>","");
	return 0;
}

// <destroyFile> tells the OS to DESTROY a file.
// if the fileName does not exist the function returns an error code '-1' else it DESTROYS the file.
// ARGS:
// fileName: const char* (c - string)
RC PagedFileManager::destroyFile(const char *fileName)
{
	dbgnPFM("<PFM--------destroyFile--------PFM>","");
	dbgnPFM("Filename",fileName);
	if(!FileExists(fileName)|| (files.find(fileName)!=files.end() && files.find(fileName)->second!=0))
		return -1;
	if(files.find(fileName)!=files.end())
		dbgnPFM("Reference count for file",files[fileName]);
	RC rc=remove(fileName);
	if(rc==0)
	{
		dbgnPFM("File destroyed ","");
		if(FileExists(fileName))
		{
			dbgnPFM(fileName,"oops still exists----some problem dude..destroy is  not working");
			dbgnPFM("</PFM--------createFile--------PFM/>","");
			return -1;
		}
		if(files.find(fileName)!=files.end())
			files.erase(files.find(fileName));
		dbgnPFM("</PFM-------destroyFile-------PFM/>","");
		return 0;
	}
	dbgnPFM("File not destroyed ","some filsystem problem presumably");
	return rc;
}

//	<openFile> open a file which is already created by the program and associate it with a fileHandle
//	Returns an error if file does not exist OR the fileHandle is already associated to a file
//  This function ALWAYS assigns read privileges ONLY while opening the file
//	It also updates the map::<fileName, fileHandle Count> if a new file is opened.

RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	dbgnPFM("<PFM---------openFile---------PFM>","");
	dbgnPFM("Filename",fileName);
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
	dbgnPFM("Reference count for file",files[fileName]);
	dbgnPFM("</PFM--------openFile--------PFM/>","");

	return 0;
}

// <closeFile> Closes the stream associated with the FileHandle object passed as an argument
// It checks for a stream associated with the fileHandle, if none exist it returns an error
// if it exists, it closes that stream
// This function also updates the count of open streams of a file(read/write)
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	dbgnPFM("<PFM---------closeFile---------PFM>","");
	dbgnPFM("Filename",fileHandle.fileName);
	if(fileHandle.stream==0)
		return -1;

	if(files.find(fileHandle.fileName)==files.end()){
		dbgnPFM("CLOSING A FILE WHICH IS NOT OPENED",fileHandle.fileName);
		return -1;
	}

	if(fileHandle.mode)
		files[fileHandle.fileName] = -1*files[fileHandle.fileName];
	files[fileHandle.fileName]--;
	dbgnPFM("reference count for file",files[fileHandle.fileName]);

	fclose(fileHandle.stream);
	fileHandle.stream = 0;
	dbgnPFM("</PFM--------closeFile--------PFM/>","");
	return 0;
}

//Initializes the stream and mode
FileHandle::FileHandle()
{
	dbgnFHU("<FHu----filehanld opened----FHu>","");
	stream = 0;
	mode = false;
}

//Make sure stream is deallocated
FileHandle::~FileHandle()
{
	dbgnFHU("<FHu----filehanld eclosed----FHu>","");

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
	dbgnFHU("<FHu----gHeaderPageNum----FHu>","");

	// Request to find the header page of a data page which does not exist
	if(pageNum>=getNumberOfPages()) return -1;

	INT32 headPageNum=(pageNum)/681;INT32 tempPgNum=0;
	dbgnFHU("Virtual page no ",pageNum);

	for(int i=0;i<headPageNum;i++)
		tempPgNum=getNextHeaderPage(tempPgNum);
	dbgnFHU("Header page no ",tempPgNum);

	dbgnFHU("</FHu---gHeaderPageNum---FHu/>","");
	return tempPgNum;
}

//	<translatePageNum> Takes VIRTUAL PAGE NUMBER AS INPUT
//	reads the Header pages and	RETURNS ACTUAL PAGE NUMBER
INT32 FileHandle::translatePageNum(INT32 pageNum)
{
	dbgnFHU("<FHu----traslatePageNum----FHu>","");

	//
	if(pageNum>=getNumberOfPages()) return -1;

	INT32 headPageNum=getHeaderPageNum(pageNum);
	dbgnFHU("Virtual page no ",pageNum);
	dbgnFHU("Header page no ",headPageNum);

	INT32 offset=pageNum%681;

	if(headPageNum==-1)return -1;

	INT32 actualPgNum=-1;
	fseek(stream,(headPageNum*PAGE_SIZE)+((offset+1)*PES),SEEK_SET);
	fread(&actualPgNum, 1, 4, stream);
	dbgnFHU("Actual page no ",actualPgNum);

	dbgnFHU("</FHu---traslatePageNum---FHu/>","");
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
	dbgnFH("<FH----------readPage----------FH>","");
	// If request to read a page which does not exist is made throw error
	if(pageNum>=getNumberOfPages()){
		dbgnFH("Number of pages exceeds","total pages")
		dbgnFH("</FH---------readPage---------FH/>","");
		return -1;
	}
	INT32 actualPgNum=translatePageNum(pageNum);
	dbgnFH("Virtual page no",pageNum);
	dbgnFH("Actual page no",actualPgNum);

	if(actualPgNum==-1)return -1;

	fseek(stream,actualPgNum*PAGE_SIZE,SEEK_SET);
	fread(data, 1, PAGE_SIZE, stream);
	dbgnFH("</FH---------readPage---------FH/>","");
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
	dbgnFH("<FH----------writePage----------FH>","");
	PagedFileManager *pfm = PagedFileManager::instance();

	// If current file does not have write permission and some other file is assigned write permissions
	// throw an error
	// If file is not opened throw error
	// If request to write to a page which does not exist is made, throw error

	if((pfm->files[fileName]<0 && !mode)||pageNum>=getNumberOfPages())
		return -1;
	INT32 actualPgNum=translatePageNum(pageNum);
	dbgnFH("Virtual page no",pageNum);
	dbgnFH("Actual page no",actualPgNum);
	if(actualPgNum==-1)return -1;

	// If write permissions not given, give write permissions
	if(!mode)
	{
		freopen(fileName.c_str(),"r+b",stream);
		(pfm->files[fileName]) = -1*(pfm->files[fileName]);
		mode=true;
	}
	fseek(stream,actualPgNum*PAGE_SIZE,SEEK_SET);
	fwrite(data, 1, PAGE_SIZE, stream);
	fflush(stream);
	dbgnFH("</FH---------writePage---------FH/>","");
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
	dbgnFH("<FH----------AppendPage----------FH>","");
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
	dbgnFH("page no of New page added:",actualPageNo);
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
	dbgnFH("AppendPage: actual page no of newly appended page",translatePageNum(virtualPageNo));
	dbgnFH("number of pages after insertion",getNumberOfPages());
	dbgnFH("</FH---------AppendPage--------FH/>","");
	return 0;

}

//	<getNumberOfPages> is used to get the number of VIRTUAL PAGES in a heap file
//		This is stored in the first 4 BYTES  of the heap file. this function simply reads
//		these 4 bytes into an integer and returns the integer.
//		It returns an error if no stream is assigned to the fileHandle
unsigned FileHandle::getNumberOfPages()
{
	dbgnFHU("<FHu----noOFPages----FHu>","");
	if(stream==0)
	{
		dbgnFHU("Stream error","Does not exist!");
		return -1;
	}
	INT32 pgn=-1;
	fseek(stream,0,SEEK_SET);
	fread(&pgn, 4, 1, stream);
	dbgnFHU("</FHu---noOFPages---FHu/>","");
	return(pgn);
}

// <updateFreeSpaceInHeader> updates the free space in the header page by the amount specified in the parameter
// If called with increasedBy = 0, will return current free space.
INT16 FileHandle::updateFreeSpaceInHeader(PageNum pageNum, INT16 increaseBy){

	dbgnFH("<FH----UpdFreeSpaceIH----FH>","");
	dbgnFH("Updating FreeSpace for Page",pageNum);
	PagedFileManager *pfm = PagedFileManager::instance();
	if((pfm->files[fileName]<0 && !mode)||pageNum>=getNumberOfPages())
		return -1;
	INT32 actualPgNum=translatePageNum(pageNum);
	dbgnFH("Virtual page no",pageNum);
	dbgnFH("Actual page no",actualPgNum);
	if(actualPgNum==-1)return -1;

	// If write permissions not given, give write permissions

	INT32 headerPageNumber = getHeaderPageNum(pageNum);
	INT16 prevFreeSpace = 0;
	fseek(stream,headerPageNumber*PAGE_SIZE+(pageNum % 681)*6 + 10,SEEK_SET);
	fread(&prevFreeSpace,1,2,stream);
	if(increaseBy==0){
		dbgnFH("Called for current freeSpace","");
		dbgnFH("</FH---UpdFreeSpaceIH---FH/>","");
		return prevFreeSpace;
	}
	if(!mode)
	{
			freopen(fileName.c_str(),"r+b",stream);
			(pfm->files[fileName]) = -1*(pfm->files[fileName]);
			mode=true;
	}
	dbgnFH("Old Free Space: ",prevFreeSpace);
	dbgnFH("Increase by",increaseBy);

	prevFreeSpace += increaseBy;
	dbgAssert(mode);
	fseek(stream,headerPageNumber*PAGE_SIZE+(pageNum % 681)*6 + 10,SEEK_SET);
	fwrite(&prevFreeSpace,1,2,stream);

	// Should we still keep this ?
	fseek(stream,headerPageNumber*PAGE_SIZE+(pageNum % 681)*6 + 10,SEEK_SET);
	fread(&prevFreeSpace,1,2,stream);

	dbgnFH("Updated Free Space(Read from HeaderFile)",prevFreeSpace);
	dbgnFH("</FH---UpdFreeSpaceIH---FH/>","");
	return prevFreeSpace;
}

// Function to check if the file exists in the file system
bool FileExists(string fileName)
{
	struct stat stFileInfo;
	if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
	else return false;
}

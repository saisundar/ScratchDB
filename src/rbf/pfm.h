#ifndef _pfm_h_
#define _pfm_h_
#include <inttypes.h>
#include <sys/stat.h>
#include <string>
#include <cstring>
#include <stdio.h>
#include <map>
#include <cassert>
#include<iostream>
#include<iomanip>
#include<stdio.h>
#include<cstdlib>
using namespace std;

#define debugAss 1
#define debugIX 1
#define debugIXU 1
#define debugFH 1
#define debugFHU 1
#define debugPFM 1
#define debugPFMU 1
//#define debugRBFM 1
//#define debugRBFMU 1
//#define debug1 1
//#define debug2 1
////#define debug3 1

typedef int32_t INT32;
typedef int16_t INT16;
typedef int8_t BYTE;
typedef float FLOAT;
#define PES 6
#define TOMBSIZE 6
#define isNull(num) (num==1346458179)
#define isNullA(addr) (*(INT32 *)addr==1346458179)
#define intVal(addr) (*(INT32 *)addr)
#define modlus(a)  (((a)>0)?(a):(-a))

#define maxim(a,b) a>b?a:b

# ifdef debugAss
# define dbgAssert(cond) assert(cond);
# else
# define dbgAssert(cond) (void)0;
#endif

# ifdef debugRBFM
# define dbgnRBFM(str1,str2) cout<<"\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnRBFM(str1,str2) (void)0;
#endif

# ifdef debugRBFMU
# define dbgnRBFMU(str1,str2) cout<<"\t\t\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnRBFMU(str1,str2) (void)0;
#endif

# ifdef debugPFMU
# define dbgnPFMU(str1,str2) cout<<"\t\t\t\t\t\t\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnPFMU(str1,str2) (void)0;
#endif

# ifdef debugFHU
# define dbgnFHU(str1,str2) cout<<"\t\t\t\t\t\t\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnFHU(str1,str2) (void)0;
#endif

# ifdef debugPFM
# define dbgnPFM(str1,str2) cout<<"\t\t\t\t\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnPFM(str1,str2) (void)0;
#endif

# ifdef debugFH
# define dbgnFH(str1,str2) cout<<"\t\t\t\t\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnFH(str1,str2) (void)0;
#endif

# ifdef debug1
# define dbgn1(str1,str2) cout<<setw(50)<<str1<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgn1(str1,str2) (void)0;
#endif

# ifdef debug2
# define dbgn2(str1,str2) cout<<setw(50)<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgn2(str1,str2) (void)0;
#endif

# define getSlotOffA(page,i) ((BYTE *)page+4088-(i*4)) 				// gives the address of slot offset
# define getSlotLenA(page,i) ((BYTE *)page+4090-(i*4)) 				// gives the address of slot length
# define getSlotOffV(page,i) (*(INT16 *)((BYTE *)page+4088-(i*4)))  // gives the address of slot offset Value
# define getSlotLenV(page,i) (*(INT16 *)((BYTE *)page+4090-(i*4)))  // gives the address of slot length Value
# define getFreeOffsetA(page) ((BYTE *)page+4094)					// gives the address of within the page
# define getSlotNoA(page) ((BYTE *)page+4092)					    // gives the adress of "num of slots" field within the page
# define getFreeOffsetV(page) *(INT16 *)((BYTE *)page+4094)			// gives the freeoffset value within the page
# define getSlotNoV(page) *(INT16 *)((BYTE *)page+4092)				// gives the num of slots value within the page
# define getRecordAtSlot(page,i) ((BYTE*)page+getSlotOffV(page,i-1))//gives the starting adress of the record at slot i

typedef INT32 RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096
using namespace std;

bool FileExists(string fileName);
INT32 getNextHeaderPage(BYTE *headerStart);

class FileHandle;

class PagedFileManager
{
public:
	static PagedFileManager* instance();                     		 // Access to the _pf_manager instance

	map< string ,INT32> files;												 // Maintain record of files created

	RC createFile    (const char *fileName);                         // Create a new file
	RC destroyFile   (const char *fileName);                         // Destroy a file
	RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
	RC closeFile     (FileHandle &fileHandle);                       // Close a file
	INT32 insertHeader  (FILE* fileStream);											 // Inserts Header Page

protected:
	PagedFileManager();                                   // Constructor
	~PagedFileManager();                                  // Destructor

private:
	static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
	FileHandle();                                                    // Default constructor
	~FileHandle();                                                   // Destructor
	FILE* stream;
	string fileName;
	bool mode;															//0 for read,1 for write
	INT32 translatePageNum(INT32 pagenum);
	INT32 getNextHeaderPage(INT32 pageNum);
	INT32 getHeaderPageNum(INT32 pageNum);
	RC readPage(PageNum pageNum, void *data);                           // Get a specific page
	RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
	RC appendPage(const void *data);                                    // Append a specific page
	unsigned getNumberOfPages();                                        // Get the number of pages in the file
	INT16 updateFreeSpaceInHeader(PageNum pageNum, INT16 increaseBy);
};

#endif

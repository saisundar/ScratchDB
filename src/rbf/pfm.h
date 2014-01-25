#ifndef _pfm_h_
#define _pfm_h_
#include <inttypes.h>
#include <sys/stat.h>
#include <string>
#include <cstring>
#include <stdio.h>
#include <map>
#include<iostream>
#include<stdio.h>
using namespace std;

//# define debug 1
typedef int32_t INT32;
typedef int16_t INT16;
typedef int8_t BYTE;
typedef float FLOAT;
#define PES 6
# ifdef debug
# define dbgn(str1,str2) cout<<(str1)<<": "<<(str2)<<"\n";
# else
# define dbgn(str1,str2) (void)0;
#endif


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
 };

 #endif

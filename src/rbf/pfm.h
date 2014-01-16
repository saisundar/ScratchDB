#ifndef _pfm_h_
#define _pfm_h_

#include <sys/stat.h>
#include <string>
#include <stdio.h>
#include <map>
using namespace std;

# define debug 1

# ifdef debug
# define dbgn(str1,str2) cout<<str1<<": "<<str2<<"\n";
# else
# define dbgn(str1,str2) (void)0;
#endif


typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096
#define debug 1

using namespace std;

bool FileExists(string fileName);

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     		 // Access to the _pf_manager instance

    map< string ,int> files;												 // Maintain record of files created

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file

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
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
 };

 #endif

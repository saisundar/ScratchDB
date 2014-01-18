#include <fstream>
#include <iostream>
#include <cassert>
#include<cstdlib>
#include "pfm.h"
#include "rbfm.h"

using namespace std;
const int success = 0;

int RBFTest_3(PagedFileManager *pfm)
{
	 cout << "****In RBF Test Case 7****" << endl;

	    RC rc;
	    string fileName = "test_2";

	    // Create the file named "test_2"
	    rc = pfm->createFile(fileName.c_str());
//	    assert(rc == success);

	    if(FileExists(fileName.c_str()))
	    {
	        cout << "File " << fileName << " has been created." << endl;
	    }
	    else
	    {
	        cout << "Failed to create file!" << endl;
	        cout << "Test Case 7 Failed!" << endl << endl;
	        return -1;
	    }

	    // Open the file "test_2"
	    FileHandle fileHandle;
	    rc = pfm->openFile(fileName.c_str(), fileHandle);
	    assert(rc == success);

	    // Append 50 pages
	    void *data = malloc(PAGE_SIZE);
	    for(unsigned j = 0; j < 50; j++)
	    {
	        for(unsigned i = 0; i < PAGE_SIZE; i++)
	        {
	            *((char *)data+i) = i % (j+1) + 32;
	        }
	        rc = fileHandle.appendPage(data);
	        assert(rc == success);
	    }
	    cout << "50 Pages have been successfully appended!" << endl;

	    // Get the number of pages
	    unsigned count = fileHandle.getNumberOfPages();
//	    assert(count == (unsigned)50);

	    // Read the 25th page and check integrity
	    void *buffer = malloc(PAGE_SIZE);
	    rc = fileHandle.readPage(24, buffer);
	    assert(rc == success);

	    for(unsigned i = 0; i < PAGE_SIZE; i++)
	    {
	        *((char *)data + i) = i % 25 + 32;
	    }

	    rc = memcmp(buffer, data, PAGE_SIZE);
	    assert(rc == success);
	    cout << "The data in 25th page is correct!" << endl;

	    // Update the 25th page
	    for(unsigned i = 0; i < PAGE_SIZE; i++)
	    {
	        *((char *)data+i) = i % 60 + 32;
	    }
	    rc = fileHandle.writePage(24, data);
	    assert(rc == success);

	    // Read the 25th page and check integrity
	    rc = fileHandle.readPage(24, buffer);
	    assert(rc == success);

	    rc = memcmp(buffer, data, PAGE_SIZE);
	    assert(rc == success);

	    // Close the file "test_2"
	    rc = pfm->closeFile(fileHandle);
	    assert(rc == success);

	    // DestroyFile
	    rc = pfm->destroyFile(fileName.c_str());
	    assert(rc == success);

	    free(data);
	    free(buffer);

	    if(!FileExists(fileName.c_str()))
	    {
	        cout << "File " << fileName << " has been destroyed." << endl;
	        cout << "Test Case 7 Passed!" << endl << endl;
	        return 0;
	    }
	    else
	    {
	        cout << "Failed to destroy file!" << endl;
	        cout << "Test Case 7 Failed!" << endl << endl;
	        return -1;
	    }
}
void rbfTest()
{
	PagedFileManager *pfm = PagedFileManager::instance();
	RBFTest_3(pfm);
	// RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	// write your own testing cases here
//	string text="test";s
//	if(FileExists(text.c_str()))remove(text);
//	pfm->createFile(text.c_str());
//	FileHandle fileHandle;
//	pfm->openFile("test",fileHandle);
//	FileHandle fileHandle2;
//	pfm->openFile("test",fileHandle2);
//	pfm->destroyFile("test");

}


int main() 
{
	cout << "test..." << endl;

	rbfTest();
	// other tests go here

	cout << "OK" << endl;
}

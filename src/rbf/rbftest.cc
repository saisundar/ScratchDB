#include <fstream>
#include <iostream>
#include <cassert>

#include "pfm.h"
#include "rbfm.h"

using namespace std;


void rbfTest()
{
	PagedFileManager *pfm = PagedFileManager::instance();
	// RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	// write your own testing cases here
	if(FileExists("test"))remove("test");
	pfm->createFile("test");
	FileHandle fileHandle;
	pfm->openFile("test",fileHandle);
	FileHandle fileHandle2;
	pfm->openFile("test",fileHandle2);
	pfm->destroyFile("test");

}


int main() 
{
	cout << "test..." << endl;

	rbfTest();
	// other tests go here

	cout << "OK" << endl;
}

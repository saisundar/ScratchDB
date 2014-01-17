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
	string text="test";
	if(FileExists(text.c_str()))remove(text);
	pfm->createFile(text.c_str());
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

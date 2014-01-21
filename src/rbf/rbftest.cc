#include <fstream>
#include <iostream>
#include <cassert>
#include<cstdlib>
#include "pfm.h"
#include "rbfm.h"

using namespace std;
const INT32 success = 0;

INT32 RBFTest_3(PagedFileManager *pfm)
{
	return 0;
}
INT32 RBFTest(RecordBasedFileManager *rfm)
{

	rfm->createFile("summa");
	return 0;
}
void rbfTest()
{
	//PagedFileManager *pfm = PagedFileManager::instance();
	//RBFTest_3(pfm);
	 RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	 RBFTest(rbfm);
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


INT32 main()
{
	cout << "test..." << endl;

	rbfTest();
	// other tests go here

	cout << "OK" << endl;
}

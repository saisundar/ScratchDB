#include <fstream>
#include <iostream>
#include <cassert>
#include<cstdlib>
#include "pfm.h"
#include "rbfm.h"

using namespace std;
const INT32 success = 0;


INT32 RBFTest(RecordBasedFileManager *rfm)
{

	rfm->createFile("summa");
	return 0;
}
void rbfTest()
{
	//PagedFileManager *pfm = PagedFileManager::instance();
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFTest(rbfm);
	// write your own testing cases here

}


INT32 main()
{
	cout << "test..." << endl;

	rbfTest();

	cout << "OK" << endl;
}

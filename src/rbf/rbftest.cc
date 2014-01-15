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

   pfm->createFile("summa");
   pfm->destroyFile("summa");

   pfm->createFile("summa");
  // write your own testing cases here
}


int main() 
{
  cout << "test..." << endl;

  rbfTest();
  // other tests go here

  cout << "OK" << endl;
}

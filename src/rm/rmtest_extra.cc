
#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

using namespace std;

void rmTest()
{
  // RM *rm = RM::instance();

  // write your own testing cases here
}

int main() 
{
  cout << "test..." << endl;

  rmTest();
  // other tests go here

  cout << "OK" << endl;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	void *recData=malloc(PAGE_SIZE);
	readRecord(fileHandle,recordDescriptor, rid, recData);

	BYTE * printData = (BYTE*)recData;
	INT32 num = 0;FLOAT num1;
	bool found=false;
	dbgn1("this ","readAttribute");

	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	dbgn("num of attributes",recordDescriptor.size());
	while(it != recordDescriptor.end() && !found)
	{
		switch(it->type){
		case 0:
			if((it->name).compare(attributeName)==0)
			{
				memcpy(data,printData,4);
				found=true;
				dbgn1("attribute found","integer");
			}
			printData = printData+4;
			break;

		case 1:
			if((it->name).compare(attributeName)==0)
			{
				memcpy(data,printData,4);
				found=true;
				dbgn1("attribute found","float");
			}
			printData = printData+4;

			break;

		case 2:
			num = *((INT32 *)printData);
			if((it->name).compare(attributeName)==0)
			{
				memcpy(data,printData,4+num);
				found=true;
				dbgn1("attribute found","string");
			}
			printData = printData+4+num;
			break;

		default:
			break;

		}
		++it;
	}
	return 0;
}

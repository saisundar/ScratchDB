
#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

using namespace std;

RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	void *page=malloc(PAGE_SIZE),*newPage=malloc(PAGE_SIZE); // to be freed when I exit
	dbgn1("this=================================================================== ","Reorganize called");
	dbgn1("Filename",fileHandle.fileName);
	INT16 length;

	INT16 freeOffset,origOffset,origLength=0,slotNo,freeSpace,slot=0;
	INT32 virtualPageNum=pageNumber,offset=virtualPageNum%681;

	fileHandle.readPage(virtualPageNum,page);
	fileHandle.readPage(virtualPageNum,newPage);
	freeOffset=0;
	slotNo=getSlotNoV(newPage);
	freeSpace=4092-(slotNo*4)-freeOffset;

	for(slot=0;slot<slotNo;slot++)
	{
		origOffset=getSlotOffV(page,slot);origLength=getslotLenV(page,slot);
		if( origOffset == -1)   //means that the slot is empty.
			continue;

		if(origLength>=0)
		{
			memcpy(newPage+freeOffset,page+origOffset,origLength);   //copy the record
			memcpy(getSlotOffA(newPage,slot),&freeOffset,2);			 //copy the new offset int othe slot
			//memcpy(getslotLenA(newPage,i),&origLength,2);			 //copy the new length int othe slot ---- redundant as length will alredy be there in the slot info
			freeOffset+=origLength;
		}
		else
		{
			//tombstone case-whee ineed to copy the first six bytes alone from the record
			memcpy(newPage+freeOffset,page+origOffset,6);   /// copying only 6 bytes as its a tombstone....
			memcpy(getSlotOffA(newPage,slot),&freeOffset,2);
			origLength=-6;
			memcpy(getSlotLenA(newPage,slot),&origLength,2);	//update length of slot to -6 ????? required or not ?
			freeOffset+=TOMBSIZE;								/// incrementing by 6 bytes..
		}
	}
	memcpy(getFreeOffsetA(newPage),&freeOffset,2);
	dbgn1("the new free offset after reorganizing is ",freeOffset);

	fileHandle.writePage(virtualPageNum,newPage);
	free(page);
	free(newPage);
	return 0;
}
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

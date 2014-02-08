
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
				if(isNull(num))
				memcpy(data,printData,4);
				else
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

RC RecordBasedFileManager::scan(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor,const string &conditionAttribute,const CompOp compOp,              // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator)
{
	return(rbfm_ScanIterator.setValues(fileHandle,
			recordDescriptor,
			conditionAttribute.c_str,
			compOp,
			value,
			attributeNames));

}

bool RBFM_ScanIterator::isValidAttr(string condAttr,const vector<Attribute> &recordDescriptor){
		INT32 i;
		for(i=0;i<recordDescriptor.size();i++)
			if(recordDescriptor[i].name.compare(condAttr)==0)
			{
				attrLength=recordDescriptor[i].length;
				valueP=malloc(attrLength);
				type=recordDescriptor[i].type;
				break;
			}
		return(i<recordDescriptor.size());
	}
RC RBFM_ScanIterator::setValues(FileHandle &fileHandle,							//
		  const vector<Attribute> &recordDescriptor,				//
		  const char *conditionAttribute,							//
		  const CompOp compOp,              						// comparision type such as "<" and "="
		  const void *value,                    					// used in the comparison
		  const vector<string> &attributeNames){

	  if(fileHandle.stream==0||!isValidAttr(conditionAttribute,recordDescriptor))
	  {
		  isValid=false;
		  return 1;
	  }

	  condAttr=conditionAttribute;
	  recDesc=recordDescriptor;
	  oper=compOp;
	  memcpy(valueP,value,attrLength);
	  attrNames=attributeNames;
	  currHandle=fileHandle;

	  curHeaderPage=malloc(PAGE_SIZE);
	  curDataPage=malloc(PAGE_SIZE);
	  fseek(fileHandle.stream,0,SEEK_SET);
	  fread(curHeaderPage, PAGE_SIZE, 1, stream);
	  numOfPages=*((INT32 *)curHeaderPage);
	  fileHandle.readPage(0,curDataPage);
	  numOfSlots=getSlotNoV(curDataPage);
	  currRid.pageNum=0;
	  currRid.slotNum=0;

	  return(isValid=(numOfPages>0));

  }

RC RBFM_ScanIterator::getNextDataPage()
{
	INT32 actualPageNum;
	currRid.pageNum++;
	if(currRid.pageNum==numOfPages)
		{
		currRid.slotNum==numOfSlots;
		return 0;
		}
	if(currRid.pageNum%681==0)
	{
		headerPageNum=currHandle.getNextHeaderPage(headerPageNum);
		fseek(currHandle.stream,headerPageNum*PAGE_SIZE,SEEK_SET);
		fread(curHeaderPage, PAGE_SIZE, 1, currHandle.stream);
	}
	actualPageNum=*((INT32 *)(headerPageNum+((currRid.pageNum%681+1)*PES)));
	fseek(currHandle.stream,actualPageNum*PAGE_SIZE,SEEK_SET);
	fread(curDataPage, 1, 4, currHandle.stream);
	currRid.slotNum=0;
	numOfSlots=getSlotNoV(curDataPage);
	return 0;
}

RC RBFM_ScanIterator::incrementRID(){

	if(currRid.slotNum<numOfSlots-1)
		currRid.slotNum++;
	else if(currRid.slotNum==numOfSlots-1)
		getNextDataPage();

	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	bool found=false;

	while(!found)
	{
		if(currRid.pageNum==numOfPages && currRid.slotNum==numOfSlots||!isValid)
			return RBFM_EOF;




	}


}

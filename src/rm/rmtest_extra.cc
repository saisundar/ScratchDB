
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
	dbgn1("num of attributes",recordDescriptor.size());
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


//just invokes the set values which initilaises all the freaking variables in the scan iterator object
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

// returns false if attribute is not found in the given record descriptors.
bool RBFM_ScanIterator::isValidAttr(string condAttr,const vector<Attribute> &recordDescriptor){
	INT32 i;
	dbgn1("enter RBFM_ScanIterator::isValidAttr","");
	for(i=0;i<recordDescriptor.size();i++)
		if(recordDescriptor[i].length!=0 && recordDescriptor[i].name.compare(condAttr)==0)
		{
			attrNum=i;
			attrLength=recordDescriptor[i].length;
			valueP=malloc(attrLength);
			type=recordDescriptor[i].type;
			dbgn1("attirbute found.number",i);
			isValid=true;
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

	dbgn1("enter RBFM_ScanIterator::setValues","");
	if(((!strcmp(conditionAttribute,"")) && (compOp==NO_OP) && (value== NULL))||compOp==NO_OP)
	{unconditional=true;isValid=true;}
	else if(fileHandle.stream==0||!isValidAttr(conditionAttribute,recordDescriptor))
	{
		isValid=false;
		return 1;
	}

	condAttr=conditionAttribute;
	recDesc=recordDescriptor;
	oper=compOp;
	if(value==NULL)
	{
		free(valueP);
		valueP=NULL;
	}
	else
	{
		switch(type)
		{
		case 0:
		case 1:
			memcpy(valueP,value,attrLength);
			break;
		case 2:
			memcpy(valueP,value+4,attrLength); // copy the string alone if the atribute is a string
			break;
		}

	}

	attrNames=attributeNames;
	currHandle=fileHandle;
	recDesc=recordDescriptor;
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
		currRid.slotNum=numOfSlots;
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
	fread(curDataPage, PAGE_SIZE, 1, currHandle.stream);
	currRid.slotNum=0;
	numOfSlots=getSlotNoV(curDataPage);
	return 0;
}

RC RBFM_ScanIterator::incrementRID(){

	if(currRid.slotNum<numOfSlots-1)
		currRid.slotNum++;
	else if(currRid.slotNum==numOfSlots-1)
		getNextDataPage();

	dbgn1("new RID page number",currRid.pageNum);
	dbgn1("new RID slot number",currRid.slotNum);

	return 0;
}

bool RBFM_ScanIterator::returnRes(int diff)
{
	if(diff==0)
		return((oper==EQ_OP)||(oper==LE_OP)||(oper==GE_OP));
	if(diff<0)
		return((oper==LT_OP)||(oper==LE_OP)||(oper==NE_OP));
	return((oper==GT_OP)||(oper==GE_OP)||(opers==NE_OP));
}

bool RBFM_ScanIterator::evaluateCondition(void * temp)
{
	int diff;
	bool result=false;

	switch(type)
	{
	case 0:
		diff=intVal(temp)-intVal(valueP);
		break;
	case 1:
			if(*((float *)temp)>*((float *)valueP))
				diff=1;
			else if(*((float *)temp)==*((float *)valueP))
				diff=0;
			else
				diff=-1;
		break;
	case 2:
		diff= strcmp((char *)temp,(char *)valueP);
		break;
	}
	result=returnRes(diff);
	dbgn("result of comparison",result==1);
	return(result);
}

RC RBFM_ScanIterator::getAttributeGroup(void * data,void *temp)
{
		BYTE * printData = (BYTE*)temp;
		BYTE * tempData = (BYTE *)data;
		INT32 num = 0;FLOAT num1;
		dbgn1("this ","getAttributeGroup");

		std::vector<Attribute>::const_iterator it = recDesc.begin();
		std::vector<string>::const_iterator st = attrNames.begin();
		dbgn("num of attributes",recDesc.size());
		for(it = recDesc.begin();(it != recDesc.end() && st!=attrNames.end());it++)
		{
			if(it->length==0)continue;

			switch(it->type){
			case 0:
				if((it->name).compare(st)==0)
				{
					memcpy(tempData,printData,4);
					found=true;
					dbgn1("int attribute found",st);
				}
				printData = printData+4;
				tempData+=4;
				st++;
				break;

			case 1:
				if((it->name).compare(st)==0)
				{
					memcpy(data,printData,4);
					found=true;
					dbgn1("float attribute found",st);
				}
				printData = printData+4;
				tempData+=4;
				st++;
				break;

			case 2:
				num = *((INT32 *)printData);
				if((it->name).compare(st)==0)
				{
					if(isNull(num))
						memcpy(data,printData,4);
					else
						memcpy(data,printData,4+num);
					found=true;
					dbgn1("string attribute found",st);
				}
				printData = printData+4+num;
				tempData+=4+num;
				st++;
				break;

			default:
				break;

			}
		}
		return 0;

}
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	bool found=false;
	INT16 offset=0,len=0,numOfAttr=-1,startOff,endOff,attrLen;
	void * modRecord=NULL,*temp=NULL,tempAttr=NULL;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	for(;!found;incrementRID())
	{
		if((scurrRid.pageNum==numOfPages && currRid.slotNum==numOfSlots)||!isValid)
			{
				dbgn1("end of scan....","");
				dbgn1("scan ",(unconditional)?"unconditional":"conditional");
				return RBFM_EOF;
			}

		offset=getSlotOffV(curDataPage,currRid.slotNum);
		len=getslotLenV(curDataPage,currRid.slotNum);

		if(offset<0|| len<0)continue;

		modRecord=malloc(len);temp=malloc(len);
		memcpy(modRecord,curDataPage+offset,len);
		numOfAttr=*(INT16 *)modRecord;

		if(!unconditional){								// means check for attiute condition

			if(attrNum>=numOfAttr)						//means null records at the end-===> added column but not updated===>record may be ignored.
			{	dbgn1(" desc attribute number is"," greater than disk attributes==>null");

			// see if the given value itself is null in that case this record matches....
			continue;
			}

			if(attrNum==0)
			{
				startOff=2*(numOfAttr+1);
				endOff=*((INT16 *)modRecord+2);
			}
			else
			{
				startOff=*((INT16 *)modRecord+2*(attrNum));
				endOff=*((INT16 *)modRecord+2*(attrNum+1));
			}

			attrLen=endOff-startOff;
			tempAttr=malloc(attrLen+1);

			memcpy(temp,modRecord+startOff,attrLen);
			*((char *)tempAttr+attrLen)=0;

			if(!evaluateCondition(tempAttr))
			{
				//means the record did not satisfy the criteron.
				//move to next record

				free(modRecord);free(tempAttr);free(temp);continue;

			}

		}
		found=true;
		rbfm->modifyRecordForRead(recDesc,temp,modRecord);

		getAttributeGroup(data,temp);
		free(modRecord);free(tempAttr);free(temp);

		return 0;

	}

	//wont come here if next record is not found. as it wont break and will return EOF.
	rid=currRid;
	return 0;
}

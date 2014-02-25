
#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>

#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
	unsigned pageNum;
	unsigned slotNum;
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string   name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length

	Attribute(){
		type= TypeInt;
		length=0;
		name="";

	}
	~Attribute(){
			type= TypeInt;
			length=0;
			name="";
		}
	Attribute(const Attribute &obj)
	{
		type= obj.type;
		length=obj.length;
		name=obj.name;

	}
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
	LT_OP,      // <
	GT_OP,      // >
	LE_OP,      // <=
	GE_OP,      // >=
	NE_OP,      // !=
	NO_OP       // no condition
} CompOp;

/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
 *****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
private:

	void *curHeaderPage;				//current header page
	INT32 headerPageNum;
	void *curDataPage;					//curent data page
	RID currRid;
	vector<Attribute> recDesc;			//record descriptor
	bool isValid;						//indicate swhther the iterator will produce useful info or not
	bool unconditional;
	CompOp oper;						//operation performed
	string condAttr;					//the name of the conditonla attribute
	void *valueP;						// pointer to the vlaue to be checked
	AttrType type;						//type pf attribute
	INT32 attrLength;					//length of the concerned attribute
	INT32 attrNum;
	vector<string> attrNames;			//vetor of attributes which are wanted
	INT32 numOfPages;					//overall number of data pages in file
	INT32 numOfSlots;					// number of slots in page
	FileHandle currHandle;
	bool isValidAttr(string condAttr,const vector<Attribute> &recordDescriptor);
	RC getAttributeGroup(void * data,void *temp);
public:
	RC setValues(FileHandle &fileHandle,							//
			const vector<Attribute> &recordDescriptor,				//
			const char *conditionAttribute,							//
			const CompOp compOp,              						// comparision type such as "<" and "="
			const void *value,                    					// used in the comparison
			const vector<string> &attributeNames);

	RBFM_ScanIterator() {
		curHeaderPage=NULL;
		curDataPage=NULL;
		isValid=false;
		valueP=NULL;
		attrLength=0;
		headerPageNum=0;
		attrNum=-1;
		numOfSlots = 0;
		numOfPages = 0;
		unconditional=false;

	};
	~RBFM_ScanIterator() {
		currHandle.stream=0;
		if(curHeaderPage!=NULL)free(curHeaderPage);
		if(curDataPage!=NULL)free(curDataPage);
		if(valueP!=NULL)free(valueP);
		dbgn1("destructur called for scaniterator.","");
		//NOTE that there could be a risk of handle clsoing the stream.hene set the stream=0 here.

	};

	// "data" follows the same format as RecordBasedFileManager::insertRecord()
	bool evaluateCondition(void * temp);
	bool returnRes(int diff);
	RC getNextDataPage();
	RC incrementRID();
	RC getNextRecord(RID &rid, void *data);
	RC close() {currHandle.stream=0;
	free(curHeaderPage);curHeaderPage=NULL;
	free(curDataPage);curDataPage=NULL;
	if(valueP!=NULL)free(valueP);
	valueP=NULL;
	recDesc.clear();attrNames.clear();dbgn1("vectors cleared","");return 0; };
};


class RecordBasedFileManager
{
public:
	bool isRedirected;

	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
	INT32 findFirstFreePage(FileHandle &fileHandle, INT16 requiredSpace, INT32  &headerPageNumber);

	void* modifyRecordForInsert(const vector<Attribute> &recordDescriptor,const void *data,INT16 &length);

	RC modifyRecordForRead(const vector<Attribute> &recordDescriptor,const void *data,const void *modRecord);

	RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

	RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

	// This method will be mainly used for debugging/testing
	RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/**************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
	 ***************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************/
	RC deleteRecords(FileHandle &fileHandle);

	RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

	// Assume the rid does not change after update
	RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

	RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);

	RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);


	// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:

	RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);

	// Our Functions

	INT16 getFreeSpaceBlockSize(FileHandle &fileHandle, PageNum pageNum);


protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
};

#endif

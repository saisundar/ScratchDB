
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../ix/ix.h"
#include "../rbf/rbfm.h"

using namespace std;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {

public:
	RBFM_ScanIterator rbfmsi;
	FileHandle fileHandle;
	RM_ScanIterator() {
	};
	~RM_ScanIterator() {
	};

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data) {
		dbgn2("get next tuple============","");
		if(rbfmsi.getNextRecord(rid,data)==RBFM_EOF) return RM_EOF;
		return 0;
	};
	RC close() {
		if(RecordBasedFileManager::instance()->closeFile(fileHandle)==-1){
			dbgn2("could not close the file","In close (RM Iterator)");
			return -1;
		}
		if(rbfmsi.close()==-1)return -1;
		return 0;
	};
};

class RM_IndexScanIterator {
public:
	IX_ScanIterator ix_scaniterator;

	RM_IndexScanIterator() {

	};

	~RM_IndexScanIterator() {
	};

	// "key" follows the same format as in IndexManager::insertEntry()
	RC getNextEntry(RID &rid, void *key) {
		if((ix_scaniterator.getNextEntry(rid, key)) == IX_EOF)return RM_EOF;
		return 0;
	};

	RC close() {
		return 0;
	};
};

// Relation Manager
class RelationManager
{
	vector<Attribute> systemDescriptor;
	vector<Attribute> tableDescriptor;
	FileHandle systemHandle;
	RecordBasedFileManager * rbfm; // To test the functionality of the record-based file manager
	IndexManager* im;
	map< string ,vector<Attribute> > descriptors;	   // Maintain RecordDescriptors
	string systemCatalog;
	char* tableCatalogConcat;

public:
	static RelationManager* instance();
	// Our Functions
	RC updateTableCatalogIndex(const string &tableName,INT32 hasIndexValue,const string &attributeName);

	RC updateMemDescriptor(const string &tableName,Attribute attr,INT32 loc);

	RC updateIndexIfRequired(const string &tableName,vector<Attribute> recordDescriptor, const void *data, RID rid,bool isInsert);

	RC getIndexName(const string &tableName, const string &attributeName,char* indexName, INT32 len);

	RC getAttributeObj(const string &attributeName,vector<Attribute> recordDescriptor,Attribute &attr);

	RC insertEntryForSystemCatalog(const string &tableName, const string &tableType, INT32 numCols);

	RC insertEntryForTableCatalog(FileHandle &fileHandle, const string &tableName, const string &columnName, INT32 columnType, INT32 columnPosition, INT32 maxSize, bool hasIndex);



	// Main Structure
	RC createTable(const string &tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string &tableName);

	RC getAttributes(const string &tableName, vector<Attribute> &attrs);

	RC insertTuple(const string &tableName, const void *data, RID &rid);

	RC deleteTuples(const string &tableName);

	RC deleteTuple(const string &tableName, const RID &rid);

	// Assume the rid does not change after update
	RC handleUpdateIndex(const string &tableName,vector<Attribute> recordDescriptor, //
			const void *oldData,const void* newData,const RID rid);

	RC updateTuple(const string &tableName, const void *data, const RID &rid);

	RC readTuple(const string &tableName, const RID &rid, void *data);

	RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

	RC reorganizePage(const string &tableName, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(const string &tableName,
			const string &conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);
	RC createIndex(const string &tableName, const string &attributeName);

	RC destroyIndex(const string &tableName, const string &attributeName);

	// indexScan returns an iterator to allow the caller to go through qualified entries in index
	RC indexScan(const string &tableName,
			const string &attributeName,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive,
			RM_IndexScanIterator &rm_IndexScanIterator);


	// Extra credit
public:
	RC dropAttribute(const string &tableName, const string &attributeName);

	RC addAttribute(const string &tableName, const Attribute &attr);

	RC reorganizeTable(const string &tableName);

protected:
	RelationManager();
	~RelationManager();

private:
	static RelationManager *_rm;
};

#endif

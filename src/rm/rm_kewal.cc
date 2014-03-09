#include "rm.h"

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
	dbgnRMFn();
	// Create Record Descriptor
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In index scan (RM)");
		return -1;
	}

	// Check if Index for file has been created
	Attribute attr;
	if(getAttributeObj(attributeName, recordDescriptor, attr) == -1){
		dbgnRM("Index for file is not present","In index scan (RM)");
		return -1;
	}

	// Decide Index File Name which has to exist !
	int len=strlen(tableName.c_str())+4+strlen(attributeName.c_str());
	char* indexName = (char *) malloc(len+1);
	getIndexName(tableName, attributeName, indexName);
	remove(indexName);



	free(indexName);
	dbgnRMFnc();
	return 0;
}

RC RelationManager::indexScan(const string &tableName,
		const string &attributeName,
		const void *lowKey,
		const void *highKey,
		bool lowKeyInclusive,
		bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator)
{
	dbgnRMFn();
	// Create Record Descriptor
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In index scan (RM)");
		return -1;
	}

	// Check if Index for file has been created
	Attribute attr;
	if(getAttributeObj(attributeName, recordDescriptor, attr) == -1){
		dbgnRM("Index for file is not present","In index scan (RM)");
		return -1;
	}

	// Decide Index File Name
	int len=strlen(tableName.c_str())+4+strlen(attributeName.c_str());
	char* indexName= (char*) malloc(len+1);
	getIndexName(tableName, attributeName, indexName);

	// Create a fileHandle for the index file
	FileHandle indexHandle;

	// Fill value for index file name
	if(im->openFile(indexName,indexHandle)==-1){
		dbgnRM("could not associate index handle","In index scan (RM)");
		return -1;
	}

	if(im->scan(indexHandle,attr,lowKey,highKey,lowKeyInclusive,highKeyInclusive,*(rm_IndexScanIterator.ix_scaniterator))==-1){
		dbgnRM("could not start scan","In index scan (RM)");
		return -1;
	}

	if(im->closeFile(indexHandle)==-1){
		dbgnRM("could not close index handle","In index scan (RM)");
		return -1;
	}

	free(indexName);
	dbgnRMFnc();
	return 0;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	im = IndexManager::instance();

	systemCatalog = "System_Catalog";
	tableCatalogConcat = new char[5];
	*(tableCatalogConcat) = 'c';
	*(tableCatalogConcat+1) = 'a';
	*(tableCatalogConcat+2) =  't';
	*(tableCatalogConcat+3) = '_';
	*(tableCatalogConcat+4) = 0;

	if(FileExists(systemCatalog))
		rbfm->openFile(systemCatalog.c_str(),systemHandle);

	Attribute attr;
	attr.name = "tableName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)30;
	systemDescriptor.push_back(attr);

	attr.name = "tableType";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)10;
	systemDescriptor.push_back(attr);

	attr.name = "numCols";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	systemDescriptor.push_back(attr);

	attr.name = "tableName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)30;
	tableDescriptor.push_back(attr);

	attr.name = "columnName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)30;
	tableDescriptor.push_back(attr);

	attr.name = "columnType";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	tableDescriptor.push_back(attr);

	attr.name = "columnPosition";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	tableDescriptor.push_back(attr);

	attr.name = "maxSize";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	tableDescriptor.push_back(attr);
}


RelationManager::~RelationManager()
{

	rbfm->closeFile(systemHandle);
	// ********* ???
	free(im);
	free(rbfm);
	free(tableCatalogConcat);
}



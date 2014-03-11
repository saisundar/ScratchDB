#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
	if(!_rm)
		_rm = new RelationManager();

	return _rm;
}

//update attribute datatye first to have one boolean hasIndex.
RC RelationManager::getAttributeObj(const string &attributeName,vector<Attribute> recordDescriptor,Attribute &attr)
{
	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	INT32 count=0;
	dbgnRMFn();
	dbgnRBFM("num of attributes",recordDescriptor.size());
	while(it != recordDescriptor.end() )
	{

		if((it->name).compare(attributeName)==0)
		{

			attr.hasIndex=it->hasIndex;
			attr.name=it->name;
			attr.type=it->type;
			attr.hasIndex=it->hasIndex;
			break;
		}
		++it;
		count++;
	}

	dbgnRMFnc();
	if(count==recordDescriptor.size())
		return -1;
	return count;
}

RC RelationManager::getIndexName(const string &tableName, const string &attributeName,char* indexName, INT32 len)
{
	dbgnRMFn();

	strcpy(indexName,tableName.c_str());
	strcat(indexName,"_idx_");
	strcat(indexName,attributeName.c_str());

	indexName[len+1]=0;

	return 0;
	dbgnRMFnc();
}

RC RelationManager::updateTableCatalogIndex(const string &tableName,INT32 hasIndexValue,const string &attributeName)
{
	char * tableCatalogName =(char *)malloc(strlen(tableName.c_str())+5);
	tableCatalogName=strcpy(tableCatalogName,tableCatalogConcat);
	tableCatalogName=strcat(tableCatalogName,tableName.c_str());
	FileHandle tableCatalogHandle;
	dbgnRMFn();
	if(rbfm->openFile(tableCatalogName, tableCatalogHandle)==-1)
	{
		dbgnRM("Failed to open",tableCatalogName);
		return -1;
	}

	// Create attributes necessary for scan iterator
	string conditionAttr ="columnName";

	vector<string> projectedAttributes;
	RID dummyRid;

	//	Attribute attr;
	void* data = malloc(100);
	void* attributeNameForm=malloc(100);
	projectedAttributes.push_back("tableName");
	projectedAttributes.push_back("columnName");
	projectedAttributes.push_back("columnType");
	projectedAttributes.push_back("columnPosition");
	projectedAttributes.push_back("maxSize");
	projectedAttributes.push_back("hasIndex");

	// Create Projected attributes to retrieve relevant attributes for catalog table

	// Scan through the table catalog file to create the record descriptor

	RBFM_ScanIterator rbfmsi;
	INT32 len=strlen(attributeName.c_str());
	memcpy(attributeNameForm,&len,4);
	memcpy((BYTE *)attributeNameForm+4,attributeName.c_str(),len);
	dbgnRM("the scan being done to find this attribute",attributeName);

	rbfm->scan(tableCatalogHandle, tableDescriptor, conditionAttr, EQ_OP,attributeNameForm, projectedAttributes, rbfmsi);
	dbgAssert(rbfmsi.getNextRecord(dummyRid, data)!=RBFM_EOF);
	free(attributeNameForm);
	dbgnRM("the row for the attribute has been found","");
	dbgnRM("now updating hasIndex attribute to true","");


	BYTE * iterData=(BYTE *)data;
	std::vector<Attribute>::const_iterator it = tableDescriptor.begin();
	INT32 num = 0;
	for(;it != tableDescriptor.end();it++)
	{
		dbgnRBFMU("type",it->type);
		switch(it->type){
		case 0:
			if((it->name).compare("hasIndex")==0)
			{
				memcpy(iterData,&hasIndexValue,4);
			}

			iterData=iterData+4;
			break;

		case 1:
			iterData=iterData+4;
			break;

		case 2:
			num = *((INT32 *)iterData);
			iterData=iterData+4+num;
			break;

		default:
			break;

		}
	}
	dbgnRM("breaking as the record has been updated.","now need to insert it");

	if(rbfm->updateRecord(tableCatalogHandle,tableDescriptor,data,dummyRid)==-1)
	{
		dbgnRM("oops unable to insert the new attrbute info into the table catalog","");
		free(tableCatalogName);
		free(data);
		return -1;
	}

	if(rbfm->closeFile(tableCatalogHandle)==-1){
		dbgnRM("could close the Table catalog file","");
		free(tableCatalogName);
		free(data);
		return -1;
	}
	free(tableCatalogName);
	free(data);
	dbgnRMFnc();
	return 0;
}
RC RelationManager::updateMemDescriptor(const string &tableName,Attribute attr,INT32 loc)
{
	vector<Attribute> attrs;
	dbgnRMFn();
	if(descriptors.find(tableName)==descriptors.end()){
		dbgnRM("no descriptor in memory===error","cannot be true");
		attrs = (vector<Attribute>)descriptors[tableName];
		return 0;
	}

	attrs=descriptors[tableName];
	attrs[loc]=attr;
	descriptors[tableName]=attrs;
	dbgnRM("updated the in memory record descriptor","");
	dbgnRMFnc();
	return 0;

}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	//    1) check if table exists, if not return error.if index already created on attribute dont allow it
	//	  2) i will need to update the hasIndex attribute in cat_relation.
	//	  3) i will need to create a new file called relation_index_attributename
	//	  4) once file is made, i need to iterate over all existing records in the file and load them in the index.0
	dbgnRMFn();
	if(!FileExists(systemCatalog))
	{
		dbgnRM("no system catalog","");
		return -1;
	}

	vector<Attribute> recordDescriptor;
	Attribute attr;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","");
		return -1;
	}
	INT32 loc=getAttributeObj(attributeName,recordDescriptor,attr);
	if(loc==-1)
	{
		dbgnRM("no such attribute","");
		return -1;
	}

	if(attr.hasIndex==true)
	{
		dbgnRM("Index already exists for the attribute","");
		return -1;
	}
	int len=strlen(tableName.c_str())+4+strlen(attributeName.c_str());
	char* idxName = (char *)malloc(len+1);
	getIndexName(tableName,attributeName,idxName,len);

	// Create new file for the index
	if(im->createFile(idxName)==-1){
		dbgnRM("Create Index file  Failed","(in createTable)");
		return -1;
	}

	//update the hasIndex attribute in the catalog
	updateTableCatalogIndex(tableName,1,attributeName);
	attr.hasIndex=true;
	updateMemDescriptor(tableName,attr,loc);

	RM_ScanIterator rmsi;
	RID rid;
	vector<string> attributes;
	FileHandle indexHandle;
	RC rc = im->openFile(idxName, indexHandle);
	if(rc != 0)
	{
		dbgnRM("open Index file  Failed","(in createTable)");
		free(idxName);
		return -1;
	}

	void *returnedData = malloc(attr.length);
	attributes.push_back(attributeName);
	rc = scan(tableName,"", NO_OP, NULL, attributes, rmsi);
	if(rc != 0) {
		dbgnRM("Create Index file  Failed","(in createTable)");
		free(idxName);
		return -1;
	}

	while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		rc=im->insertEntry(indexHandle,attr,returnedData,rid);
		if(rc != 0) {
			dbgnRM("inserting into the  Index file  Failed","(in createTable)");
			free(idxName);
			free(returnedData);
			return -1;
		}
	}
	rmsi.close();

	free(returnedData);
	free(idxName);
	dbgnRMFnc();
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
	dbgnRMFn();
	// Create Record Descriptor
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In destroy index (RM)");
		return -1;
	}

	// Check if Attribute name is contained in record descriptor
	Attribute attr;
	int loc;
	if((loc = getAttributeObj(attributeName, recordDescriptor, attr)) == -1){
		dbgnRM("Index for file is not present","In destroy index (RM)");
		return -1;
	}

	// Check if Index for file has been created
	if(!attr.hasIndex){
		dbgnRM("Index for this attribute is not present","In destroy index (RM)");
		return -1;
	}

	// Decide Index File Name which has to exist !
	int len=strlen(tableName.c_str())+4+strlen(attributeName.c_str());
	char* indexName = (char *) malloc(len+1);
	getIndexName(tableName, attributeName, indexName,len);

	// Actual delete happens here
	remove(indexName);

	// Update catalog for that relation
	updateTableCatalogIndex(tableName,0,attributeName);

	// Update record descriptor if it is in memory
	attr.hasIndex = false;
	updateMemDescriptor(tableName,attr,loc);

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

	// Check if attribute name is present
	Attribute attr;
	if(getAttributeObj(attributeName, recordDescriptor, attr) == -1){
		dbgnRM("Attribute name is not valid","In index scan (RM)");
		return -1;
	}

	// Check if Index for file has been created
	if(attr.hasIndex==0){
		dbgnRM("Index for this attribute is not present","In index scan (RM)");
		return -1;
	}

	// Decide Index File Name
	int len=strlen(tableName.c_str())+4+strlen(attributeName.c_str());
	char* indexName= (char*) malloc(len+1);
	getIndexName(tableName, attributeName, indexName,len);

	// Create a fileHandle for the index file
	FileHandle indexHandle;

	// FileHandle association for index file name
	if(im->openFile(indexName,indexHandle)==-1){
		dbgnRM("could not associate index handle","In index scan (RM)");
		return -1;
	}

	// Scan starts here
	if(im->scan(indexHandle,attr,lowKey,highKey,lowKeyInclusive,highKeyInclusive,*(rm_IndexScanIterator.ix_scaniterator))==-1){
		dbgnRM("could not start scan","In index scan (RM)");
		return -1;
	}

	// Close fileHandle
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

	dbgnRMFn();
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

	attr.name = "hasIndex";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	tableDescriptor.push_back(attr);
	dbgnRMFnc();
}

RelationManager::~RelationManager()
{

	dbgnRMFn();
	rbfm->closeFile(systemHandle);
	// ********* ???
	free(im);
	free(rbfm);
	free(tableCatalogConcat);
	dbgnRMFnc();
}

RC RelationManager::insertEntryForTableCatalog(FileHandle &tableCatalogHandle, const string &tableName, const string &columnName, \
		INT32 columnType,INT32 columnPosition, INT32 maxSize,bool hasIndex)
{
	dbgnRMFn();
	INT32 l1 = strlen(tableName.c_str());
	INT32 l2 = strlen(columnName.c_str());
	INT32 dataLength = 24 + l1 + l2;
	void* data = malloc(dataLength);
	BYTE* tableData = (BYTE*)data;
	//Copy length of tableName
	INT32 temp = l1;
	memcpy(tableData, &temp, 4);
	tableData += 4;

	//copy tableName
	memcpy(tableData,tableName.c_str(),l1);
	tableData+=l1;

	//copy length of columnName
	temp = l2;
	memcpy(tableData, &temp, 4);
	tableData+=4;

	//copy columnName
	memcpy(tableData,columnName.c_str(),l2);
	tableData+=l2;

	//copy columType
	temp = columnType;
	memcpy(tableData, &temp, 4);
	tableData+=4;

	//copy columnPosition
	temp = columnPosition;
	memcpy(tableData, &temp, 4);
	tableData+=4;

	//copy maxSize
	temp = maxSize;
	memcpy(tableData, &temp, 4);
	tableData+=4;

	temp=(hasIndex)?1:0;
	memcpy(tableData, &temp, 4);
	tableData+=4;

	// Insert Record;
	RID systemRid;
	dbgnRM("Record Being Inserted in Table Catalog: ", rbfm->printRecord(tableDescriptor, data));

	rbfm->insertRecord(tableCatalogHandle,tableDescriptor,data,systemRid);

	free(data);
	dbgnRMFnc();
	return 0;
}

RC RelationManager::insertEntryForSystemCatalog(const string &tableName, const string &tableType, INT32 numCols){
	dbgnRMFn();
	INT32 l1 = strlen(tableName.c_str());
	dbgnRM("l1",l1);
	INT32 l2 = strlen(tableType.c_str());
	dbgnRM("l2",l2);
	INT32 dataLength = 12 + l1 + l2;
	void* tempData = malloc(dataLength);
	BYTE* systemData = (BYTE*)tempData;
	INT32 temp = l1;
	memcpy(systemData, &temp, 4);

	systemData += 4;
	memcpy(systemData,tableName.c_str(),l1);

	systemData+=l1;
	temp = l2;
	memcpy(systemData, &temp, 4);

	systemData+=4;
	memcpy(systemData,tableType.c_str(),l2);

	systemData+=l2;
	temp = 3;
	memcpy(systemData, &temp, 4);

	// Insert Record;
	RID systemRid;
	dbgnRM("Record Being Inserted in System Catalog: ", rbfm->printRecord(systemDescriptor, tempData));
	rbfm->insertRecord(systemHandle,systemDescriptor,tempData,systemRid);

	free(tempData);
	dbgnRMFnc();
	return 0;
}

/*RC RelationManager::createRecordDescriptor(FileHandle &fileHandle, const string &tableName, const vector<Attribute> &recordDescriptor){
	dbgnRM("<----------------------------In Create Record Descriptor------------------------->","");
	if(descriptors.find(tableName)!=descriptors.end()){
		dbgnRM("Descriptor is already Created",", returning stored value");
		recordDescriptor = (FileHandle)descriptors[tableName];
		return 0;
	}
	dbgnRM("Descriptor is being Created","");
	// ELse create Record Descriptor for that table
// ********************************************* Left to Implement ******************************
	// Insert Record Descriptor in Map
	descriptors.insert(std::pair< string ,vector<Attribute>>(tableName, recordDescriptor));
	return 0;
}*/

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	dbgnRMFn();
	if(!FileExists(systemCatalog)){
		// Create System Catalog if not created
		dbgnRM("System catalog does not exist",", Creating IT !");
		rbfm->createFile(systemCatalog.c_str());
		rbfm->openFile(systemCatalog.c_str(),systemHandle);
		insertEntryForSystemCatalog(systemCatalog, "System", 3);

	}

	// Insert Record for new table in System_Catalog;
	insertEntryForSystemCatalog(tableName.c_str(), "User", attrs.size());

	// Create new file for the table
	if(rbfm->createFile(tableName.c_str())==-1){
		dbgnRM("Create File Failed","(in createTable)");
		return -1;
	}

	// Create new file for the table catalog and associate fileHandle
	char * tableCatalogName =(char *)malloc(strlen(tableName.c_str())+5);
	tableCatalogName=strcpy(tableCatalogName,tableCatalogConcat);
	tableCatalogName=strcat(tableCatalogName,tableName.c_str());
	dbgnRM("table Catalog Name: ",tableCatalogName);
	dbgnRM("table Catalog concat: ",tableCatalogConcat);

	if(rbfm->createFile(tableCatalogName)==-1){
		dbgnRM("Create Table Catalog Failed","");
		return -1;
	}
	FileHandle tableCatalogHandle;
	if(rbfm->openFile(tableCatalogName,tableCatalogHandle)==-1){
		dbgnRM("Open Table Catalog Failed","");
		return -1;
	}

	// Insert Record for new Table Catalog in System_Catalog
	insertEntryForSystemCatalog(tableCatalogName, "System", 5);

	// Insert Records in Table Catalog
	for(INT32 i=0;i<attrs.size();i++){
		Attribute tableAttr = attrs[i];
		insertEntryForTableCatalog(tableCatalogHandle, tableName, tableAttr.name, tableAttr.type, i, tableAttr.length,false);
	}
	if(rbfm->closeFile(tableCatalogHandle)==-1){
		dbgnRM("Close Table Catalog Failed","");
		return -1;
	}
	free(tableCatalogName);
	dbgnRMFnc();
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	dbgnRMFn();
	RBFM_ScanIterator rbfmsi;

	// Create Record Descriptor to be used for storing index information which will be used to delete index files
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In delete tuppleS (RM)");
		return -1;
	}

	// Make Application Layer Entry for tableName to insert in "ConditionAttribute" field in scan function for searching it in SystemCatalog
	INT32 length = strlen(tableName.c_str());
	void * tempData = malloc(4+length);
	BYTE* data = (BYTE*)tempData;
	dbgnRM("length of table search string", length+4);

	memcpy(data,&length,4);
	data = data + 4;
	BYTE* copyPointer = (BYTE *)tableName.c_str();
	for(int i=0;i<length;i++){
		*(data+i) = *(copyPointer+i);
	}

	string conditionAttr = "tableName";
	vector<string> dummy;
	RID deleteRid;

	for(int i=0;i<length+4;i++){
		cout<<*((char*)tempData+i);
	}

	dbgnRM("Searching In System Catalog","");
	dbgnRM("Searching Query Created",string((char*)tempData));
	if(rbfm->scan(systemHandle, systemDescriptor, conditionAttr, EQ_OP, tempData, dummy, rbfmsi)==-1)return -1;
	if(rbfmsi.getNextRecord(deleteRid, tempData)==RBFM_EOF){
		dbgnRM("Record not found by scan iterator: ",tableName);
		return -1;
	}

	vector<Attribute> dummy2;
	if(rbfm->deleteRecord(systemHandle, dummy2, deleteRid)==-1){
		dbgnRM("Could not delete","record for table in system catalog");
		return -1;
	}
	rbfmsi.close();
	free(tempData);

	// Make Application Layer Entry for tableName's Catalog to insert in "ConditionAttribute" field in scan function for searching it in SystemCatalog
	char * tableCatalogName = (char *)malloc(strlen(tableName.c_str())+5);
	tableCatalogName=strcpy(tableCatalogName,tableCatalogConcat);
	tableCatalogName=strcat(tableCatalogName,tableName.c_str());

	dbgnRM("table Catalog Name: ",tableCatalogName);
	length = strlen(tableCatalogName);
	dbgnRM("length of table catalog search string: ", length+4);
	tempData = malloc(4+length);
	data = (BYTE*)tempData;
	memcpy(data,&length,4);
	data = data + 4;

	copyPointer = (BYTE *)tableCatalogName;
	for(int i=0;i<length;i++){
		*(data+i) = *(copyPointer+i);
	}

	dbgnRM("Searching In: System Catalog","");
	dbgnRM("Searching For: ", tableCatalogName);
	rbfm->scan(systemHandle, systemDescriptor, conditionAttr, EQ_OP, tempData, dummy, rbfmsi);


	if(rbfmsi.getNextRecord(deleteRid, tempData)==RBFM_EOF){
		dbgnRM("Record not found by scan iterator: ",tableName);
		return -1;
	}

	if(rbfm->deleteRecord(systemHandle, dummy2, deleteRid)==-1){
		dbgnRM("Could not delete ","record for table CATALOG in system catalog");
		return -1;
	}
	rbfmsi.close();

	if(rbfm->destroyFile(tableName)==-1){
		dbgnRM("Could not delete file for the table: ", tableName);
		return -1;
	}

	if(rbfm->destroyFile(tableCatalogName)==-1){
		dbgnRM("Could not delete catalog file for the table: ", tableCatalogName);
		return -1;
	}
	free(tableCatalogName);
	free(tempData);


	// Delete all index files associated with this table
	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	for(;it != recordDescriptor.end();it++){
		if(it->hasIndex==true){
			int len=strlen(tableName.c_str()) + 4 + strlen((it->name).c_str());
			char* indexFileName = (char *)malloc(len+1);
			getIndexName(tableName,(it->name),indexFileName,len);
			if(im->destroyFile(indexFileName)==-1){
				dbgnRM("could not destroy index file","In delete tuppleS (RM)");
				dbgnRM("for index attribute:",it->name);
				return -1;
			}
			free(indexFileName);
		}
	}
	dbgnRMFnc();
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	dbgnRMFn();
	if(descriptors.find(tableName)!=descriptors.end()){
		dbgnRM("Descriptor is already Created",", returning stored value");
		attrs = (vector<Attribute>)descriptors[tableName];
		return 0;
	}

	dbgnRM("Descriptor is being Created","");
	// ELse create Record Descriptor for that table
	// ********************************************* Left to Implement ******************************
	// Insert Record Descriptor in Map
	char * tableCatalogName =(char *)malloc(strlen(tableName.c_str())+5);
	tableCatalogName=strcpy(tableCatalogName,tableCatalogConcat);
	tableCatalogName=strcat(tableCatalogName,tableName.c_str());
	FileHandle tableCatalogHandle;
	if(rbfm->openFile(tableCatalogName, tableCatalogHandle)==-1)
	{
		dbgnRM("Failed to open",tableCatalogName);
		return -1;
	}

	// Create attributes necessary for scan iterator
	string conditionAttr ="columnPosition";
	INT32 columnPosition = 0,i=0;
	vector<string> projectedAttributes;
	RBFM_ScanIterator rbfmsi;
	RID dummyRid;
	//	Attribute attr;
	void* data = malloc(42);

	// Create Projected attributes to retrieve relevant attributes for catalog table
	projectedAttributes.push_back("columnName");
	projectedAttributes.push_back("columnType");
	projectedAttributes.push_back("maxSize");
	projectedAttributes.push_back("hasIndex");

	// Scan through the table catalog file to create the record descriptor
	while(true){
		RBFM_ScanIterator rbfmsi;
		rbfm->scan(tableCatalogHandle, tableDescriptor, conditionAttr, EQ_OP, (void*)&columnPosition, projectedAttributes, rbfmsi);
		if(rbfmsi.getNextRecord(dummyRid, data)==RBFM_EOF)break; // If no record found break, since we have scanned through all the records in the file

		// Make Attribute object
		Attribute attr;
		BYTE* iterData = (BYTE*)data;
		INT32 nameLength = *((INT32*)iterData);
		iterData += 4;
		attr.name.resize(nameLength,0);
		dbgnRM("attr namebefore copy",attr.name);
		//memcpy(attr.name,iterData,nameLength);

		for(i=0;i<nameLength;i++,iterData++)attr.name[i]=*(char *)iterData;

		attr.type = *((AttrType*)iterData);
		iterData  +=sizeof(INT32);
		attr.length = *((AttrLength*)iterData);
		iterData  +=sizeof(INT32);
		INT32 temp;
		memcpy(&temp,iterData,4);
		attr.hasIndex=(temp==1)?true:false;
		dbgnRM("attr name",attr.name);
		dbgnRM("attr length",attr.length);
		dbgnRM("attr type",attr.type);
		dbgnRM("attr hasIndex",attr.hasIndex);
		//Add attribute to record descriptor vector
		attrs.push_back(attr);
		columnPosition++;
		rbfmsi.close();
	}
	dbgnRM("Length of recordDescriptor: ",columnPosition);

	descriptors.insert(std::pair< string ,vector<Attribute> >(tableName, attrs));
	if(rbfm->closeFile(tableCatalogHandle)==-1){
		dbgnRM("could close the Table catalog file","");
		return -1;
	}

	free(tableCatalogName);
	free(data);
	dbgnRMFnc();
	return 0;
}

RC RelationManager::updateIndexIfRequired(const string &tableName,vector<Attribute> recordDescriptor, //
		const void *data, RID rid,bool isInsert)
{

	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	dbgnRMFn();
	BYTE* iterData = (BYTE *) data;
	void* temp4=malloc(4),*temp;
	char* idxName;
	INT32 len;
	INT32 num;
	FileHandle tempHandle;
	RC rc;
	for(;it != recordDescriptor.end();it++)
	{
		dbgnRBFMU("type",it->type);
		switch(it->type){
		case 0:
		case 1:
			if(it->hasIndex)
			{
				dbgnRM("Index modification happening for 4byte attribute",it->name);
				memcpy(temp4,iterData,4);
				len=strlen(tableName.c_str())+4+strlen(it->name.c_str());
				idxName=(char *)malloc(len+1);
				getIndexName(tableName,it->name,idxName,len);
				rc=im->openFile(idxName,tempHandle);
				if(rc==0)
				{
					if(isInsert)
						im->insertEntry(tempHandle,*it,temp4,rid);
					else
						im->deleteEntry(tempHandle,*it,temp4,rid);
				}
				im->closeFile(tempHandle);
				free(idxName);
			}
			iterData=iterData+4;
			break;
		case 2:
			num = *((INT32 *)iterData);
			if(it->hasIndex)
			{
				temp=malloc(num+4);
				dbgnRM("Index modification happening for string  attribute",it->name);
				memcpy(temp,iterData,4+num);
				len=strlen(tableName.c_str())+4+strlen(it->name.c_str());
				idxName=(char *)malloc(len+1);
				getIndexName(tableName,it->name,idxName,len);
				rc=im->openFile(idxName,tempHandle);
				if(rc==0)
				{
					if(isInsert)
						im->insertEntry(tempHandle,*it,temp,rid);
					else
						im->deleteEntry(tempHandle,*it,temp,rid);
				}
				im->closeFile(tempHandle);
				free(idxName);
				free(temp);
			}
			iterData=iterData+4+num;
			break;

		default:
			break;

		}
	}
	free(temp4);
	dbgnRMFnc();
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	dbgnRMFn();
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","");
		return -1;
	}
	FileHandle tableHandle;
	if(rbfm->openFile(tableName.c_str(),tableHandle)==-1){
		dbgnRM("could not create Record descriptor","");
		return -1;
	}
	if(rbfm->insertRecord(tableHandle,recordDescriptor,data,rid)==-1){
		dbgnRM("could not insert the new record","");
		if(rbfm->closeFile(tableHandle)==-1){
			dbgnRM("could close the file","");
			return -1;
		}
		return -1;
	}

	updateIndexIfRequired(tableName,recordDescriptor,data,rid,true);

	if(rbfm->closeFile(tableHandle)==-1){
		dbgnRM("could close the file","");
		return -1;
	}
	dbgnRMFnc();
	return 0;
}

#include "rm.h"



RC RelationManager::deleteTuples(const string &tableName)
{
	dbgnRMFn();

	// Create Record Descriptor to be used for storing index information
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In delete tuppleS (RM)");
		return -1;
	}

	FileHandle tableHandle;
	if(rbfm->openFile(tableName.c_str(),tableHandle)==-1){
		dbgnRM("could not open the file","In delete tuppleS (RM)");
		return -1;
	}
	if(rbfm->deleteRecords(tableHandle)==-1)
	{
		if(rbfm->closeFile(tableHandle)==-1){
			dbgnRM("could not close the file","In delete tuppleS (RM) case 1");
			return -1;
		}
		return -1;
	}
	if(rbfm->closeFile(tableHandle)==-1){
		dbgnRM("could not close the file","In delete tuppleS (RM)");
		return -1;
	}

	// Deleting index files here
	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
	for(;it != recordDescriptor.end();it++){
		if(it->hasIndex==true){
			int len=strlen(tableName.c_str()) + 4 + strlen((it->name).c_str());
			char* indexFileName = (char *)malloc(len+1);
			getIndexName(tableName,(it->name),indexFileName,len);
			if(im->destroyFile(indexFileName)==-1){
				dbgnRM("could not destroy index file","In delete tuppleS (RM)");
				dbgnRM("for index attribute:",it->name);
				return -1;
			}
			if(im->createFile(indexFileName)==-1){
				dbgnRM("could not recreate index file","In delete tuppleS (RM)");
				dbgnRM("for index attribute:",it->name);
				return -1;
			}
			free(indexFileName);
		}
	}
	dbgnRMFnc();
	return 0;
}


RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	dbgnRMFn();
	FileHandle tableHandle;
	vector<Attribute> dummy;
	void* data=malloc(2000);
	readTuple(tableName,rid,data);
	if(rbfm->openFile(tableName.c_str(),tableHandle)==-1)
	{
		dbgnRM("open file failed","ooops");
		dbgnRMFnc();
		free(data);
		return -1;
	}

	if(rbfm->deleteRecord(tableHandle,dummy,rid)==-1)
	{
		dbgnRM("delete record file failed","ooops");
		if(rbfm->closeFile(tableHandle)==-1){
			dbgnRM("could not close the file","");
			dbgnRMFnc();
			free(data);
			return -1;
		}
		dbgnRMFnc();
		free(data);
		return -1;
	}
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","");
		dbgnRMFnc();
		free(data);
		return -1;
	}
	updateIndexIfRequired(tableName,recordDescriptor,data,rid,false);
	free(data);

	if(rbfm->closeFile(tableHandle)==-1)return -1;
	dbgnRMFnc();
	return 0;

}

RC RelationManager::handleUpdateIndex(const string &tableName,vector<Attribute> recordDescriptor, //
		const void *oldData,const void* newData,const RID rid)
{
	std::vector<Attribute>::const_iterator it = recordDescriptor.begin();
		dbgnRMFn();
		BYTE* iterDataO = (BYTE *)oldData;
		BYTE* iterDataN = (BYTE *)newData;
		void* temp4=malloc(4),*temp;
		char* idxName;
		INT32 len;
		INT32 numO,numN;
		FileHandle tempHandle;
		RC rc;
		for(;it != recordDescriptor.end();it++)
		{
			dbgnRBFMU("type",it->type);
			switch(it->type){
			case 0:
			case 1:
				if(it->hasIndex)
				{
					if(memcmp(iterDataO,iterDataN,4)!=0)
					{
						len=strlen(tableName.c_str())+4+strlen(it->name.c_str());
						idxName=(char *)malloc(len+1);
						getIndexName(tableName,it->name,idxName,len);
						rc=im->openFile(idxName,tempHandle);

						dbgnRM("Index modification happening for 4byte attribute",it->name);
						memcpy(temp4,iterDataO,4);

						if(rc==0)
						{
							im->deleteEntry(tempHandle,*it,temp4,rid);
							memcpy(temp4,iterDataN,4);
							im->insertEntry(tempHandle,*it,temp4,rid);
						}
						im->closeFile(tempHandle);
						free(idxName);
					}
				}
				iterDataO=iterDataO+4;
				iterDataN=iterDataN+4;
				break;
			case 2:
				numO= *((INT32 *)iterDataO);
				numN= *((INT32 *)iterDataN);
				if(it->hasIndex)
				{

					if(numO!=numN||memcmp(iterDataO+4,iterDataN+4,numO)!=0)
					{
						len=strlen(tableName.c_str())+4+strlen(it->name.c_str());
						idxName=(char *)malloc(len+1);
						getIndexName(tableName,it->name,idxName,len);

						temp=malloc(numO+4);
						dbgnRM("Index modification happening for string  attribute",it->name);
						memcpy(temp,iterDataO,4+numO);

						rc=im->openFile(idxName,tempHandle);
						if(rc==0)
						{
							im->deleteEntry(tempHandle,*it,temp,rid);
							free(temp);
							temp=malloc(numN+4);
							memcpy(temp,iterDataN,4+numN);
							im->insertEntry(tempHandle,*it,temp4,rid);
						}
						im->closeFile(tempHandle);
						free(idxName);
						free(temp);
					}
				}
				iterDataO=iterDataO+4+numO;
				iterDataN=iterDataN+4+numN;
				break;

			default:
				break;

			}
		}
		free(temp4);
		dbgnRMFnc();
		return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	dbgnRMFn();
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","");
		dbgnRMFnc();
		return -1;
	}
	void* oldData=malloc(2000);
	if(readTuple(tableName,rid,oldData)==-1)
	{
		dbgnRM("record is not present... so no reading..","");
		dbgnRMFnc();
		free(oldData);
		return -1;
	}

	FileHandle tableHandle;
	if(rbfm->openFile(tableName.c_str(),tableHandle)==-1){
		dbgnRM("could not create Record descriptor","");
		dbgnRMFnc();
		free(oldData);
		return -1;
	}
	if(rbfm->updateRecord(tableHandle,recordDescriptor,data,rid)==-1){
		dbgnRM("could not update the new record","");
		if(rbfm->closeFile(tableHandle)==-1){
			dbgnRM("could NOT close the file","");
			dbgnRMFnc();
			free(oldData);
			return -1;
		}
		free(oldData);
		dbgnRMFnc();
		return -1;
	}

	handleUpdateIndex(tableName,recordDescriptor, oldData, data, rid)  ;

	dbgnRM("In Process Of closing Filehandle in","RM UPDATE")
	if(rbfm->closeFile(tableHandle)==-1){
		dbgnRM("could NOT close the file","");
		dbgnRMFnc();
		return -1;
	}
	dbgnRMFnc();
	free(oldData);
	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	dbgnRMFn();
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In Read Tuple (RM)");
		dbgnRMFnc();
		return -1;
	}

	FileHandle tableHandle;
	if(rbfm->openFile(tableName.c_str(),tableHandle)==-1){
		dbgnRM("could not create Record descriptor","In Read Tuple (RM)");
		dbgnRMFnc();
		return -1;
	}
	if(rbfm->readRecord(tableHandle, recordDescriptor, rid, data)==-1){
		dbgnRM("could not read the new record","In Read Tuple (RM)");
		if(rbfm->closeFile(tableHandle)==-1){
			dbgnRM("could close the file","");dbgnRMFnc();
			return -1;
		}
		dbgnRMFnc();
		return -1;
	}
	if(rbfm->closeFile(tableHandle)==-1){
		dbgnRM("could close the file","In Read Tuple (RM)");
		dbgnRMFnc();
		return -1;
	}
	dbgnRMFnc();
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	dbgnRMFn();
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In Read Attribute (RM))");
		dbgnRMFnc();
		return -1;
	}

	FileHandle tableHandle;
	if(rbfm->openFile(tableName.c_str(),tableHandle)==-1){
		dbgnRM("could not create Record descriptor","IIn Read Attribute (RM)");
		dbgnRMFnc();
		return -1;
	}
	if(rbfm->readAttribute(tableHandle, recordDescriptor, rid, attributeName, data)==-1){
		dbgnRM("could not read the attribute","In Read Attribute (RM)");
		if(rbfm->closeFile(tableHandle)==-1){
			dbgnRM("could close the file","");
			dbgnRMFnc();
			return -1;
		}
		dbgnRMFnc();
		return -1;
	}
	if(rbfm->closeFile(tableHandle)==-1){
		dbgnRM("could close the file","In Read Attribute (RM)");
		dbgnRMFnc();
		return -1;
	}
	dbgnRMFnc();
	return 0;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	vector<Attribute> dummyDescriptor;
	FileHandle tableHandle;
	dbgnRMFn();
	if(rbfm->openFile(tableName.c_str(),tableHandle)==-1){
		dbgnRM("could not create Record descriptor","IIn Read Attribute (RM)");
		dbgnRMFnc();
		return -1;
	}
	if(rbfm->reorganizePage(tableHandle, dummyDescriptor, pageNumber)==-1){
		dbgnRM("could not insert the new record","In Read Attribute (RM)");
		if(rbfm->closeFile(tableHandle)==-1){
			dbgnRM("could close the file","");
			dbgnRMFnc();
			return -1;
		}
		dbgnRMFnc();
		return -1;
	}
	if(rbfm->closeFile(tableHandle)==-1){
		dbgnRM("could close the file","In Read Attribute (RM)");
		dbgnRMFnc();
		return -1;
	}
	dbgnRMFnc();
	return 0;
}

RC RelationManager :: scan(const string &tableName, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator)
{
	dbgnRMFn();
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In Read Attribute (RM))");
		dbgnRMFnc();
		return -1;
	}

	if(rbfm->openFile(tableName.c_str(),rm_ScanIterator.fileHandle)==-1){
		dbgnRM("could not create Record descriptor","IIn Read Attribute (RM)");
		dbgnRMFnc();
		return -1;
	}

	if(rbfm->scan(rm_ScanIterator.fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,*(rm_ScanIterator.rbfmsi)) == -1){
		dbgnRM("could not create rbfm scan iterator","In Read Attribute (RM)");
		if(rbfm->closeFile(rm_ScanIterator.fileHandle)==-1){
			dbgnRM("could close the file","");
			dbgnRMFnc();
			return -1;
		}
		dbgnRMFnc();
		return -1;
	}
	dbgnRMFnc();
	return 0;
}



// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
	return -1;
}

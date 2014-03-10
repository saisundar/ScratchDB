#include "rm.h"



RC RelationManager::deleteTuples(const string &tableName)
{
	dbgnRMFn();
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

	// Create Record Descriptor
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)==-1){
		dbgnRM("could not create Record descriptor","In delete tuppleS (RM)");
		return -1;
	}

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

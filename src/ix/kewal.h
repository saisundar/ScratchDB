/*
 * kewal.h
 *
 *  Created on: Feb 28, 2014
 *      Author: Kewal
 */

#ifndef KEWAL_H_
#define KEWAL_H_





#endif /* KEWAL_H_ */

#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define getRoot(handle,root) {           			\
		if(handle.stream==0)		\
		root=-1;				\
		else						\
		{							\
			fseek(handle.stream,PAGE_SIZE,SEEK_SET);				\
			fread(&root, 1, 4, handle.stream);	  	  	  	  	  	\
		}							\
}									\

#define pageType(data) (*((BYTE*)data))

# ifdef debugIX
# define dbgnIX(str1,str2) cout<<"\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnIX(str1,str2) (void)0;
#endif

# ifdef debugIXU
# define dbgnIXU(str1,str2) cout<<"\t\t\t\t\t\t"<<(str1)<<":\t\t\t"<< (str2)<<"\t\t\t\t\t\t"<<__func__<<":"<<__LINE__<<endl;
# else
# define dbgnIXU(str1,str2) (void)0;
#endif

class IX_ScanIterator;

class IndexManager {
 public:
  static IndexManager* instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
  RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  RC scan(FileHandle &fileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  //Utility FUnctions;
   RC insertIndexNode(INT32& pageNum, FileHandle fileHandle);
   RC insertLeafNode(INT32& pageNum, FileHandle fileHandle);
 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
};

class IX_ScanIterator {

 FileHandle fileHandle;
 RID nextRid;
 INT16 totalSlotsInCurrPage;
 void* leafPage;
 bool highKeyInclusive;
 void *highKey;
 AttrType type;

 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif

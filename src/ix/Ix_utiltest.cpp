#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <algorithm>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;
int g_nGradPoint;
int g_nGradExtraPoint;
int g_nUndergradPoint;
int g_nUndergradExtraPoint;

int testCase_1(const string &indexFileName)
{
    // Functions tested
    // 1. Create Index File **
    // 2. Open Index File **
    // 3. Create Index File -- when index file is already created **
    // 4. Close Index File **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 1****" << endl;

    RC rc;
    FileHandle fileHandle;

    // create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_return;
    }

    // create duplicate index file
    rc = indexManager->createFile(indexFileName);
    if(rc != success)
    {
        cout << "Duplicate Index File not Created -- correct!" << endl;
    }
    else
    {
        cout << "Duplicate Index File Created -- failure..." << endl;
        goto error_return;
    }

    // close index file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_return;
    }
    g_nGradPoint += 2;
    g_nUndergradPoint += 5;
    return success;

error_return:
	return fail;
}

int testCase_2(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Open Index file
    // 2. Insert entry **
    // 3. Delete entry **
    // 4. Delete entry -- when the value is not there **
    // 5. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 2****" << endl;

    RID rid;
    RC rc;
    unsigned numOfTuples = 1;
    unsigned key = 100;
    rid.pageNum = key;
    rid.slotNum = key+1;
    int age = 18;

    // open index file
    FileHandle fileHandle;
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_return;
    }

    // insert entry
    for(unsigned i = 0; i < numOfTuples; i++)
    {
        rc = indexManager->insertEntry(fileHandle, attribute, &age, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
            goto error_close_index;
        }
    }

    // delete entry
    rc = indexManager->deleteEntry(fileHandle, attribute, &age, rid);
    if(rc != success)
    {
        cout << "Failed Deleting Entry..." << endl;
        goto error_close_index;
    }

    // delete entry again
    rc = indexManager->deleteEntry(fileHandle, attribute, &age, rid);
    if(rc == success) //This time it should NOT give success because entry is not there.
    {
        cout << "Entry deleted again...failure" << endl;
        goto error_close_index;
    }

    // close index file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 3;
    g_nUndergradPoint += 5;
    return success;

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_return:
	return fail;
}


int testCase_3(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Destroy Index File **
    // 2. Open Index File -- should fail
    // 3. Scan  -- should fail
    cout << endl << "****In Test Case 3****" << endl;

    RC rc;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;

    // destroy index file
    rc = indexManager->destroyFile(indexFileName);
    if(rc != success)
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    // open the destroyed index
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success) //should not work now
    {
        cout << "Index opened again...failure" << endl;
        indexManager->closeFile(fileHandle);
        goto error_return;
    }

    // open scan
    rc = indexManager->scan(fileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan opened again...failure" << endl;
        ix_ScanIterator.close();
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;
error_return:
	return fail;
}

int testCase_4A(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries NO_OP -- open**
    // 5. Scan close **
    // 6. Close Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 4A****" << endl;

    RID rid;
    RC rc;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;
    unsigned numOfTuples = 1000;

    // create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_return;
    }

    // insert entry
    for(unsigned i = 0; i <= numOfTuples; i++)
    {
        key = i+1;//just in case somebody starts pageNum and recordId from 1
        rid.pageNum = key;
        rid.slotNum = key+1;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
            goto error_close_index;
        }
        inRidPageNumSum += rid.pageNum;
    }

    // Scan
    rc = indexManager->scan(fileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan!" << endl;
        goto error_close_index;
    }

    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        outRidPageNumSum += rid.pageNum;
    }

    if (inRidPageNumSum != outRidPageNumSum)
    {
    	cout << "Wrong entries output...failure" << endl;
    	goto error_close_scan;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // Close Index
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
    }

    g_nGradPoint += 2;
    g_nUndergradPoint += 2;
    return success;

error_close_scan: //close scan;
	ix_ScanIterator.close();

error_close_index: //close index
	indexManager->closeFile(fileHandle);

error_return:
	return fail;
}

int testCase_4B(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 2. Open Index File
    // 4. Scan entries NO_OP -- open**
    // 5. Scan close **
    // 6. Close Index File
    // 7. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 4B****" << endl;

    RID rid;
    RC rc;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;
    unsigned numOfTuples = 1000;

    // open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // compute inRidPageNumSum without inserting entries
    for(unsigned i = 0; i <= numOfTuples; i++)
    {
        key = i+1;//just in case somebody starts pageNum and recordId from 1
        rid.pageNum = key;
        rid.slotNum = key+1;

        inRidPageNumSum += rid.pageNum;
    }

    // scan
    rc = indexManager->scan(fileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan!" << endl;
        goto error_close_index;
    }

    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        outRidPageNumSum += rid.pageNum;
    }

    if (inRidPageNumSum != outRidPageNumSum)
    {
    	cout << "Wrong entries output...failure" << endl;
    	goto error_close_scan;
    }

    // close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // close index file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    // Destroy Index
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 3;
    g_nUndergradPoint += 3;
    return success;

error_close_scan: //close scan;
	ix_ScanIterator.close();

error_close_index: //close index file file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}

int testCase_5(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries using GE_OP operator and checking if the values returned are correct. **
    // 5. Scan close
    // 6. Close Index File
    // 7. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 5****" << endl;

    RID rid;
    RC rc;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 100;
    unsigned key;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;
    int value = 501;

    // create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // Test Insert Entry
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key+1;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    for(unsigned i = 501; i < numOfTuples+500; i++)
    {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key+1;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        inRidPageNumSum += rid.pageNum;
    }

    // Test Open Scan
    rc= indexManager->scan(fileHandle, attribute, &value, NULL, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    // Test IndexScan iterator
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        if (rid.pageNum < 501 || rid.slotNum < 502)
        {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
        }
        outRidPageNumSum += rid.pageNum;
    }

    if (inRidPageNumSum != outRidPageNumSum)
    {
        cout << "Wrong entries output...failure" << endl;
        goto error_close_scan;
    }

    // Test Closing Scan
    rc= ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // Test Closing Index
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    // Test Destroying Index
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}

int testCase_6(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries using LT_OP operator and checking if the values returned are correct. Returned values are part of two separate insertions **
    // 5. Scan close
    // 6. Close Index File
    // 7. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 6****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 2000;
    float key;
    float compVal = 6500;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;

    // create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        if (key < compVal)
        {
        	inRidPageNumSum += rid.pageNum;
        }
    }

    for(unsigned i = 6000; i <= numOfTuples+6000; i++)
    {
        key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i-(unsigned)500;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        if (key < compVal)
        {
        	inRidPageNumSum += rid.pageNum;
        }
    }

    // scan
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, false, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    // iterate
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if(rid.pageNum % 500 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        if ((rid.pageNum > 2000 && rid.pageNum < 6000) || rid.pageNum >= 6500)
        {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
        }
        outRidPageNumSum += rid.pageNum;
    }

    if (inRidPageNumSum != outRidPageNumSum)
    {
        cout << "Wrong entries output...failure" << endl;
        goto error_close_scan;
    }

    // close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // close index file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}


int testCase_7(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. OpenIndex File
    // 3. Insert entry
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. CloseIndex File
    // 7. DestroyIndex File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 7****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    float compVal = 100.0;
    unsigned numOfTuples = 100;
    float key;

    // create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // InsertEntry
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    // scan
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    // DeleteEntry in IndexScan Iterator
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;

        float key = (float)rid.pageNum;
        rc = indexManager->deleteEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
            goto error_close_scan;
        }
    }
    cout << endl;

    // close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // Open Scan again
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    //iterate
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << "Entry returned: " << rid.pageNum << " " << rid.slotNum << "--- failure" << endl;

        if(rid.pageNum > 100)
        {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
        }
    }

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    //close index file file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}

int testCase_8(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. OpenIndex File
    // 3. Insert entry
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex File
    // 9. DestroyIndex File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 8****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    float compVal;
    unsigned numOfTuples;
    float key;

    //create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    //open index file

    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    numOfTuples = 200;
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 200.0;
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    // Test DeleteEntry in IndexScan Iterator
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;

        key = (float)rid.pageNum;
        rc = indexManager->deleteEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
            goto error_close_scan;
        }
    }
    cout << endl;

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // insert entry Again
    numOfTuples = 500;
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 450.0;
    rc = indexManager->scan(fileHandle, attribute, &compVal, NULL, false, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum <= 450 || rid.slotNum <= 450)
        {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
        }
    }
    cout << endl;

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    //close index file file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}


int testCase_9(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex
    // 3. Insert entry
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 9****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    int compVal;
    int numOfTuples;
    int A[30000];
    int B[20000];
    int count = 0;
    int key;

    //create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    //open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    numOfTuples = 30000;
    for(int i = 0; i < numOfTuples; i++)
    {
        A[i] = i;
    }
    random_shuffle(A, A+numOfTuples);

    for(int i = 0; i < numOfTuples; i++)
    {
        key = A[i];
        rid.pageNum = i+1;
        rid.slotNum = i+1;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 20000;
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    // Test DeleteEntry in IndexScan Iterator
    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        key = A[rid.pageNum-1];
        rc = indexManager->deleteEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
            goto error_close_scan;
        }
        count++;
    }
    cout << "Number of deleted entries: " << count << endl;
    if (count != 20001)
    {
        cout << "Wrong entries output...failure" << endl;
        goto error_close_scan;
    }

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // insert entry Again
    numOfTuples = 20000;
    for(int i = 0; i < numOfTuples; i++)
    {
        B[i] = 30000+i;
    }
    random_shuffle(B, B+numOfTuples);

    for(int i = 0; i < numOfTuples; i++)
    {
        key = B[i];
        rid.pageNum = i+30001;
        rid.slotNum = i+30001;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 35000;
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if (count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum > 30000 && B[rid.pageNum-30001] > 35000)
        {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
        }
        count ++;
    }
    cout << "Number of scanned entries: " << count << endl;

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    //close index file file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}


int testCase_10(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex
    // 3. Insert entry
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 10****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    float compVal;
    int numOfTuples;
    float A[40000];
    float B[30000];
    float key;
    int count;


    //create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;

    }

    //open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    numOfTuples = 40000;
    for(int i = 0; i < numOfTuples; i++)
    {
        A[i] = (float)i;
    }
    random_shuffle(A, A+numOfTuples);

    for(int i = 0; i < numOfTuples; i++)
    {
        key = A[i];
        rid.pageNum = i+1;
        rid.slotNum = i+1;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 30000.0;
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    // Test DeleteEntry in IndexScan Iterator
    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        key = A[rid.pageNum-1];
        rc = indexManager->deleteEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
            goto error_close_scan;
        }
        count++;
    }
    cout << "Number of deleted entries: " << count << endl;
    if (count != 30001)
    {
        cout << "Wrong entries output...failure" << endl;
        goto error_close_scan;
    }

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    // insert entry Again
    numOfTuples = 30000;
    for(int i = 0; i < numOfTuples; i++)
    {
        B[i] = (float)(40000+i);
    }
    random_shuffle(B, B+numOfTuples);

    for(int i = 0; i < numOfTuples; i++)
    {
        float key = B[i];
        rid.pageNum = i+40001;
        rid.slotNum = i+40001;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 45000.0;
    rc = indexManager->scan(fileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if (count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum > 40000 && B[rid.pageNum-40001] > 45000)
        {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
        }
        count ++;
    }
    cout << "Number of scanned entries: " << count << endl;

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    //close index file file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}


int testCase_extra_1(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex
    // 3. Insert entry
    // 4. Scan entries.
    // 5. Scan close
    // 6. CloseIndex
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Extra Test Case 1****" << endl;

    RC rc;
    RID rid;
    unsigned numOfTuples;
    unsigned key;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    int compVal;
    int count = 0;

    //create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    //open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    numOfTuples = 2000;
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = 1234;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    numOfTuples = 2000;
    for(unsigned i = 500; i < numOfTuples+500; i++)
    {
        key = 4321;
        rid.pageNum = i;
        rid.slotNum = i-5;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 1234;
    rc = indexManager->scan(fileHandle, attribute, &compVal, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    //iterate
    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        count++;
    }
    cout << "Number of scanned entries: " << count << endl;
    if (count != 2000)
    {
        cout << "Wrong entries output...failure" << endl;
        goto error_close_scan;
    }

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    //close index file file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradExtraPoint += 5;
    g_nUndergradExtraPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}


int testCase_extra_2(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex
    // 3. Insert entry
    // 4. Scan entries.
    // 5. Scan close
    // 6. CloseIndex
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Extra Test Case 2****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    int compVal;
    unsigned numOfTuples;
    unsigned key;
    int count;

    //create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    //open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    numOfTuples = 2000;
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = 1234;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    numOfTuples = 2000;
    for(unsigned i = 500; i < numOfTuples+500; i++)
    {
        key = 4321;
        rid.pageNum = i;
        rid.slotNum = i-5;

        rc = indexManager->insertEntry(fileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 4321;
    rc = indexManager->scan(fileHandle, attribute, &compVal, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    //iterate
    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        count++;
    }
    cout << "Number of scanned entries: " << count << endl;
    if (count != 2000)
    {
        cout << "Wrong entries output...failure" << endl;
        goto error_close_scan;
    }

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    //close index file file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradExtraPoint += 5;
    g_nUndergradExtraPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}


int testCase_extra_3(const string &indexFileName, const Attribute &attribute)
{
    // Functions Tested:
    // 1. Create Index
    // 2. Open Index
    // 3. Insert Entry
    // 4. Scan
    // 5. Close Scan
    // 6. Close Index
    // 7. Destroy Index
    cout << endl << "****In Extra Test Case 3****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned offset;
    unsigned numOfTuples;
    char key[100];
    unsigned count;

    //create index file
    rc = indexManager->createFile(indexFileName);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        goto error_return;
    }

    //open index file
    rc = indexManager->openFile(indexFileName, fileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    numOfTuples = 5000;
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
    	count = ((i-1) % 26) + 1;
    	*(int *)key = count;
    	for(unsigned j = 0; j < count; j++)
    	{
    		*(key+4+j) = 96+count;
    	}

        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    offset = 20;
    *(int *)key = offset;
    for(unsigned j = 0; j < offset; j++)
    {
        *(key+4+j) = 96+offset;
    }

    rc = indexManager->scan(fileHandle, attribute, key, key, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    //iterate
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
    }
    cout << endl;

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    //close index file file
    rc = indexManager->closeFile(fileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradExtraPoint += 5;
    return success;

error_close_index: //close index file
	indexManager->closeFile(fileHandle);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName);

error_return:
	return fail;
}

void test()
{
	const string indexAgeFileName = "Age_idx";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "Age";
	attrAge.type = TypeInt;

	const string indexHeightFileName = "Height_idx";
	Attribute attrHeight;
	attrHeight.length = 4;
	attrHeight.name = "Height";
	attrHeight.type = TypeReal;

	const string indexEmpNameFileName = "EmpName_idx";
	Attribute attrEmpName;
	attrEmpName.length = 100;
	attrEmpName.name = "EmpName";
	attrEmpName.type = TypeVarChar;

	testCase_1(indexAgeFileName);
	testCase_2(indexAgeFileName, attrAge);
	testCase_3(indexAgeFileName, attrAge);
	testCase_4A(indexAgeFileName, attrAge);
	testCase_4B(indexAgeFileName, attrAge);
    testCase_5(indexAgeFileName, attrAge);
	//    testCase_6(indexHeightFileName, attrHeight);
//    testCase_7(indexHeightFileName, attrHeight);
//    testCase_8(indexHeightFileName, attrHeight);
//    testCase_9(indexAgeFileName, attrAge);
//    testCase_10(indexHeightFileName, attrHeight);
//
//    // Extra Credit Work
//    // Duplicat Entries
//    testCase_extra_1(indexAgeFileName, attrAge);
//    testCase_extra_2(indexAgeFileName, attrAge);
//    // TypeVarChar - mandatory for graduate students
//    testCase_extra_3(indexEmpNameFileName, attrEmpName);
    return;
}

int main()
{
    //Global Initializations
    cout << "****Starting Test Cases****" << endl;
    indexManager = IndexManager::instance();
    g_nGradPoint = 0;
    g_nGradExtraPoint = 0;
    g_nUndergradPoint = 0;
    g_nUndergradExtraPoint = 0;
    test();
    cout << "grad-point: " << g_nGradPoint << "\t grad-extra-point: " << g_nGradExtraPoint << endl;
    cout << "undergrad-point: " << g_nUndergradPoint << "\t undergrad-extra-point: " << g_nUndergradExtraPoint << endl;
    return 0;
}






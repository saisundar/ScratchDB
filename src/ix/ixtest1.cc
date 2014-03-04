#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

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

void test()
{
	const string indexAgeFileName = "Age_idx";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "Age";
	attrAge.type = TypeInt;

//    testCase_1(indexAgeFileName);
//    testCase_2(indexAgeFileName, attrAge);
//    testCase_3(indexAgeFileName, attrAge);
    testCase_4A(indexAgeFileName, attrAge);
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


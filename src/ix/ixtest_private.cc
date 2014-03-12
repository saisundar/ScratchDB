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

void print_point() {
    cout << "grad-point-p: " << g_nGradPoint << "\t grad-extra-point-p: " << g_nGradExtraPoint << endl;
    cout << "undergrad-point-p: " << g_nUndergradPoint << "\t undergrad-extra-point-p: " << g_nUndergradExtraPoint << endl;
}

int testCase_p1(const string &indexFileName1, const string &indexFileName2, const Attribute &attribute)
{
    cout << endl << "****In Test Case p1****" << endl;

    RC rc;
    RID rid;
    RID rid2;
    FileHandle fileHandle1;
    FileHandle fileHandle2;
    IX_ScanIterator ix_ScanIterator1;
    IX_ScanIterator ix_ScanIterator2;
    unsigned numOfTuples = 2000;
    float key;
    float key2;
    float compVal = 6500;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;

    // create index file1
    rc = indexManager->createFile(indexFileName1);
    if(rc == success)
    {
        cout << "Index File1 Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File1..." << endl;
        goto error_return;
    }

    // create index file2
    rc = indexManager->createFile(indexFileName2);
    if(rc == success)
    {
        cout << "Index File2 Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File2..." << endl;
        goto error_return;
    }

    // open index file1
    rc = indexManager->openFile(indexFileName1, fileHandle1);
    if(rc == success)
    {
        cout << "Index File1 Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File1..." << endl;
        goto error_destroy_index;
    }

    // open index file2
    rc = indexManager->openFile(indexFileName2, fileHandle2);
    if(rc == success)
    {
        cout << "Index File2 Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File2..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle1, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        if (key < compVal)
        {
        	inRidPageNumSum += rid.pageNum;
        }
        rc = indexManager->insertEntry(fileHandle2, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    for(unsigned i = 6000; i <= numOfTuples+6000; i++)
    {
        key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i-(unsigned)500;

        rc = indexManager->insertEntry(fileHandle1, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        if (key < compVal)
        {
        	inRidPageNumSum += rid.pageNum;
        }
        rc = indexManager->insertEntry(fileHandle2, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    // scan
    rc = indexManager->scan(fileHandle1, attribute, NULL, &compVal, true, false, ix_ScanIterator1);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }
    rc = indexManager->scan(fileHandle2, attribute, NULL, &compVal, true, false, ix_ScanIterator2);
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
    while(ix_ScanIterator1.getNextEntry(rid, &key) == success)
    {
    	if (ix_ScanIterator2.getNextEntry(rid2, &key2) != success) {
    		cout << "Wrong entries output...failure" << endl;
    	}
    	if (rid.pageNum != rid2.pageNum) {
    		cout << "Wrong entries output...failure" << endl;
    	}
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
    rc = ix_ScanIterator1.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    rc = ix_ScanIterator2.close();
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
    rc = indexManager->closeFile(fileHandle1);
    if(rc == success)
    {
        cout << "Index File1 Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File1..." << endl;
        goto error_destroy_index;
    }

    // close index file
    rc = indexManager->closeFile(fileHandle2);
    if(rc == success)
    {
        cout << "Index File2 Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File2..." << endl;
        goto error_destroy_index;
    }

    //destroy index file file
    rc = indexManager->destroyFile(indexFileName1);
    if(rc == success)
    {
        cout << "Index File1 Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File1..." << endl;
        goto error_return;
    }

    rc = indexManager->destroyFile(indexFileName2);
    if(rc == success)
    {
        cout << "Index File2 Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File2..." << endl;
        goto error_return;
    }

    g_nGradPoint += 5;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
	ix_ScanIterator1.close();
	ix_ScanIterator2.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle1);
	indexManager->closeFile(fileHandle2);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName1);
	indexManager->destroyFile(indexFileName2);

error_return:
	return fail;
}

int testCase_p2(const string &indexFileName1, const string &indexFileName2, const Attribute &attribute)
{
    cout << endl << "****In Test Case p2****" << endl;

    RC rc;
    RID rid;
    RID rid2;
    FileHandle fileHandle1;
    IX_ScanIterator ix_ScanIterator1;
    FileHandle fileHandle2;
    IX_ScanIterator ix_ScanIterator2;
    int compVal;
    int numOfTuples;
    int A[30000];
    int B[20000];
    int count = 0;
    int key;
    int key2;

    //create index file
    rc = indexManager->createFile(indexFileName1);
    if(rc == success)
    {
        cout << "Index1 Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File1..." << endl;
        goto error_return;
    }

    rc = indexManager->createFile(indexFileName2);
    if(rc == success)
    {
        cout << "Index2 Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File2..." << endl;
        goto error_return;
    }

    //open index file
    rc = indexManager->openFile(indexFileName1, fileHandle1);
    if(rc == success)
    {
        cout << "Index File1 Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File1..." << endl;
        goto error_destroy_index;
    }

    //open index file
    rc = indexManager->openFile(indexFileName2, fileHandle2);
    if(rc == success)
    {
        cout << "Index File2 Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File2..." << endl;
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

        rc = indexManager->insertEntry(fileHandle1, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        rc = indexManager->insertEntry(fileHandle2, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    //scan
    compVal = 20000;
    rc = indexManager->scan(fileHandle1, attribute, NULL, &compVal, true, true, ix_ScanIterator1);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    rc = indexManager->scan(fileHandle2, attribute, NULL, &compVal, true, true, ix_ScanIterator2);
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
    while(ix_ScanIterator1.getNextEntry(rid, &key) == success)
    {
    	if (ix_ScanIterator2.getNextEntry(rid2, &key2) != success || rid.pageNum != rid2.pageNum) {
    		cout << "Wrong entries output...failure" << endl;
    		goto error_close_scan;
    	}
        count++;
    }
    if (count != 20001)
    {
        cout << "Wrong entries output...failure" << endl;
        goto error_close_scan;
    }

    //close scan
    rc = ix_ScanIterator1.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    rc = ix_ScanIterator2.close();
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

        rc = indexManager->insertEntry(fileHandle1, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        rc = indexManager->insertEntry(fileHandle2, attribute, &key, rid);
        if(rc != success)
        {
        	cout << "Failed Inserting Keys..." << endl;
        	goto error_close_index;
        }
    }

    //scan
    compVal = 35000;
    rc = indexManager->scan(fileHandle1, attribute, NULL, &compVal, true, true, ix_ScanIterator1);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }
    rc = indexManager->scan(fileHandle2, attribute, NULL, &compVal, true, true, ix_ScanIterator2);
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
    while(ix_ScanIterator1.getNextEntry(rid, &key) == success)
    {
    	if (ix_ScanIterator2.getNextEntry(rid2, &key) != success) {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
    	}
        if(rid.pageNum > 30000 && B[rid.pageNum-30001] > 35000)
        {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
        }
        count ++;
    }
    cout << "Number of scanned entries: " << count << endl;

    //close scan
    rc = ix_ScanIterator1.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    rc = ix_ScanIterator2.close();
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
    rc = indexManager->closeFile(fileHandle1);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }
    rc = indexManager->closeFile(fileHandle2);
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
    rc = indexManager->destroyFile(indexFileName1);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }
    rc = indexManager->destroyFile(indexFileName2);
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
	ix_ScanIterator1.close();
	ix_ScanIterator2.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle1);
	indexManager->closeFile(fileHandle2);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName1);
	indexManager->destroyFile(indexFileName2);

error_return:
	return fail;
}

int testCase_p3(const string &indexFileName, const Attribute &attribute)
{
    // Functions Tested:
    // 1. Create Index
    // 2. Open Index
    // 3. Insert Entry
    // 4. Scan
    // 5. Close Scan
    // 6. Close Index
    // 7. Destroy Index
    cout << endl << "****In Test Case p3****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle;
    IX_ScanIterator ix_ScanIterator;
	char key[100];
    int numOfTuples = 1000;
	int i = 0;
	*(int*)key = 4;
	int count = 0;
    char lowKey[100];
    char highKey[100];

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
    for(i = 1; i <= numOfTuples; i++)
    {
    	sprintf(key + 4, "%04d", i);
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle, attribute, key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    *(int*)lowKey = 4;
    sprintf(lowKey+4, "%04d", 601);
    *(int*)highKey =4;
    sprintf(highKey+4, "%04d", 700);

    rc = indexManager->scan(fileHandle, attribute, lowKey, highKey, true, true, ix_ScanIterator);
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
    while(ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF)
    {
    	key[8] = '\0';
    	printf("output: %s\n", key+4);
    	count++;
    }
    if (count != 100) {
        cout << "Wrong output count! expected: 100\tactual: " << count << " ...Failure" << endl;
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

    g_nGradPoint += 3;
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

int testCase_p4(const string &indexFileName1, const Attribute &attribute1, const string &indexFileName2, const Attribute &attribute2)
{
    // Functions Tested:
    // 1. Create Index
    // 2. Open Index
    // 3. Insert Entry
    // 4. Scan
    // 5. Close Scan
    // 6. Close Index
    // 7. Destroy Index
    cout << endl << "****In Test Case p4****" << endl;

    RC rc;
    RID rid;
    FileHandle fileHandle1;
    IX_ScanIterator ix_ScanIterator1;
    FileHandle fileHandle2;
    IX_ScanIterator ix_ScanIterator2;

    int numOfPage1 = 0;
    int numOfPage2 = 0;

	char key[100];
    int numOfTuples = 5000;
	int i = 0;
	*(int*)key = 4;
	int count = 0;

    char lowKey[100];
    char highKey[100];

    //create index file
    rc = indexManager->createFile(indexFileName1);
    if(rc == success)
    {
        cout << "Index1 Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File1..." << endl;
        goto error_return;
    }
    rc = indexManager->createFile(indexFileName2);
    if(rc == success)
    {
        cout << "Index2 Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File2..." << endl;
        goto error_return;
    }

    //open index file
    rc = indexManager->openFile(indexFileName1, fileHandle1);
    if(rc == success)
    {
        cout << "Index File1 Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File1..." << endl;
        goto error_destroy_index;
    }
    rc = indexManager->openFile(indexFileName2, fileHandle2);
    if(rc == success)
    {
        cout << "Index File2 Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File2..." << endl;
        goto error_destroy_index;
    }

    // insert entry
    for(i = 1; i <= numOfTuples; i++)
    {
    	sprintf(key + 4, "%04d", i);
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager->insertEntry(fileHandle1, attribute1, key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
        rc = indexManager->insertEntry(fileHandle2, attribute2, key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
            goto error_close_index;
        }
    }

    numOfPage1 = fileHandle1.getNumberOfPages();
    if (numOfPage1 < 1) {
        cout << "Failed Inserting Keys..." << endl;
         goto error_close_index;
    }
    numOfPage2 = fileHandle2.getNumberOfPages();
    if (numOfPage1 < 1) {
        cout << "Failed Inserting Keys..." << endl;
         goto error_close_index;
    }

    if (numOfPage2 - numOfPage1 < 10) {
    	g_nUndergradExtraPoint += 5;
    	g_nGradPoint += 5;
    	print_point();
    } else {
    	cout << "file1's number of pages: " << numOfPage1 << "\tfile2's number of pages: " << numOfPage2 << endl;
    	cout << "Failed to handle space nicely for VarChar keys..." << endl;
    }

    *(int*)lowKey = 4;
    sprintf(lowKey+4, "%04d", 701);
    *(int*)highKey =4;
    sprintf(highKey+4, "%04d", 800);

    rc = indexManager->scan(fileHandle1, attribute1, lowKey, highKey, true, true, ix_ScanIterator1);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
        goto error_close_index;
    }

    rc = indexManager->scan(fileHandle2, attribute2, lowKey, highKey, true, true, ix_ScanIterator2);
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
    while(ix_ScanIterator1.getNextEntry(rid, &key) != IX_EOF)
    {
    	if (ix_ScanIterator2.getNextEntry(rid, &key) != success) {
            cout << "Wrong entries output...failure" << endl;
            goto error_close_scan;
    	}
    	key[8] = '\0';
    	printf("output: %s\n", key+4);
    	count++;
    }
    if (count != 100) {
    	cout << "Wrong output count! expected: 100\tactual: " << count << " ...Failure" << endl;
        goto error_close_scan;
    }

    //close scan
    rc = ix_ScanIterator1.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
        goto error_close_index;
    }

    rc = ix_ScanIterator2.close();
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
    rc = indexManager->closeFile(fileHandle1);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        goto error_destroy_index;
    }

    rc = indexManager->closeFile(fileHandle2);
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
    rc = indexManager->destroyFile(indexFileName1);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }
    rc = indexManager->destroyFile(indexFileName2);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
        goto error_return;
    }

    g_nGradPoint += 2;
    g_nUndergradPoint += 5;
    return success;

error_close_scan: //close scan
    ix_ScanIterator1.close();
    ix_ScanIterator2.close();

error_close_index: //close index file
	indexManager->closeFile(fileHandle1);
	indexManager->closeFile(fileHandle2);

error_destroy_index: //destroy index file
	indexManager->destroyFile(indexFileName1);
	indexManager->destroyFile(indexFileName2);

error_return:
	return fail;
}

void test()
{
	const string indexAgeFileName1 = "Age_idx1";
	const string indexAgeFileName2 = "Age_idx2";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "Age";
	attrAge.type = TypeInt;

	const string indexHeightFileName1 = "Height_idx1";
	const string indexHeightFileName2 = "Height_idx2";
	Attribute attrHeight;
	attrHeight.length = 4;
	attrHeight.name = "Height";
	attrHeight.type = TypeReal;

	const string indexEmpNameFileName1 = "EmpName_ShortIdx";
	const string indexEmpNameFileName2 = "EmpName_LongIdx";
	Attribute attrShortEmpName;
	attrShortEmpName.length = 10;
	attrShortEmpName.name = "ShortEmpName";
	attrShortEmpName.type = TypeVarChar;
	Attribute attrLongEmpName;
	attrLongEmpName.length = 100;
	attrLongEmpName.name = "LongEmpName";
	attrLongEmpName.type = TypeVarChar;

	testCase_p1(indexHeightFileName1, indexHeightFileName2, attrHeight);
	print_point();
	testCase_p2(indexAgeFileName1, indexAgeFileName2, attrAge);
	print_point();

	testCase_p3(indexEmpNameFileName1, attrShortEmpName);
	print_point();
	testCase_p4(indexEmpNameFileName1, attrShortEmpName, indexEmpNameFileName2, attrLongEmpName);
	print_point();


    return;
}

int main()
{
    //Global Initializations
    cout << "****Starting Private Test Cases****" << endl;
    indexManager = IndexManager::instance();
    g_nGradPoint = 0;
    g_nGradExtraPoint = 0;
    g_nUndergradPoint = 0;
    g_nUndergradExtraPoint = 0;
    test();
    return 0;
}






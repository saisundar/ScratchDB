#include "test_util.h"

#include <sstream>

unsigned total = 0;

int secA_8_B(const string &tableName)
{
    // Functions Tested
    // 1. Simple scan **
    cout << "****In Test Case 8_B****" << endl;

    RID rid;    
    int numTuples = 100;
    void *returnedData = malloc(100);

    set<int> ages; 
    RC rc = 0;
    for(int i = 0; i < numTuples; i++)
    {
        int age = 20+i;
        ages.insert(age);
    }

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr);
    rc = rm->scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    if(rc != success) {
        cout << "****Test case 8_B failed****" << endl << endl;
        return -1;
    }

    cout << "Scanned Data:" << endl;
    
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        cout << "Age: " << *(int *)returnedData << endl;
        if (ages.find(*(int *)returnedData) == ages.end())
        {
            cout << "****Test case 8_B failed****" << endl << endl;
            rmsi.close();
            free(returnedData);
            return -1;
        }
    }
    rmsi.close();
    
    // Delete a Table
    rc = rm->deleteTable(tableName);
    if(rc != success) {
        cout << "****Test case 8_B failed****" << endl << endl;
        return -1;
    }

    free(returnedData);
    cout << "****Test case 8_B passed****" << endl << endl; 
    return 0;
}


int secA_9(const string &tableName, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested:
    // 1. create table
    // 2. getAttributes
    // 3. insert tuple
    cout << "****In Test case 9****" << endl;

    RID rid; 
    void *tuple = malloc(1000);
    int numTuples = 2000;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    if(rc != success) {
         cout << "****Test case 9 failed****" << endl << endl;
        return -1;
    }

    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }

    // Insert 2000 tuples into table
    for(int i = 0; i < numTuples; i++)
    {
        // Test insert Tuple
        int size = 0;
        memset(tuple, 0, 1000);
        prepareLargeTuple(i, tuple, &size);

        rc = rm->insertTuple(tableName, tuple, rid);
        if(rc != success) {
             cout << "****Test case 9 failed****" << endl << endl;
            return -1;
        }

        rids.push_back(rid);
        sizes.push_back(size);        
    }
    cout << "****Test case 9 passed****" << endl << endl;

    free(tuple);

    return 0;
}


int secA_10(const string &tableName, const vector<RID> &rids, const vector<int> &sizes)
{
    // Functions Tested:
    // 1. read tuple
    cout << "****In Test case 10****" << endl;

    int numTuples = 2000;
    void *tuple = malloc(1000);
    void *returnedData = malloc(1000);

    RC rc = 0;
    for(int i = 0; i < numTuples; i++)
    {
        memset(tuple, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rm->readTuple(tableName, rids[i], returnedData);
        if(rc != success) {
             cout << "****Test case 10 failed****" << endl << endl;
            return -1;
        }

        int size = 0;
        prepareLargeTuple(i, tuple, &size);
        if(memcmp(returnedData, tuple, sizes[i]) != 0)
        {
            cout << "****Test case 10 failed****" << endl << endl;
            return -1;
        }
    }
    cout << "****Test case 10 passed****" << endl << endl;

    free(tuple);
    free(returnedData);

    return 0;
}


int secA_11(const string &tableName, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested:
    // 1. update tuple
    // 2. read tuple
    cout << "****In Test case 11****" << endl;

    RC rc = 0;
    void *tuple = malloc(1000);
    void *returnedData = malloc(1000);

    // Update the first 1000 tuples
    int size = 0;
    for(int i = 0; i < 1000; i++)
    {
        memset(tuple, 0, 1000);
        RID rid = rids[i];

        prepareLargeTuple(i+10, tuple, &size);
        rc = rm->updateTuple(tableName, tuple, rid);
        if(rc != success) {
             cout << "****Test case 11 failed****" << endl << endl;
            return -1;
        }

        sizes[i] = size;
        rids[i] = rid;
    }
    cout << "Updated!" << endl;

    // Read the recrods out and check integrity
    for(int i = 0; i < 1000; i++)
    {
        memset(tuple, 0, 1000);
        memset(returnedData, 0, 1000);
        prepareLargeTuple(i+10, tuple, &size);
        rc = rm->readTuple(tableName, rids[i], returnedData);
        if(rc != success) {
            cout << "****Test case 11 failed****" << endl << endl;
            return -1;
        }

        if(memcmp(returnedData, tuple, sizes[i]) != 0)
        {
            cout << "****Test case 11 failed****" << endl << endl;
            return -1;
        }
    }
    cout << "****Test case 11 passed****" << endl << endl;

    free(tuple);
    free(returnedData);

    return 0;
}


int secA_12(const string &tableName, const vector<RID> &rids)
{
    // Functions Tested
    // 1. delete tuple
    // 2. read tuple
    cout << "****In Test case 12****" << endl;

    RC rc = 0;
    void * returnedData = malloc(1000);

    // Delete the first 1000 tuples
    for(int i = 0; i < 1000; i++)
    {
        rc = rm->deleteTuple(tableName, rids[i]);
        if(rc != success) {
            cout << "****Test case 12 failed****" << endl << endl;
            return -1;
        }

        rc = rm->readTuple(tableName, rids[i], returnedData);
        if(rc == success) {
            cout << "****Test case 12 failed****" << endl << endl;
            return -1;
        }
    }
    cout << "After deletion!" << endl;

    for(int i = 1000; i < 2000; i++)
    {
        rc = rm->readTuple(tableName, rids[i], returnedData);
        if(rc != success) {
            cout << "****Test case 12 failed****" << endl << endl;
            return -1;
        }
    }
    cout << "****Test case 12 passed****" << endl << endl;

    free(returnedData);

    return 0;
}


int secA_13(const string &tableName)
{
    // Functions Tested
    // 1. scan
    cout << "****In Test case 13****" << endl;

    RM_ScanIterator rmsi;
    vector<string> attrs;
    attrs.push_back("attr5");
    attrs.push_back("attr12");
    attrs.push_back("attr28");
   
    RC rc = rm->scan(tableName, "", NO_OP, NULL, attrs, rmsi); 
    if(rc != success) {
        cout << "****Test case 13 failed****" << endl << endl;
        return -1;
    }

    RID rid;
    int j = 0;
    void *returnedData = malloc(1000);

    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        if(j % 200 == 0)
        {
            int offset = 0;

            cout << "Real Value: " << *(float *)(returnedData) << endl;
            offset += 4;
        
            int size = *(int *)((char *)returnedData + offset);
            cout << "String size: " << size << endl;
            offset += 4;

            char *buffer = (char *)malloc(size + 1);
            memcpy(buffer, (char *)returnedData + offset, size);
            buffer[size] = 0;
            offset += size;
    
            cout << "Char Value: " << buffer << endl;

            cout << "Integer Value: " << *(int *)((char *)returnedData + offset ) << endl << endl;
            offset += 4;

            free(buffer);
        }
        j++;
        memset(returnedData, 0, 1000);
    }
    rmsi.close();
    cout << "Total number of tuples: " << j << endl << endl;

    cout << "****Test case 13 passed****" << endl << endl;
    free(returnedData);

    return 0;
}


int secA_14(const string &tableName, const vector<RID> &rids)
{
    // Functions Tested
    // 1. reorganize page
    // 2. delete tuples
    // 3. delete table
    cout << "****In Test case 14****" << endl;

    RC rc;
    rc = rm->reorganizePage(tableName, rids[1000].pageNum);
    if(rc != success) {
        cout << "****Test case 14 failed****" << endl << endl;
        return -1;
    }

    rc = rm->deleteTuples(tableName);
    if(rc != success) {
        cout << "****Test case 14 failed****" << endl << endl;
        return -1;
    }

    rc = rm->deleteTable(tableName);
    if(rc != success) {
        cout << "****Test case 14 failed****" << endl << endl;
        return -1;
    }

    cout << "****Test case 14 passed****" << endl << endl;
    return 0;
}


int secA_15(const string &tableName) {

    cout << "****In Test case 15****" << endl;
    
    RID rid;    
    int tupleSize = 0;
    int numTuples = 500;
    void *tuple;
    void *returnedData = malloc(100);
    int ageVal = 25;

    RID rids[numTuples];
    vector<char *> tuples;

    RC rc = 0;
    int age;
    for(int i = 0; i < numTuples; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;
        
        age = (rand()%20) + 15;
        
        prepareTuple(6, "Tester", age, height, 123, tuple, &tupleSize);
        rc = rm->insertTuple(tableName, tuple, rid);
        if(rc != success) {
            cout << "****Test case 15 failed****" << endl << endl;
            return -1;
        }

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr); 
    rc = rm->scan(tableName, attr, GT_OP, &ageVal, attributes, rmsi);
    if(rc != success) {
        cout << "****Test case 15 failed****" << endl << endl;
        return -1;
    }

    cout << "Scanned Data:" << endl;
    
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        cout << "Age: " << *(int *)returnedData << endl;
        if((*(int *) returnedData) <= ageVal) {
            cout << "****Test case 15 failed****" << endl << endl;
            return -1;
        }
    }
    rmsi.close();
    
    // Deleta Table
    rc = rm->deleteTable(tableName);
    if(rc != success) {
        cout << "****Test case 15 failed****" << endl << endl;
        return -1;
    }

    free(returnedData);
    for(int i = 0; i < numTuples; i++)
    {
        free(tuples[i]);
    }
    
    cout << "****Test case 15 passed****" << endl << endl;
    return 0;
}

int testRMLayer(const string &tableName) {
    cout << "****In testRMLayer ****" << endl;

    RID rid;
    int tupleSize = 0;
    int numTuples = 10;
    void *tuple;
    void *returnedData = malloc(100);

    RID rids[numTuples];
    vector<char *> tuples;
    set<int> ages;
    RC rc = 0;
    for(int i = 0; i < numTuples; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;
        int age = i;
        ostringstream convert;   // stream used for the conversion
        convert << i;
        string name = "Tester" + convert.str();
        prepareTuple(name.size(), name, age, height, 2000 + i, tuple, &tupleSize);
        ages.insert(age);
        rc = rm->insertTuple(tableName, tuple, rid);
        if(rc != success) {
            cout << "****Test case testRMLayer failed****" << endl << endl;
            return -1;
        }

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    for(int i = 0; i < numTuples; i++)
    {
        int attrID = rand() % 4;
        string attributeName;
        if (attrID == 0) {
            attributeName = "EmpName";
        } else if (attrID == 1) {
            attributeName = "Age";
        } else if (attrID == 2) {
            attributeName = "Height";
        } else if (attrID == 3) {
            attributeName = "Salary";
        }
        rc = rm->readAttribute(tableName, rids[i], attributeName, returnedData);
        if(rc != success) {
            cout << "****Test case testRMLayer failed****" << endl << endl;
            return -1;
        }

        int nameLength = *(int *)tuples.at(i);
        if (attrID == 0) {
            if (memcmp(((char *)returnedData + 4), ((char *)tuples.at(i) + 4), nameLength) != 0) {
                cout << "****Test case testRMLayer failed" << endl << endl;
                return -1;
            }
        } else if (attrID == 1) {
            if (memcmp(((char *)returnedData), ((char *)tuples.at(i) + nameLength + 4), 4) != 0) {
                cout << "****Test case testRMLayer failed" << endl << endl;
                return -1;
            }
        } else if (attrID == 2) {
            if (memcmp(((char *)returnedData), ((char *)tuples.at(i) + nameLength + 4 + 4), 4) != 0) {
                cout << "****Test case testRMLayer failed" << endl << endl;
                return -1;
            }
        } else if (attrID == 3) {
            if (memcmp(((char *)returnedData), ((char *)tuples.at(i) + nameLength + 4 + 4 + 4), 4) != 0) {
                cout << "****Test case testRMLayer failed" << endl << endl;
                return -1;
            }
        }
    }


    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr);
    rc = rm->scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    if(rc != success) {
        cout << "****Test case testRMLayer failed****" << endl << endl;
        return -1;
    }

    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        if (ages.find(*(int *)returnedData) == ages.end())
        {
            cout << "****Test case testRMLayer failed****" << endl << endl;
            rmsi.close();
            free(returnedData);
            for(int i = 0; i < numTuples; i++)
            {
                free(tuples[i]);
            }
            return -1;
        }
    }
    rmsi.close();


    RM_ScanIterator rmsi2;

    void *value = malloc(14);
    string name = "Tester9999";
    int nameLength = 10;

    memcpy((char *)value, &nameLength, 4);
    memcpy((char *)value + 4, name.c_str(), nameLength);

    attributes.clear();
    attr = "EmpName";

    attributes.push_back("Height");
    attributes.push_back("Salary");
    rc = rm->scan(tableName, attr, GT_OP, value, attributes, rmsi2);
    if(rc != success) {
        free(returnedData);
        for(int i = 0; i < numTuples; i++)
        {
            free(tuples[i]);
        }
        cout << "****Test case testRMLayer failed****" << endl << endl;
        return -1;
    }

    int counter = 0;
    while(rmsi2.getNextTuple(rid, returnedData) != RM_EOF)
    {
        counter++;
        if (*(float *)returnedData > 100000.0 || *(float *)returnedData < 99990.0 || *(int *)((char *)returnedData + 4) > 102000 || *(int *)((char *)returnedData + 4) < 101990)
        {
             cout << "****Test case testRMLayer failed" << endl << endl;
             rmsi2.close();
             free(returnedData);
             for(int i = 0; i < numTuples; i++)
             {
                 free(tuples[i]);
             }
             return -1;
        }
    }
    rmsi2.close();

    for(int i = 0; i < numTuples; i++)
    {
        rc = rm->deleteTuple(tableName, rids[i]);
        if(rc != success) {
            free(returnedData);
            for(int i = 0; i < numTuples; i++)
            {
                free(tuples[i]);
            }
            cout << "****Test case testRMLayer failed****" << endl << endl;
            return -1;
        }

        rc = rm->readTuple(tableName, rids[i], returnedData);
        if(rc == success) {
            free(returnedData);
            for(int i = 0; i < numTuples; i++)
            {
                free(tuples[i]);
            }
            cout << "****Test case testRMLayer failed****" << endl << endl;
            return -1;
        }
    }

    // Set up the iterator
    RM_ScanIterator rmsi3;
    rc = rm->scan(tableName, "", NO_OP, NULL, attributes, rmsi3);
    if(rc != success) {
        free(returnedData);
        for(int i = 0; i < numTuples; i++)
        {
            free(tuples[i]);
        }
        cout << "****Test case testRMLayer failed****" << endl << endl;
        return -1;
    }

    if(rmsi3.getNextTuple(rid, returnedData) != RM_EOF)
    {
        cout << "****Test case testRMLayer failed" << endl << endl;
        rmsi3.close();
        free(returnedData);
        for(int i = 0; i < numTuples; i++)
        {
            free(tuples[i]);
        }
        return -1;
    }
    rmsi3.close();

    // Delete a Table
    rc = rm->deleteTable(tableName);
    if(rc != success) {
        cout << "****Test case testRMLayer failed****" << endl << endl;
        return -1;
    }

    free(returnedData);
    for(int i = 0; i < numTuples; i++)
    {
        free(tuples[i]);
    }
    cout << "****Test case testRMLayer passed****" << endl << endl;
    return 0;
}

void Tests()
{
    // Simple Scan
    int rc = secA_8_B("tbl_employee3");
    if (rc != 0) {
        total -= 4;
    }

    memProfile();

    // Pressure Test
    createLargeTable("tbl_employee4");

    vector<RID> rids;
    vector<int> sizes;

    // Insert Tuple
    rc = secA_9("tbl_employee4", rids, sizes);
    if (rc == 0) {
        total += 4;
        cout << "Grade is: " << total << endl;
    }
    // Read Tuple
    rc = secA_10("tbl_employee4", rids, sizes);
    if (rc == 0) {
        total += 4;
        cout << "Grade is: " << total << endl;
    }

    // Update Tuple
    rc = secA_11("tbl_employee4", rids, sizes);
    if (rc == 0) {
        total += 4;
        cout << "Grade is: " << total << endl;
    }

    // Delete Tuple
    rc = secA_12("tbl_employee4", rids);
    if (rc == 0) {
        total += 4;
        cout << "Grade is: " << total << endl;
    }

    memProfile();

    // Scan
    rc = secA_13("tbl_employee4");
    if (rc == 0) {
        total += 4;
        cout << "Grade is: " << total << endl;
    }

    // DeleteTuples/Table
    rc = secA_14("tbl_employee4", rids);
    if (rc == 0) {
        total += 4;
        cout << "Grade is: " << total << endl;
    }

    // Scan with conditions
    createTable("tbl_b_employee4");
    rc = secA_15("tbl_b_employee4");
    if (rc == 0) {
        total += 4;
        cout << "Grade is: " << total << endl;
    }

    memProfile();

    createTable("tbl_employee5");
    rc = testRMLayer("tbl_employee5");
    if (rc == 0) {
        total += 16;
        cout << "Grade is: " << total << endl;
    }
    
    memProfile();

    cout << "Grade is: " << total << endl;
    return;
}

void tests2(){
    createTable("tbl_employee5");
        RC rc = testRMLayer("tbl_employee5");
        if (rc == 0) {
            total += 16;
        }

        memProfile();
}

int main()
{
    // Basic Functions
	remove("System_Catalog");
	remove("cat_tbl_employee5");
	remove("tbl_employee5");
    cout << endl << "Test Basic Functions..." << endl;
  //  tests2();

   Tests();

    return 0;
}


#include <fstream>
#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <csignal>
#include <vector>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe.h"

#ifndef _success_
#define _success_
const int success = 0;
#endif

#ifndef _fail_
#define _fail_
const int fail = -1;
#endif

// Global Initialization
RelationManager *rm = RelationManager::instance();
IndexManager *im = IndexManager::instance();

// Number of tuples in each relation
const int tupleCount = 100;

// Number of tuples in left large relation
const int varcharTupleCount = 1000;

// Buffer size and character buffer size
const unsigned bufSize = 200;

char testLog[18];
int testG=0;
void incT()
{
	testG++;
}
void testInc()
{
	testLog[testG]='P';

}
#define TestSucc() {cout<<"****"<<__func__<<" passed!!!****"<<endl;testInc();}

void testDisp()
{

for(int i=1;i<9 && i<=testG;i++)
	cout<<"test number "<<i<<((testLog[i]=='P')?" passed":" failed")<<endl;
for(int i=9;i<11 && i<=testG;i++)
	cout<<"test number 9"<<(i==9?"grad":"undergrad")<<((testLog[i]=='P')?" passed":" failed")<<endl;
for(int i=11;i<=13 && i<=testG;i++)
	cout<<"test number "<<i-1<<((testLog[i]=='P')?" passed":" failed")<<endl;
for(int i=14;i<=17 && i<=testG;i++)
	cout<<"test number extraTestCase_"<<i-13<<((testLog[i]=='P')?" passed":" failed")<<endl;
}
void signalHandler( int signum )
 {
    cout.flush();
	cout << "Crash occurred while executing testcase "<<testG<<"leading to" << signum << "error..debug carefully....."<<endl;
     // cleanup and close up stuff here
     // terminate program
     testDisp();

    exit(signum);

 }
void testInit()
{
	signal (SIGINT, signalHandler);
	signal (SIGABRT, signalHandler);
	signal (SIGILL, signalHandler);
	signal (SIGSEGV, signalHandler);
	signal (SIGINT, signalHandler);
	signal (SIGTERM, signalHandler);

}

int createLeftTable() {
	// Functions Tested;
	// 1. Create Table
	cout << "****Create Left Table****" << endl;

	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "A";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "B";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "C";
	attr.type = TypeReal;
	attr.length = 4;
	attrs.push_back(attr);

	RC rc = rm->createTable("left", attrs);
	if (rc == success) {
		cout << "****Left Table Created!****" << endl;
	}
	return rc;
}

int createLeftVarCharTable() {
	// Functions Tested;
	// 1. Create Table
	cout << "****Create Left Large Table****" << endl;

	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "A";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "B";
	attr.type = TypeVarChar;
	attr.length = 30;
	attrs.push_back(attr);

	RC rc = rm->createTable("leftvarchar", attrs);
	if (rc == success) {
		cout << "****Left Var Char Table Created!****" << endl;
	}
	return rc;
}

int createRightTable() {
	// Functions Tested;
	// 1. Create Table
	cout << "****Create Right Table****" << endl;

	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "B";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "C";
	attr.type = TypeReal;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "D";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	RC rc = rm->createTable("right", attrs);
	if (rc == success) {
		cout << "****Right Table Created!****" << endl;
	}
	return rc;
}

int createRightVarCharTable() {
	// Functions Tested;
	// 1. Create Table
	cout << "****Create Right Large Table****" << endl;

	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "B";
	attr.type = TypeVarChar;
	attr.length = 30;
	attrs.push_back(attr);

	attr.name = "C";
	attr.type = TypeReal;
	attr.length = 4;
	attrs.push_back(attr);

	RC rc = rm->createTable("rightvarchar", attrs);
	if (rc == success) {
		cout << "****Right Var Char Table Created!****" << endl;
	}
	return rc;
}

int createGroupTable() {
	// Functions Tested;
	// 1. Create Table
	cout << "****Create Group Table****" << endl;

	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "A";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "B";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "C";
	attr.type = TypeReal;
	attr.length = 4;
	attrs.push_back(attr);

	RC rc = rm->createTable("group", attrs);
	if (rc == success) {
		cout << "****Group Table Created!****" << endl;
	}
	return rc;
}

// Prepare the tuple to left table in the format conforming to Insert/Update/ReadTuple and readAttribute
void prepareLeftTuple(const int a, const int b, const float c, void *buf) {
	int offset = 0;

	memcpy((char *) buf + offset, &a, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buf + offset, &b, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buf + offset, &c, sizeof(float));
	offset += sizeof(float);
}

// Prepare the tuple to right table in the format conforming to Insert/Update/ReadTuple, readAttribute
void prepareRightTuple(const int b, const float c, const int d, void *buf) {
	int offset = 0;

	memcpy((char *) buf + offset, &b, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buf + offset, &c, sizeof(float));
	offset += sizeof(float);

	memcpy((char *) buf + offset, &d, sizeof(int));
	offset += sizeof(int);
}

// Prepare the tuple to left var char table in the format conforming to Insert/Update/ReadTuple and readAttribute
void prepareLeftVarCharTuple(int a, int length, const string b, void *buf) {
	int offset = 0;

	memcpy((char *) buf + offset, &a, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buf + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buf + offset, b.c_str(), length);
	offset += length;
}

// Prepare the tuple to right var char table in the format conforming to Insert/Update/ReadTuple and readAttribute
void prepareRightVarCharTuple(int length, const string b, float c, void *buf) {
	int offset = 0;

	memcpy((char *) buf + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buf + offset, b.c_str(), length);
	offset += length;

	memcpy((char *) buf + offset, &c, sizeof(float));
	offset += sizeof(float);
}

int populateLeftTable() {
	// Functions Tested
	// 1. InsertTuple
	RC rc = success;
	RID rid;
	void *buf = malloc(bufSize);
	for (int i = 0; i < tupleCount; ++i) {
		memset(buf, 0, bufSize);

		// Prepare the tuple data for insertion
		// a in [0,99], b in [10, 109], c in [50, 149.0]
		int a = i;
		int b = i + 10;
		float c = (float) (i + 50);
		prepareLeftTuple(a, b, c, buf);

		rc = rm->insertTuple("left", buf, rid);
		if (rc != success) {
			goto clean_up;
		}
	}

clean_up:
	free(buf);
	return rc;
}

int populateRightTable() {
	// Functions Tested
	// 1. InsertTuple
	RC rc = success;
	RID rid;
	void *buf = malloc(bufSize);

	for (int i = 0; i < tupleCount; ++i) {
		memset(buf, 0, bufSize);

		// Prepare the tuple data for insertion
		// b in [20, 119], c in [25, 124.0], d in [0, 99]
		int b = i + 20;
		float c = (float) (i + 25);
		int d = i;
		prepareRightTuple(b, c, d, buf);

		rc = rm->insertTuple("right", buf, rid);
		if (rc != success) {
			goto clean_up;
		}
	}

clean_up:
	free(buf);
	return rc;
}

int populateLeftVarCharTable() {
	// Functions Tested
	// 1. InsertTuple
	RC rc = success;
	RID rid;
	void *buf = malloc(bufSize);

	for (int i = 0; i < varcharTupleCount; ++i) {
		memset(buf, 0, bufSize);

		// Prepare the tuple data for insertion
		int a = i + 20;

		int length = (i % 26) + 1;
		string b = string(length, '\0');
		for (int j = 0; j < length; j++) {
			b[j] = 96 + length;
		}
		prepareLeftVarCharTuple(a, length, b, buf);

		rc = rm->insertTuple("leftvarchar", buf, rid);
		if (rc != success) {
			goto clean_up;
		}
	}

clean_up:
	free(buf);
	return rc;
}

int populateRightVarCharTable() {
	// Functions Tested
	// 1. InsertTuple
	RC rc = success;
	RID rid;
	void *buf = malloc(bufSize);

	for (int i = 0; i < varcharTupleCount; ++i) {
		memset(buf, 0, bufSize);

		// Prepare the tuple data for insertion
		int length = (i % 26) + 1;
		string b = string(length, '\0');
		for (int j = 0; j < length; j++) {
			b[j] = 96 + length;
		}

		float c = (float) (i + 10);
		prepareRightVarCharTuple(length, b, c, buf);

		rc = rm->insertTuple("rightvarchar", buf, rid);
		if (rc != success) {
			goto clean_up;
		}
	}

clean_up:
	free(buf);
	return rc;
}

int populateGroupTable() {
	// Functions Tested
	// 1. InsertTuple
	RC rc = success;
	RID rid;
	void *buf = malloc(bufSize);
	for (int i = 0; i < tupleCount; ++i) {
		memset(buf, 0, bufSize);

		// Prepare the tuple data for insertion
		// a in repetition of [1,5], b in repetition of [1, 5], c in [50, 149.0]
		int a = i%5 + 1;
		int b = i%5 + 1;
		float c = (float) (i + 50);
		prepareLeftTuple(a, b, c, buf);

		rc = rm->insertTuple("group", buf, rid);
		if (rc != success) {
			goto clean_up;
		}
	}

clean_up:
	free(buf);
	return rc;
}

int createIndexforLeftB() {
	return rm->createIndex("left", "B");
}

int createIndexforLeftC() {
	return rm->createIndex("left", "C");
}

int createIndexforRightB() {
	return rm->createIndex("right", "B");
}

int createIndexforRightC() {
	return rm->createIndex("right", "C");
}

int testCase_1() {
	RC rc = success;
	incT();
	cout << "****In Test Case 1****" << endl;
	rc = createIndexforLeftB();
	if (rc != success) {
		return rc;
	}
	rc = populateLeftTable();
	if (rc != success) {
		return rc;
	}
	rc = createIndexforLeftC();
	if (rc != success) {
		return rc;
	}

	TestSucc();
	return rc;
}

int testCase_2() {
	RC rc = success;
	incT();
	cout << "****In Test Case 2****" << endl;
	rc = createIndexforRightB();
	if (rc != success) {
		return rc;
	}
	rc = populateRightTable();
	if (rc != success) {
		return rc;
	}
	rc = createIndexforRightC();
	if (rc != success) {
		return rc;
	}
	TestSucc();
	return rc;
}

int testCase_3() {
	// Functions Tested;
	// 1. Filter -- TableScan as input, on Integer Attribute
	cout << "****In Test Case 3****" << endl;
	incT();
	RC rc = success;

	TableScan *ts = new TableScan(*rm, "left");
	int compVal = 25;
	int valueB = 0;

	// Set up condition
	Condition cond;
	cond.lhsAttr = "left.B";
	cond.op = LE_OP;
	cond.bRhsIsAttr = false;
	Value value;
	value.type = TypeInt;
	value.data = malloc(bufSize);
	*(int *) value.data = compVal;
	cond.rhsValue = value;

	int expectedResultCnt = 16; //10~25;
	int actualResultCnt = 0;

	// Create Filter
	Filter *filter = new Filter(ts, cond);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (filter->getNextTuple(data) != QE_EOF) {
		int offset = 0;
		// Print left.A
		cout << "left.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.B
		valueB = *(int *) ((char *) data + offset);
		cout << "left.B " << valueB << endl;
		offset += sizeof(int);
		if (valueB > compVal) {
			rc = fail;
			goto clean_up;
		}

		// Print left.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete filter;
	delete ts;
	free(value.data);
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_4() {
	RC rc = success;
	// Functions Tested
	// 1. Filter -- IndexScan as input, on TypeReal attribute
	cout << "****In Test Case 3****" << endl;
incT();
	IndexScan *is = new IndexScan(*rm, "right", "C");
	float compVal = 100.0;
	float valueC = 0;

	// Set up condition
	Condition cond;
	cond.lhsAttr = "right.C";
	cond.op = GE_OP;
	cond.bRhsIsAttr = false;
	Value value;
	value.type = TypeReal;
	value.data = malloc(bufSize);
	*(float *) value.data = compVal;
	cond.rhsValue = value;

	int expectedResultCnt = 25; //100.00 ~ 124.00;
	int actualResultCnt = 0;

	// Create Filter
	Filter *filter = new Filter(is, cond);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (filter->getNextTuple(data) != QE_EOF) {
		int offset = 0;
		// Print right.B
		cout << "right.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print right.C
		valueC = *(float *) ((char *) data + offset);
		cout << "right.C " << valueC << endl;
		offset += sizeof(float);
		if (valueC < compVal) {
			rc = fail;
			goto clean_up;
		}

		// Print right.D
		cout << "right.D " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}
	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete filter;
	delete is;
	free(value.data);
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_5() {
	RC rc = success;
	// Functions Tested
	// 1. Project -- TableScan as input
	cout << "****In Test Case 5****" << endl;
	incT();
	TableScan *ts = new TableScan(*rm, "right");

	vector<string> attrNames;
	attrNames.push_back("right.C");
	attrNames.push_back("right.D");

	int expectedResultCnt = 100;
	int actualResultCnt = 0;
	int valueD = 0;

	// Create Projector
	Project *project = new Project(ts, attrNames);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (project->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print right.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.D
		valueD = *(int *) ((char *) data + offset);
		cout << "right.D " << valueD << endl;
		offset += sizeof(int);
		if (valueD < 0 || valueD > 99) {
			rc = fail;
			goto clean_up;
		}

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete project;
	delete ts;
	if(rc==success)
		TestSucc();
	free(data);
	return rc;
}

int testCase_6() {
	RC rc = success;
	// Functions Tested
	// 1. NLJoin -- on TypeInt Attribute
	cout << "****In Test Case 6****" << endl;
	incT();
	// Prepare the iterator and condition
	TableScan *leftIn = new TableScan(*rm, "left");
	TableScan *rightIn = new TableScan(*rm, "right");

	Condition cond;
	cond.lhsAttr = "left.B";
	cond.op = EQ_OP;
	cond.bRhsIsAttr = true;
	cond.rhsAttr = "right.B";

	int expectedResultCnt = 90; //20~109 --> left.B: [10,109], right.B: [20,119]
	int actualResultCnt = 0;
	int valueB = 0;

	// Create NLJoin
	NLJoin *nlJoin = new NLJoin(leftIn, rightIn, cond, 10);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (nlJoin->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print left.A
		cout << "left.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.B
		cout << "left.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.B
		valueB =  *(int *) ((char *) data + offset);
		cout << "right.B " << valueB << endl;
		offset += sizeof(int);

		if (valueB < 20 || valueB > 109) {
			rc = fail;
			goto clean_up;
		}

		// Print right.C
		cout << "right.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.D
		cout << "right.D " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete nlJoin;
	delete leftIn;
	delete rightIn;
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_7() {
	RC rc = success;
	// Functions Tested
	// 1. INLJoin -- on TypeReal Attribute
	cout << "****In Test Case 7****" << endl;
	incT();
	// Prepare the iterator and condition
	TableScan *leftIn = new TableScan(*rm, "left");
	IndexScan *rightIn = new IndexScan(*rm, "right", "C");

	Condition cond;
	cond.lhsAttr = "left.C";
	cond.op = EQ_OP;
	cond.bRhsIsAttr = true;
	cond.rhsAttr = "right.C";

	int expectedResultCnt = 75; // 50.0~124.0  left.C: [50.0,149.0], right.C: [25.0,124.0]
	int actualResultCnt = 0;
	float valueC = 0;

	// Create INLJoin
	INLJoin *inlJoin = new INLJoin(leftIn, rightIn, cond, 10);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (inlJoin->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print left.A
		cout << "left.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.B
		cout << "left.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.B
		cout << "right.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print right.C
		valueC = *(float *) ((char *) data + offset);
		cout << "right.C " << valueC << endl;
		offset += sizeof(float);
		if (valueC < 50.0 || valueC > 124.0) {
			rc = fail;
			goto clean_up;
		}

		// Print right.D
		cout << "right.D " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete inlJoin;
	delete leftIn;
	delete rightIn;
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_8() {
	RC rc = success;
	// Functions Tested
	// 1. NLJoin -- on TypeInt Attribute
	// 2. Filter -- on TypeInt Attribute
	cout << "****In Test Case 8****" << endl;
	incT();
	// Prepare the iterator and condition
	TableScan *leftIn = new TableScan(*rm, "left");
	TableScan *rightIn = new TableScan(*rm, "right");

	Condition cond_j;
	cond_j.lhsAttr = "left.B";
	cond_j.op = EQ_OP;
	cond_j.bRhsIsAttr = true;
	cond_j.rhsAttr = "right.B";

	// Create NLJoin
	NLJoin *nlJoin = new NLJoin(leftIn, rightIn, cond_j, 10);

	int compVal = 100;

	// Create Filter
	Condition cond_f;
	cond_f.lhsAttr = "right.B";
	cond_f.op = GE_OP;
	cond_f.bRhsIsAttr = false;
	Value value;
	value.type = TypeInt;
	value.data = malloc(bufSize);
	*(int *) value.data = compVal;
	cond_f.rhsValue = value;

	int expectedResultCnt = 10; // join result: [20,109] --> filter result [100, 109]
	int actualResultCnt = 0;
	int valueB = 0;

	Filter *filter = new Filter(nlJoin, cond_f);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (filter->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print left.A
		cout << "left.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.B
		valueB = *(int *) ((char *) data + offset);
		cout << "left.B " << valueB << endl;
		offset += sizeof(int);
		if (valueB < 100 || valueB > 109) {
			rc = fail;
			goto clean_up;
		}

		// Print left.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.B
		cout << "right.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);


		// Print right.C
		cout << "right.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.D
		cout << "right.D " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete filter;
	delete nlJoin;
	delete leftIn;
	delete rightIn;
	free(value.data);
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_9_Grad() {
	RC rc = 0;
	// Functions Tested
	// 1. Filter
	// 2. Project
	// 3. INLJoin(Grad)/NLJoin(Undergrad)
	incT();
	cout << "****In Test Case 9_Grad****" << endl;

	// Create Filter
	IndexScan *leftIn = new IndexScan(*rm, "left", "B");

	int compVal = 75;

	Condition cond_f;
	cond_f.lhsAttr = "left.B";
	cond_f.op = LT_OP;
	cond_f.bRhsIsAttr = false;
	Value value;
	value.type = TypeInt;
	value.data = malloc(bufSize);
	*(int *) value.data = compVal;
	cond_f.rhsValue = value;

	leftIn->setIterator(NULL, value.data, true, false);
	Filter *filter = new Filter(leftIn, cond_f); //left.B: 10~74, left.C: 50.0~114.0

	// Create Project
	vector<string> attrNames;
	attrNames.push_back("left.A");
	attrNames.push_back("left.C");
	Project *project = new Project(filter, attrNames);

	Condition cond_j;
	cond_j.lhsAttr = "left.C";
	cond_j.op = EQ_OP;
	cond_j.bRhsIsAttr = true;
	cond_j.rhsAttr = "right.C";

	// Create Join
	IndexScan *rightIn = NULL;
	Iterator *join = NULL;
	rightIn = new IndexScan(*rm, "right", "C");
	join = new INLJoin(project, rightIn, cond_j, 8);

	int expectedResultCnt = 65; //50.0~114.0
	int actualResultCnt = 0;
	float valueC = 0;

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (join->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print left.A
		cout << "left.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.B
		cout << "right.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print right.C
		valueC = *(float *) ((char *) data + offset);
		cout << "right.C " << valueC << endl;
		offset += sizeof(float);
		if (valueC < 50.0 || valueC > 114.0) {
			rc = fail;
			goto clean_up;
		}

		// Print right.D
		cout << "right.D " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete join;
	delete rightIn;
	delete project;
	delete filter;
	delete leftIn;
	free(value.data);
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_9_Undergrad() {
	RC rc = 0;
	// Functions Tested
	// 1. Filter
	// 2. Project
	// 3. INLJoin(Grad)/NLJoin(Undergrad)
	incT();
	cout << "****In Test Case 9_Undergrad****" << endl;

	// Create Filter
	IndexScan *leftIn = new IndexScan(*rm, "left", "B");

	int compVal = 75;

	Condition cond_f;
	cond_f.lhsAttr = "left.B";
	cond_f.op = LT_OP;
	cond_f.bRhsIsAttr = false;
	Value value;
	value.type = TypeInt;
	value.data = malloc(bufSize);
	*(int *) value.data = compVal;
	cond_f.rhsValue = value;

	leftIn->setIterator(NULL, value.data, true, false);
	Filter *filter = new Filter(leftIn, cond_f); //left.B: 10~74, left.C: 50.0~114.0

	// Create Project
	vector<string> attrNames;
	attrNames.push_back("left.A");
	attrNames.push_back("left.C");
	Project *project = new Project(filter, attrNames);

	Condition cond_j;
	cond_j.lhsAttr = "left.C";
	cond_j.op = EQ_OP;
	cond_j.bRhsIsAttr = true;
	cond_j.rhsAttr = "right.C";

	// Create Join
	TableScan *rightIn = NULL;
	Iterator *join = NULL;
	rightIn = new TableScan(*rm, "right");
	join = new NLJoin(project, rightIn, cond_j, 10);

	int expectedResultCnt = 65; //50.0~114.0
	int actualResultCnt = 0;
	float valueC = 0;

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (join->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print left.A
		cout << "left.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.B
		cout << "right.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print right.C
		valueC = *(float *) ((char *) data + offset);
		cout << "right.C " << valueC << endl;
		offset += sizeof(float);
		if (valueC < 50.0 || valueC > 114.0) {
			rc = fail;
			goto clean_up;
		}

		// Print right.D
		cout << "right.D " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

clean_up:
	delete join;
	delete rightIn;
	delete project;
	delete filter;
	delete leftIn;
	free(value.data);
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_10() {
	RC rc = success;
	// Functions Tested
	// 1. NLJoin -- on TypeInt Attribute
	cout << "****In Test Case 10****" << endl;
	incT();
	// Prepare the iterator and condition
	TableScan *leftIn = new TableScan(*rm, "left");
	TableScan *rightIn = new TableScan(*rm, "right");

	Condition cond;
	cond.lhsAttr = "left.B";
	cond.op = LE_OP;
	cond.bRhsIsAttr = true;
	cond.rhsAttr = "right.B";

	int expectedResultcnt = 5995;
	int actualResultCnt = 0;

	// Create NLJoin
	NLJoin *nlJoin = new NLJoin(leftIn, rightIn, cond, 10);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (nlJoin->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print left.A
		cout << "left.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.B
		cout << "left.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print left.C
		cout << "left.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.B
		cout << "right.B " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print right.C
		cout << "right.C " << *(float *) ((char *) data + offset) << endl;
		offset += sizeof(float);

		// Print right.D
		cout << "right.D " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultcnt != actualResultCnt) {
		rc = fail;
	}

	delete nlJoin;
	delete leftIn;
	delete rightIn;
	free(data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_11() {
	RC rc = success;
	// Functions Tested
	// 1. Filter -- on TypeVarChar Attribute
	cout << "****In Test Case 11****" << endl;
	incT();
	TableScan *ts = new TableScan(*rm, "leftvarchar");

	// Set up condition
	Condition cond;
	cond.lhsAttr = "leftvarchar.B";
	cond.op = EQ_OP;
	cond.bRhsIsAttr = false;
	Value value;
	value.type = TypeVarChar;
	value.data = malloc(bufSize);
	int length = 12;
	*(int *) ((char *) value.data) = length;
	for (unsigned i = 0; i < 12; ++i) {
		*(char *) ((char*)value.data + 4 + i) = 12 + 96;
	}
	cond.rhsValue = value; // "llllllllllll"

	// Create Filter
	Filter *filter = new Filter(ts, cond);

	int expectedResultCnt = 39;
	int actualResultCnt = 0;

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (filter->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print leftvarchar.A
		cout << "leftvarchar.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print leftvarchar.B
		int length = *(int *) ((char *) data + offset);
		offset += 4;
		cout << "leftvarchar.B.length " << length << endl;

		char *b = (char *) malloc(100);
		memcpy(b, (char *) data + offset, length);
		b[length] = '\0';
		offset += length;
		cout << "leftvarchar.B " << b << endl;

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

	delete filter;
	delete ts;
	free(data);
	free(value.data);
	if(rc==success)
		TestSucc();
	return rc;
}

int testCase_12() {
	RC rc = success;
	// Functions Tested
	// 1. NLJoin -- on TypeVarChar Attribute
	cout << "****In Test Case 12****" << endl;
	incT();
	// Prepare the iterator and condition
	TableScan *leftIn = new TableScan(*rm, "leftvarchar");
	TableScan *rightIn = new TableScan(*rm, "rightvarchar");

	Condition cond;
	cond.lhsAttr = "leftvarchar.B";
	cond.op = EQ_OP;
	cond.bRhsIsAttr = true;
	cond.rhsAttr = "rightvarchar.B";

	int expectedResultCnt = 38468;
	int actualResultCnt = 0;

	// Create NLJoin
	NLJoin *nlJoin = new NLJoin(leftIn, rightIn, cond, 5);

	// Go over the data through iterator
	void *data = malloc(bufSize);
	while (nlJoin->getNextTuple(data) != QE_EOF) {
		int offset = 0;

		// Print leftvarchar.A
		cout << "leftvarchar.A " << *(int *) ((char *) data + offset) << endl;
		offset += sizeof(int);

		// Print leftvarchar.B
		int length = *(int *) ((char *) data + offset);
		offset += 4;
		cout << "leftvarchar.B.length " << length << endl;

		char *b = (char *) malloc(100);
		memcpy(b, (char *) data + offset, length);
		b[length] = '\0';
		offset += length;
		cout << "leftvarchar.B " << b << endl;

		// Print rightvarchar.B
		length = *(int *) ((char *) data + offset);
		offset += 4;
		cout << "rightvarchar.B.length " << length << endl;

		b = (char *) malloc(100);
		memcpy(b, (char *) data + offset, length);
		b[length] = '\0';
		offset += length;
		cout << "rightvarchar.B " << b << endl;

		// Print rightvarchar.B
		cout << "rightvarchar.C " << *(float *) ((char *) data + offset)
				<< endl;
		offset += sizeof(float);

		memset(data, 0, bufSize);
		++actualResultCnt;
	}

	if (expectedResultCnt != actualResultCnt) {
		rc = fail;
	}

	delete nlJoin;
	delete leftIn;
	delete rightIn;
	if(rc==success)
		TestSucc();
	free(data);
	return rc;
}


int extraTestCase_1()
{
	RC rc = success;
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- MAX
    cout << "****In Extra Test Case 1****" << endl;
    incT();
    // Create TableScan
    TableScan *input = new TableScan(*rm, "left");

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "left.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;
    Aggregate *agg = new Aggregate(input, aggAttr, MAX);

    void *data = malloc(bufSize);
    int maxVal = 0;
    while(agg->getNextTuple(data) != QE_EOF)
    {
    	maxVal = *(int *)data;
        cout << "MAX(left.B) " << maxVal << endl;
        memset(data, 0, sizeof(int));
    }

    if (maxVal != 109) {
    	rc = fail;
    }

    delete agg;
    delete input;
    free(data);
    if(rc==success)
		TestSucc();
    return rc;
}


int extraTestCase_2()
{
	RC rc = success;
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- AVG
    cout << "****In Extra Test Case 2****" << endl;
    incT();
    // Create TableScan
    TableScan *input = new TableScan(*rm, "right");

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "right.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;
    Aggregate *agg = new Aggregate(input, aggAttr, AVG);

    void *data = malloc(bufSize);
    float average = 0;
    while(agg->getNextTuple(data) != QE_EOF)
    {
    	average = *(float *)data;
        cout << "AVG(right.B) " << average << endl;
        memset(data, 0, sizeof(float));
    }

    if (average != 69.5) {
    	rc = fail;
    }

    delete agg;
    delete input;
    free(data);
    if(rc==success)
		TestSucc();
    return rc;
}

int extraTestCase_3()
{
	RC rc = 0;
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- MIN (with GroupBy)
    cout << "****In Extra Test Case 3****" << endl;
	incT();
    // Create TableScan
    TableScan *input = new TableScan(*rm, "group");

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "group.A";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;

    Attribute gAttr;
    gAttr.name = "group.B";
    gAttr.type = TypeInt;
    gAttr.length = 4;
    Aggregate *agg = new Aggregate(input, aggAttr, gAttr, MIN);

    int idVal = 0;
    int minVal = 0;
    int expectedResultCnt = 5;
    int actualResultCnt = 0;

    void *data = malloc(bufSize);
    while(agg->getNextTuple(data) != QE_EOF)
    {
        int offset = 0;

        // Print group.B
        idVal = *(int *)((char *)data + offset);
        cout << "group.B " << idVal << endl;
        offset += sizeof(float);

        // Print MIN(group.A)
        minVal = *(int *)((char *)data + offset);
        cout << "MIN(group.A) " <<  minVal << endl;
        offset += sizeof(int);

        memset(data, 0, bufSize);
        if (idVal != minVal) {
        	rc = fail;
        	goto clean_up;
        }
        ++actualResultCnt;
    }

    if (expectedResultCnt != actualResultCnt) {
    	rc = fail;
    }

clean_up:
	delete agg;
	delete input;
    free(data);
    if(rc==success)
		TestSucc();
    return rc;
}


int extraTestCase_4()
{
	RC rc = success;
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- SUM (with GroupBy)
    cout << "****In Extra Test Case 4****" << endl;
    incT();
    // Create TableScan
    TableScan *input = new TableScan(*rm, "group");

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "group.A";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;

    Attribute gAttr;
    gAttr.name = "group.B";
    gAttr.type = TypeInt;
    gAttr.length = 4;
    Aggregate *agg = new Aggregate(input, aggAttr, gAttr, SUM);

    int idVal = 0;
    int sumVal = 0;
    int expectedResultCnt = 5;
    int actualResultCnt = 0;

    void *data = malloc(bufSize);
    while(agg->getNextTuple(data) != QE_EOF)
    {
        int offset = 0;

        // Print group.B
        idVal = *(int *)((char *)data + offset);
        cout << "group.B " << idVal << endl;
        offset += sizeof(float);

        // Print SUM(group.A)
        sumVal = *(int *)((char *)data + offset);
        cout << "SUM(group.A) " <<  sumVal << endl;
        offset += sizeof(int);

        memset(data, 0, bufSize);
        if (sumVal != (idVal*20)) {
        	rc = fail;
        	goto clean_up;
        }
        ++actualResultCnt;
    }

    if (expectedResultCnt != actualResultCnt) {
    	rc = fail;
    }

clean_up:
	delete agg;
	delete input;
    free(data);
    if(rc==success)
		TestSucc();
    return rc;
}


int main() {

	int g_nGradPoint = 0;
	int g_nGradExtraPoint = 0;
	int g_nUndergradPoint = 0;
	int g_nUndergradExtraPoint = 0;
	testInit();
	// Create the left table
	if (createLeftTable() != success) {
		goto print_point;
	}

	if (testCase_1() != success) {
		goto print_point;
	}
	g_nGradPoint += 5;
	g_nUndergradPoint += 5;

	// Create the right table
	if (createRightTable() != success) {
		goto print_point;
	}

	if (testCase_2() != success) {
		goto print_point;
	}
	g_nGradPoint += 5;
	g_nUndergradPoint += 5;

	if (testCase_3() == success) {
		g_nGradPoint += 5;
		g_nUndergradPoint += 5;
	}

	if (testCase_4() == success) {
		g_nGradPoint += 5;
		g_nUndergradPoint += 5;
	}

	if (testCase_5() == success) {
		g_nGradPoint += 3;
		g_nUndergradPoint += 3;
	}

	if (testCase_6() == success) {
		g_nGradPoint += 5;
		g_nUndergradPoint += 10;
	}

	if (testCase_7() == success) {
		g_nGradPoint += 5;
		g_nUndergradExtraPoint += 3;
	}

	if (testCase_8() == success) {
		g_nGradPoint += 3;
		g_nUndergradPoint += 3;
	}

	if (testCase_9_Grad() == success) {
		g_nGradPoint += 3;
		g_nUndergradExtraPoint += 2;
	}

	if (testCase_9_Undergrad() == success) {
		g_nGradPoint += 2;
		g_nUndergradPoint += 5;
	}

	if (testCase_10() == success) {
		g_nGradPoint += 3;
		g_nUndergradPoint += 3;
	}

	// Create left/right large table, and populate the table
	if (createLeftVarCharTable() != success) {
		goto print_point;
	}

	if (populateLeftVarCharTable() != success) {
		goto print_point;
	}

	if (createRightVarCharTable() != success) {
		goto print_point;
	}

	if (populateRightVarCharTable() != success) {
		goto print_point;
	}

	if (testCase_11() == success) {
		g_nGradPoint += 3;
		g_nUndergradPoint += 3;
	}

	if (testCase_12() == success) {
		g_nGradPoint += 3;
		g_nUndergradPoint += 3;
	}

    // Extra Credit
	// Aggregate
	if (extraTestCase_1() == success) {
		g_nGradExtraPoint += 3;
		g_nUndergradExtraPoint += 3;
	}

	if (extraTestCase_2() == success) {
		g_nGradExtraPoint += 2;
		g_nUndergradExtraPoint += 2;
	}

	if (createGroupTable() != success) {
		goto print_point;
	}

	if (populateGroupTable() != success) {
		goto print_point;
	}

	// Aggregate with GroupBy
    if (extraTestCase_3() == success) {
		g_nGradExtraPoint += 5;
		g_nUndergradExtraPoint += 5;
    }

    if (extraTestCase_4() == success) {
		g_nGradExtraPoint += 5;
		g_nUndergradExtraPoint += 5;
    }

    testDisp();
	print_point: cout << "grad-point: " << g_nGradPoint
			<< "\t grad-extra-point: " << g_nGradExtraPoint << endl;
	cout << "undergrad-point: " << g_nUndergradPoint
			<< "\t undergrad-extra-point: " << g_nUndergradExtraPoint << endl;

	return 0;
}


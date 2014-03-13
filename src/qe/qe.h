#ifndef _qe_h_
#define _qe_h_

#include <limits>
#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan
# define FLTMAX (std::numeric_limits<float>::max());
# define FLTMIN (std::numeric_limits<float>::min());
# define INTMIN (std::numeric_limits<int>::min()+10000);
# define INTMAX (std::numeric_limits<int>::max()-10000)

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;


// The following functions use  the following
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by
//                          the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string lhsAttr;         // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class mapKey {

public:

	mapKey()
	{
		num=0;
		val=0;
		name="";
	}
	mapKey(Attribute attribute,INT32 value)
	{
		num=value;
		attr=attribute;
		val=0;
		name="";
	}

	mapKey(Attribute attribute,float value)
	{
		num=0;
		attr=attribute;
		val=value;
		name="";


	}
	mapKey(Attribute attribute,string value)
	{
		num=0;
		attr=attribute;
		val=0;
		name=value;
	}

	Attribute attr;
	INT32 num;
	float val;
	string name;

	bool operator <(const mapKey& rhs) const
	{
		INT32 diff;
		switch(attr.type)
		{
		case 0:
			return num < rhs.num;
			break;
		case 1:
			return val < rhs.val;
			break;
		case 2:
			diff=strcmp(name.c_str(),rhs.name.c_str());
			return (diff<0);
			break;
		default:
			break;

		}
		return true;
	}
};

class TableScan : public Iterator
{
// A wrapper inheriting Iterator over RM_ScanIterator
// Its an iterator which use the RM layer table iterator.
//	It contains a resettable rm iterator object.
//	always does a full table scan
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }

            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;
            dbgnQE("size of new attrs",attrs.size());
            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        	delete iter;
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
	// Its an iterator which use the RM layer Index iterator.
	//	It contains a resettable rmIndex iterator object.
	//	May do a full table scan or a partial can based on setIterators condition.
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
            delete iter;
        };
};


class Filter : public Iterator {
    // Filter operator
	Iterator *inp;
	Condition cond;
	vector<Attribute> attrs;
	void* valueP;
	bool valid;
	AttrType type;
	float result;
    public:
        Filter(Iterator *input,const Condition &condition)
        {
        	dbgnQEFn();
        	inp=input;
        	cond=condition;
        	valueP=NULL;
        	inp->getAttributes(attrs);
        	valid=isValidAttr();
        	dbgnQEFnc();
        }
        ~Filter(){
        	dbgnQEFn();
        	inp=NULL;
        	if(valueP!=NULL)
        		free(valueP);
        	dbgnQEFnc();
        };
        bool evaluateCondition(void * temp);
        bool returnRes(int diff);
        RC getRHSAddr(void* data);
        BYTE* getLHSAddr(void* data);


        RC getNextTuple(void *data)
        {
        	dbgnQEFn();
        	static int tupleCount=0;
        	if(!valid)return QE_EOF;
        	BYTE* lhs;
        	while(1)
        	{
        		tupleCount++;
        		if(inp->getNextTuple(data)==QE_EOF){
        			dbgnQE("tuple count",tupleCount);
        			return QE_EOF;
        		}
        		lhs= getLHSAddr(data);

        		if(cond.bRhsIsAttr)
        			getRHSAddr(data);

        		if(evaluateCondition(lhs))
        			return 0;
        		dbgnQEFnc();
        	}

        };
        bool isValidAttr();



        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{

        	 attrs.clear();
        	 attrs = this->attrs;
        };
};


class Project : public Iterator {
	// Projection operator
public:

	vector<Attribute> lowerLevelDescriptor;
	vector<string> attrNames;
	Iterator *input;
	BYTE* lowerLevelData;
	bool valid ;
	vector<Attribute> projAttrs;
	bool isValidAttr();
	Project(Iterator *input,                            // Iterator of input R
			const vector<string> &attrNames);           // vector containing attribute names
	~Project()
	{
		dbgnQEFn();
		free(lowerLevelData);
		dbgnQEFnc();
	}

	RC getNextTuple(void *data);
	void getAttributes(vector<Attribute> &attrs) const;
	int readAttribute(const string &attributeName, BYTE * inputData, BYTE* outputData);
	void setValidFalse();
};


class NLJoin : public Iterator {
    // Nested-Loop join operator
    public:
        NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        ){};
        ~NLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};


class INLJoin : public Iterator {
    // Index Nested-Loop join operator
    public:
        INLJoin(Iterator *leftIn,                               // Iterator of input R
                IndexScan *rightIn,                             // IndexScan Iterator of input S
                const Condition &condition,                     // Join condition
                const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
        ){};

        ~INLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};

class Answer{
public:
    float val; // for holding min/max or sum
	int count;	// for counting the tuples
	Answer()
	{

		count=0;
		val=0;
	}
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
		Iterator *inp;
	    Attribute attr;
	    Attribute groupAttr;
	    AggregateOp opAg;
	    bool valid;
	    vector<Attribute> attrs;
	    bool groupBy;
	    void* valueP;
	    void* groupP;


	    bool isValidAttr();
	    RC getAttrAddr(void* data);
	    RC updateAns(Answer &ans,float diff);
	    float compareAttr(Answer ans);
	    RC copyFinalAns(void* data,Answer ans,mapKey tempKey);
	    map<mapKey,Answer> grouper;
	    map<mapKey,Answer>::iterator mapIter;
	    mapKey tempKey;
	    Answer getGroupAns();
	    void setExtremeAns(Answer &ans);


        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
        ){
        	dbgnQEFn();
        	inp=input;
        	attr=aggAttr;
        	opAg=op;
        	Answer ans;
        	valid=false;
        	groupBy=false;
        	inp->getAttributes(attrs);
        	valid=isValidAttr();
        	dbgnQE("valid",valid);
        	setExtremeAns(ans);

        	if(valid)
        	{
        		void* data=malloc(1024);
        		mapKey dummy(attr,"saisundar");
        		float diff;
        		dbgnQE("valid and no group by ..so iterating through every tuple","");
        		while(inp->getNextTuple(data)!=QE_EOF)
        		{
        			getAttrAddr(data);
        			diff=compareAttr(ans);
        			updateAns(ans,diff);
        			dbgnQE("ans count  updated ",ans.count);
        		    dbgnQE("ans val upadted   ",ans.val);
        		}
        		grouper[dummy]=ans;
        		dbgnQE("final count  inserted ",ans.count);
        		dbgnQE("final answer  inserted ",ans.val);
        		mapIter=grouper.begin();
        		free(data);
        	}


            dbgnQEFnc();
        };

        // Extra Credit
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  Attribute gAttr,                              // The attribute over which we are grouping the tuples
                  AggregateOp op                                // Aggregate operation
        ){
        	dbgnQEFn();
        	inp=input;
        	attr=aggAttr;
        	groupAttr=gAttr;
        	opAg=op;
        	valid=false;
        	groupBy=true;
        	inp->getAttributes(attrs);
        	valid=isValidAttr();
        	dbgnQE("valid",valid);

            if(valid)
        	{
        		void* data=malloc(1024);
        	    float diff;
        		Answer ans;
        		setExtremeAns(ans);
        		dbgnQE("valid and group by ..so iterating through every tuple","");
        		while(inp->getNextTuple(data)!=QE_EOF)
        		{
        			getAttrAddr(data);
        			ans=getGroupAns();
        			diff=compareAttr(ans);
        			updateAns(ans,diff);
        			grouper[tempKey]=ans;
        		}

        		dbgnQE("the new count inserted ",ans.count);
        		dbgnQE("the new value inserted ",ans.val);
        		mapIter=grouper.begin();
        		free(data);
        	}
        	dbgnQEFnc();
        };

        ~Aggregate(){
        dbgnQEFn();
    	inp=NULL;
    	if(valueP!=NULL)
    		free(valueP);
    	if(groupP!=NULL)
    		free(groupP);
    	grouper.clear();
    	 dbgnQEFnc();
        };

        RC getNextTuple(void *data){
        	if(!valid)return QE_EOF;
        	 dbgnQEFn();

        	if(mapIter!=grouper.end())
        	{
        		Answer ans=mapIter->second;
        		copyFinalAns(data,ans,mapIter->first);
        		 mapIter++;
        	}
        	else
        	   return QE_EOF;

        	 dbgnQEFnc();
        	 return 0;
        };
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
};

#endif

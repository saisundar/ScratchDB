#ifndef _qe_h_
#define _qe_h_

#include <limits>
#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan
# define FLTMAX std::numeric_limits<float>::max();
# define FLTMIN std::numeric_limits<float>::min();

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
	    AggregateOp opAg;
	    bool valid;
	    vector<Attribute> attrs;
	    void* valueP;
	    Answer ans;
	    bool groupBy;
	    bool done;

	    bool isValidAttr();
	    RC getAttrAddr(void* data);
	    RC updateAns(float diff);
	    float compareAttr();
	    RC copyFinalAns(void* data);

        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
        ){
        	dbgnQEFn();
        	inp=input;
        	attr=aggAttr;
        	opAg=op;
        	valid=false;
        	valid=isValidAttr();
        	done=false;
        	inp->getAttributes(attrs);
        	valid=isValidAttr();
        	dbgnQE("valid",valid);
        	groupBy=false;
        	if(opAg==0)
        		{
        		ans.val=FLTMAX;
        		dbgnQE("val initialised to max",ans.val);
        		}
        	else if(opAg==1)
        		{
        		ans.val=FLTMIN;
        		dbgnQE("val initialised to min",ans.val);
        		}

            dbgnQEFnc();
        };

        // Extra Credit
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  Attribute gAttr,                              // The attribute over which we are grouping the tuples
                  AggregateOp op                                // Aggregate operation
        ){};

        ~Aggregate(){
        dbgnQEFn();
    	inp=NULL;
    	if(valueP!=NULL)
    		free(valueP);
    	 dbgnQEFnc();
        };

        RC getNextTuple(void *data){
        	if(!valid||done)return QE_EOF;
        	 dbgnQEFn();
        	float diff;

        	while(inp->getNextTuple(data)!=QE_EOF)
        	{
        		getAttrAddr(data);
        		diff=compareAttr();
        		updateAns(diff);
        	}

        	if(!groupBy)
        	{
        		copyFinalAns(data);
        		dbgnQEFnc();
        		done=true;
        		return 0;
        	}

        	 dbgnQEFnc();
        	 return 0;
        };
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
};

#endif

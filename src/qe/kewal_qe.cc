#include "qe.h"

class INLJoin : public Iterator {
	// Index Nested-Loop join operator
public:
	Iterator *leftIn;
	IndexScan *rightIn;
	Condition condition;

	void* leftData = NULL;
	void* rightData = NULL;
	void* lhsValue = NULL;
	bool errorFlag;

	INLJoin(Iterator *leftIn,                               // Iterator of input R
			IndexScan *rightIn,                             // IndexScan Iterator of input S
			const Condition &condition,                     // Join condition
			const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
	){

		this->leftIn = leftIn;
		this->rightIn = rightIn;
		this->condition = condition;

		leftData = malloc(1024);
		rightData = malloc(1024);
		lhsValue = malloc(108);

		if(condition.bRhsIsAttr)
			errorFlag = false;
		else
			errorFlag = true;

	};

	~INLJoin(){
		free(leftData);
		free(rightData);
		free(lhsValue);
	};

	RC getNextTuple(void *data){
		if(errorFlag)return QE_EOF;
		bool matched = false;
		static int rightInScans = 0;
		while(!matched){
			if(leftData == NULL || rightIn->getNextTuple(rightData) == QE_EOF){

				if(condition.op == NE_OP && rightInScans%2 == 1 && rightIn->getNextTuple(rightData) == QE_EOF){
					rightIn->setIterator(lhsValue, NULL, false, true);
					continue;
				}

				if(leftIn->getNextTuple(leftData) == QE_EOF)
					return QE_EOF;
				else{
					// Find value of lhsAttribute in leftData
					int rc = readAttribute(condition.lhsAttr, (BYTE*)leftData, (BYTE*)lhsValue, leftIn);
					assert(rc!=-1);

					// Reset rightIn
					setCondition(lhsValue);
				}

			}
			else{
				// Found a join here, output one tuple here
				vector<Attribute> attrs;
				leftIn->getAttributes(attrs);
				int offset = copyTuple((BYTE*)data, (BYTE*)leftData, attrs);
				assert(offset!=-1);
				rightIn->getAttributes(attrs);
				copyTuple((BYTE*)data+offset, (BYTE*)rightData, attrs);
				matched = true;
			}
		}
		return 0;
	};

	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const{
		leftIn->getAttributes(attrs);
		vector<Attribute> &attrsRightIn;
		rightIn->getAttributes(attrsRightIn);
		std::vector<Attribute>::const_iterator it = attrsRightIn.begin();
		while(it != attrsRightIn.end()){
			attrs.push_back(*(it));
		}

	};

	void setCondition(void* lhsValue){
		int lowKeyInclusive = false;
		int highKeyInclusive = false;
		void* lowKey = NULL;
		void* highKey = NULL;

		if(condition.op == EQ_OP){
			lowKeyInclusive = true;
			highKeyInclusive = true;
			lowKey = highKey = lhsValue;
		}

		if(condition.op == EQ_OP){
			lowKeyInclusive = true;
			lowKey = NULL;
			highKey = lhsValue;
		}

		if(condition.op == LE_OP){
			lowKeyInclusive = true;
			highKeyInclusive = true;
			highKey = lhsValue;
		}

		if(condition.op == GE_OP){
			lowKeyInclusive = true;
			highKeyInclusive = true;
			lowKey = lhsValue;
		}

		if(condition.op == GT_OP){
			highKeyInclusive = true;
			lowKey = lhsValue;
		}

		if(condition.op == LT_OP){
			lowKeyInclusive = true;
			highKey = lhsValue;
		}
		rightIn->setIterator(lowKey, highKey, lowKeyInclusive, highKeyInclusive);
	}

};

class NLJoin : public Iterator {
	// Nested-Loop join operator
public:

	Iterator *leftIn;
	TableScan *rightIn;
	Condition condition;

	void* leftData = NULL;
	void* rightData = NULL;
	void* lhsValue = NULL;

	bool errorFlag;

	NLJoin(Iterator *leftIn,                             // Iterator of input R
			TableScan *rightIn,                           // TableScan Iterator of input S
			const Condition &condition,                   // Join condition
			const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
	){
		this->leftIn = leftIn;
		this->rightIn = rightIn;
		this->condition = condition;

		leftData = malloc(1024);
		rightData = malloc(1024);
		lhsValue = malloc(108);

		if(condition.bRhsIsAttr)
			errorFlag = false;
		else
			errorFlag = true;

	};
	~NLJoin(){
		free(leftData);
		free(rightData);
		free(lhsValue);
	};

	RC getNextTuple(void *data){
		if(errorFlag)return QE_EOF;
		bool matched = false;
		while(!matched){
			if(leftData == NULL || rightIn->getNextTuple(rightData) == QE_EOF){

				if(leftIn->getNextTuple(leftData) == QE_EOF)
					return QE_EOF;
				else{
					// Find value of lhsAttribute in leftData

					int rc = readAttribute(condition.lhsAttr, (BYTE*)leftData, (BYTE*)lhsValue, leftIn);
					assert(rc!=-1);
					rightIn->setIterator(condition.rhsAttr,condition.op,lhsValue);
				}

			}
			else{
				// Found a join here, output one tuple here
				vector<Attribute> attrs;
				leftIn->getAttributes(attrs);
				int offset = copyTuple((BYTE*)data, (BYTE*)leftData, attrs);
				assert(offset!=-1);
				rightIn->getAttributes(attrs);
				copyTuple((BYTE*)data+offset, (BYTE*)rightData, attrs);
				matched = true;
			}
		}
		return 0;
	};

	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const{
		leftIn->getAttributes(attrs);
		vector<Attribute> &attrsRightIn;
		rightIn->getAttributes(attrsRightIn);
		std::vector<Attribute>::const_iterator it = attrsRightIn.begin();
		while(it != attrsRightIn.end()){
			attrs.push_back(*(it));
		}

	};
};


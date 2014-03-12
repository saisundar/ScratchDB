/*
 * kewal_qe.cc
 *
 *  Created on: Mar 11, 2014
 *      Author: Kewal
 */


#include "qe.h"

// CHANGES IN qe.h
class Project : public Iterator {
	// Projection operator
public:

	vector<Attribute> lowerLevelDescriptor;
	vector<Attribute> currentLevelDescriptor;
	vector<string> attrNames;
	Iterator *input;
	BYTE* lowerLevelData;

	Project(Iterator *input,                            // Iterator of input R
			const vector<string> &attrNames);           // vector containing attribute names

	RC getNextTuple(void *data);
	void getAttributes(vector<Attribute> &attrs) const;

	int readAttribute(const string &attributeName, BYTE * inputData, BYTE* outputData);

};

Project :: ~Project()
{
	free(lowerLevelData);
}

int Project :: readAttribute(const string &attributeName, BYTE * inputData, BYTE* outputData){

	INT32 type2Length = 0;
	bool found=false;

	std::vector<Attribute>::const_iterator it = lowerLevelDescriptor.begin();
	while(it != lowerLevelDescriptor.end() && !found)
	{
		if((it->name).compare(attributeName)==0)
		{
			found = true;
			switch(it->type){
			case 0:
				memcpy(outputData,inputData,4);
				return 4;
			case 1:
				memcpy(outputData,inputData,4);
				return 4;
			case 2:
				type2Length = *((INT32 *)inputData);
				memcpy(outputData,inputData,4 + type2Length);
				return (4 + type2Length);
			default:
				break;
			}
		}
		else{
			switch(it->type){
			case 0:
				inputData += 4;
				break;
			case 1:
				inputData += 4;
				break;
			case 2:
				type2Length = *((INT32 *)inputData);
				inputData += ( 4 + type2Length);
				break;
			default:
				break;
			}
		}
		++it;
	}
	return -1;
}

RC Project :: getNextTuple(void *data) {
	BYTE* outputData = (BYTE*) data;
	if(input->getNextTuple(lowerLevelData)==QE_EOF)
		return QE_EOF;
	std::vector<Attribute>::const_iterator it = currentLevelDescriptor.begin();
	for(unsigned int i=0; i<currentLevelDescriptor.size(); i++){
		int offset = readAttribute(currentLevelDescriptor[i].name,lowerLevelData,outputData);
		outputData += offset;
	}
	return 0;
}


Project :: Project(Iterator *input, const vector<string> &attrNames){
	this->input = input;
	this->attrNames = attrNames;
	lowerLevelDescriptor.clear();
	lowerLevelData = malloc(1024);
	input->getAttributes(this->lowerLevelDescriptor);
	getAttributes(currentLevelDescriptor);
}

void Project :: getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	std::vector<Attribute>::const_iterator it = lowerLevelDescriptor.begin();
	for(unsigned int i=0; i<attrNames.size(); i++){
		while(it != lowerLevelDescriptor.end())
		{
			if((it->name).compare(attrNames[i])==0)
			{
				attrs.push_back(*(it));
			}
			++it;
		}
		if(it == lowerLevelDescriptor.end()){
			// Throw Error
		}
	}
}

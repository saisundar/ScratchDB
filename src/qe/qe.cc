
#include "qe.h"

bool Filter::isValidAttr(){
	INT32 i;
	dbgnQEFn();
	bool lhs=false;
	bool rhs=false;
	AttrType l,r;
	INT16 lLength;

	for(i=0;i<attrs.size();i++)
		{
		dbgnQE("name of atribute",attrs[i].name);
		dbgnQE("name of the lhs attrute",cond.lhsAttr);
		dbgnQE("attribute length",attrs[i].length);
		if(attrs[i].length!=0 && attrs[i].name.compare(cond.lhsAttr)==0)
		{
			lLength=attrs[i].length;
			valueP=malloc(attrs[i].length+4);
			l=attrs[i].type;
			type=l;
			dbgnQE("LHS attirbute found.number",i);
			lhs=true;
		}
		else if(cond.bRhsIsAttr && attrs[i].name.compare(cond.rhsAttr)==0)
		{

			r=attrs[i].type;
			rhs=true;
			dbgnQE("RHS attirbute found.number",i);
		}
		}

	if(!cond.bRhsIsAttr && lhs){

		r=cond.rhsValue.type;
		rhs=true;
		memcpy(valueP,cond.rhsValue.data,lLength);

		if(type==2)
		{
			INT32 length = *((INT32*)cond.rhsValue.data);
			*((BYTE*)valueP + length+4)=0;
			dbgnQE("string value",valueP+4);
		}
	}
	dbgnQEFnc();
	dbgnQE("lhs",lhs);
	dbgnQE("rhs",rhs);
	dbgnQE("l==r",l==r);
	return(lhs && rhs && l==r);
}

bool Filter::returnRes(int diff)
{
	dbgnQEFn();
	if(diff==0)
		{
		dbgnQEFnc();
		return((cond.op==EQ_OP)||(cond.op==LE_OP)||(cond.op==GE_OP));
		}
	if(diff<0)
		{
		dbgnQEFnc();
		return((cond.op==LT_OP)||(cond.op==LE_OP)||(cond.op==NE_OP));
		}
	dbgnQEFnc();
	return((cond.op==GT_OP)||(cond.op==GE_OP)||(cond.op==NE_OP));
}

bool Filter::evaluateCondition(void * temp)
{
	int diff;
	bool result=false;
	dbgnQEFn();
	dbgnQE("type of attribute tobe compared",type);
	switch(type)
	{
	case 0:
		diff=intVal(temp)-intVal(valueP);
		dbgnQE("Comparing the integers here !","");
		dbgnQE(intVal(temp),intVal(valueP));
		break;
	case 1:
		dbgnQE("Comparing the floats here !","");
		dbgnQE((*((float *)temp)),(*((float *)valueP)));
		if(*((float *)temp)>*((float *)valueP))
			diff=1;
		else if(*((float *)temp)==*((float *)valueP))
			diff=0;
		else
			diff=-1;
		break;
	case 2:
		INT32 num=*((INT32*)valueP);
		diff= strncmp((char *)temp+4,(char *)valueP+4,num);
		dbgnQE("Comparing the strings here !","");
		dbgnQE((char*)temp+4,(char*)valueP+4);
		break;
	}
	result=returnRes(diff);
	dbgnQE("result of comparison",result==1);
	dbgnQEFnc();
	return(result);
}
RC Filter::getRHSAddr(void* data)
{
	BYTE * iterData=(BYTE *)data;
	dbgnQEFn();

	bool found=false;
	std::vector<Attribute>::const_iterator it = attrs.begin();
	INT32 num = 0;
	for(;it != attrs.end() && !found;it++)
	{
		dbgnQE("type",it->type);
		switch(it->type){
		case 0:
		case 1:
			if((it->name).compare(cond.lhsAttr)==0)
			{
				dbgnQE("found the ttribute",it->name);
				memcpy(valueP,iterData,4);
				found=true;
			}
			iterData=iterData+4;
			break;
		case 2:
			num = *((INT32 *)iterData);
			if((it->name).compare(cond.lhsAttr)==0)
			{
				dbgnQE("found the ttribute",it->name);
				memcpy(valueP,iterData,4+num);
				found=true;
			}
			iterData=iterData+4+num;
			break;
		default:
			break;
		}
	}
	dbgnQEFnc();
	if(!found)
		return -1;
	return 0;
}
 BYTE* Filter::getLHSAddr(void* data){
	 BYTE * iterData=(BYTE*)data;
	 dbgnQEFn();
	 INT32 num;
	 std::vector<Attribute>::const_iterator it = attrs.begin();
	  for(;it != attrs.end();it++)
	 {
		 dbgnQE("type",it->type);
		 switch(it->type){
		 case 0:
		 case 1:
			 if((it->name).compare(cond.lhsAttr)==0)
			 {
				 dbgnQE("found the ttribute",it->name);
				return iterData;
			 }
			 iterData=iterData+4;
			 break;
		 case 2:
			 num = *((INT32 *)iterData);
			 if((it->name).compare(cond.lhsAttr)==0)
			 {
				 dbgnQE("found the ttribute",it->name);
				 return iterData;
			 }
			 iterData=iterData+4+num;
			 break;
		 default:
			 break;
		 }
	 }
	 return NULL;
}

bool Aggregate::isValidAttr()
{
	INT32 i;
	dbgnQEFn();
	bool attrFound=false,groupFound=false;
	dbgnQE("siez of attrs",attrs.size());
	for(i=0;i<attrs.size();i++)
	{
		dbgnQE("name of atribute",attrs[i].name);
		dbgnQE("name of the aggregating attrute",attr.name);
		dbgnQE("attribute length",attrs[i].length);
		if(attrs[i].length!=0 && attrs[i].name.compare(attr.name)==0)
		{
			valueP=malloc(attrs[i].length+4);
			dbgnQE("Aggregate attirbute found.number",i);
			attrFound=true;
			if(attrFound && groupFound)break;
		}
		if((groupBy && attrs[i].length!=0 && attrs[i].name.compare(groupAttr.name)==0)||(!groupBy))
		{
			groupFound=true;
			groupP=malloc(attrs[i].length+4);
			dbgnQE("group attribute found.number",i);
			if(attrFound && groupFound)break;
		}
	}
	dbgnQEFnc();
	return(i<attrs.size());
}

RC Aggregate::getAttrAddr(void* data)
{
	BYTE * iterData=(BYTE *)data;
	dbgnQEFn();

	bool attrFound=false,groupFound=false;
	std::vector<Attribute>::const_iterator it = attrs.begin();
	INT32 num = 0;
	dbgnQE("group attribyte",groupAttr.name);
	for(;it != attrs.end() && !(attrFound && groupFound);it++)
	{
		dbgnQE("type",it->type);
		dbgnQE("name",it->name);
		switch(it->type){
		case 0:
		case 1:
			if((it->name).compare(attr.name)==0)
			{
				dbgnQE("found the ttribute",it->name);
				memcpy(valueP,iterData,4);
				attrFound=true;
			}
			else if((it->name).compare(groupAttr.name)==0)
			{
	    		dbgnQE("found the group attribute",it->name);
				memcpy(groupP,iterData,4);
				groupFound=true;
			}
			iterData=iterData+4;
			break;
		case 2:
			num = *((INT32 *)iterData);
			if((it->name).compare(attr.name)==0)
			{
				dbgnQE("found the ttribute",it->name);
				memcpy(valueP,iterData,4+num);
				*((BYTE*)valueP+4+num)=(BYTE)0;
				attrFound=true;
			}
			else if((it->name).compare(groupAttr.name)==0)
			{
				dbgnQE("found the group attribute",it->name);
				memcpy(groupP,iterData,4+num);
				*((BYTE*)groupP+4+num)=(BYTE)0;
				groupFound=true;
			}
			iterData=iterData+4+num;
			break;
		default:
			break;
		}
	}
	dbgnQEFnc();
	if(!(attrFound && groupFound))
		return -1;
	return 0;
}


//returns >0 if ans>valueP
//		==0 if ans==valueP
//		  <0 if ans<valueP
float Aggregate::compareAttr(Answer ans)
{
	float diff;
	dbgnQEFn();
	dbgnQE("type of attribute to be compared",attr.type);
	switch(attr.type)
	{
	case 0:
		if(INT32(ans.val) > intVal(valueP))
			diff =1;
		else if(INT32(ans.val) < intVal(valueP))diff =-1;
		else diff=0;
		dbgnQE("Comparing the integers here !","");
		dbgnQE("existing ans","new value from record");
		dbgnQE((INT32(ans.val)),intVal(valueP));
		break;
	case 1:
		dbgnQE("Comparing the floats here !","");
		dbgnQE("existing ans","new value from record");
		dbgnQE(ans.val,(*((float *)valueP)));
		if(ans.val > *((float *)valueP))
		diff=1;
		else if(ans.val < *((float *)valueP))
			diff=-1;
		else diff=0;
		break;
	case 2:
		dbgnQE("oops hitting varchar in comparison..some error","");
		break;
	}

	dbgnQE("result of comparison",diff);
	dbgnQEFnc();
	return(diff);
}

Answer Aggregate::getGroupAns()
{

	string groupString((char*)groupP+4);
	tempKey.attr=groupAttr;
	switch(groupAttr.type)
	{
	case 0:
		{INT32 temp;
		memcpy(&temp,groupP,4);
		dbgnQE("int group atribute value",temp);
		tempKey.num=temp;
		break;
		}
	case 1:
		{float temp;
		memcpy(&temp,groupP,4);
		dbgnQE("float group atribute value",temp);
		tempKey.num=temp;
		break;
		}
	case 2:
		{
		INT32 temp;
		memcpy(&temp,groupP,4);
		string groupString((char*)groupP+4);
		tempKey.name=groupString;
		tempKey.num=temp;
		dbgnQE("string group atribute value",temp);
		break;
		}
	default:
		break;
	}

	if(grouper.find(tempKey)!=grouper.end())
	{
		dbgnQE(" valuea lready in map..retrieiving it","");
		return(grouper[tempKey]);
	}
	dbgnQE(" valuea not in map..making it","");

	Answer ans;
	setExtremeAns(ans);
	return ans;
}

RC Aggregate::updateAns(Answer &ans,float diff)
{
	dbgnQEFn();
	ans.count++;
	dbgnQE("items count",ans.count);
	dbgnQE("operation",opAg);
	switch(opAg)
	{
	case 0:		//min

		if(diff>0)
		{
			if(attr.type==0)
			{
				dbgnQE("older answer",ans.val);
				INT32 temp;
				memcpy(&temp,valueP,4);
				dbgnQE("new value",temp);
				dbgnQE("hence updatng","");
				ans.val=(float)temp;
			}
			else if(attr.type==1)
			{
				dbgnQE("older answer",ans.val);
				dbgnQE("new value",(*((float *)valueP)));
				memcpy(&ans.val,valueP,4);
			}
			else
				dbgnQE("0:attr tpye is actually equal to ",attr.type);
		}
		break;
	case 1:		//max
		if(diff<0)
		{
			if(attr.type==0)
			{
				dbgnQE("older answer",ans.val);
				INT32 temp;
				memcpy(&temp,valueP,4);
				dbgnQE("new value",temp);
				dbgnQE("hence updatng","");
				ans.val=(float)temp;
			}
			else if(attr.type==1)
			{
				dbgnQE("older answer",ans.val);
				dbgnQE("new value",(*((float *)valueP)));
				memcpy(&ans.val,valueP,4);
			}
			else
				dbgnQE("1:attr tpye is actually equal to ",attr.type);
		}
		break;
	case 2:		//sum
	case 3:		//avg
		if(attr.type==0)
		{
			dbgnQE("older answer",ans.val);
			INT32 temp;
			memcpy(&temp,valueP,4);
			dbgnQE("new value",temp);
			dbgnQE("hence updating","");
			ans.val+=(float)temp;
		}
		else if(attr.type==1)
		{
			dbgnQE("older answer",ans.val);
			dbgnQE("new value",(*((float *)valueP)));
			ans.val+=*((float *)valueP);
		}
		else
			dbgnQE("2,3:attr tpye is actually equal to ",attr.type);
		break;
	default:
		break;
	}
	dbgnQEFnc();
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	Attribute attr;
	dbgnQEFn();
	if(groupBy)
		attrs.push_back(groupAttr);

	if(opAg==2)
	{
		attr.type=TypeReal;
	}
	else if(opAg==4)
		attr.type=TypeInt;
	else
	{
		attr.type=this->attr.type;
	}
	attr.length=4;

	switch(opAg)
	{
	case 0:
		{
		string temp="MIN";

		temp=temp+"(";
		temp=temp+this->attr.name;
		temp=temp+")";
		attr.name=temp;
		break;
		}
	case 1:
		{string temp="MAX";
		temp=temp+"(";
		temp=temp+this->attr.name;
		temp=temp+")";
		attr.name=temp;
		break;
		}
	case 2:
		{string temp="AVG";
		temp=temp+"(";
		temp=temp+this->attr.name;
		temp=temp+")";
		attr.name=temp;
		break;
		}
	case 3:
		{string temp="SUM";
		temp=temp+"(";
		temp=temp+this->attr.name;
		temp=temp+")";
		attr.name=temp;
		break;
		}
	case 4:
		{string temp="COUNT";
		temp=temp+"(";
		temp=temp+this->attr.name;
		temp=temp+")";
		attr.name=temp;
		break;
		}
	default:
		break;
	}
	dbgnQE("attribute being pushed back",attr.name);
	attrs.push_back(attr);
	dbgnQEFnc();
}

RC Aggregate::copyFinalAns(void* data,Answer ans,mapKey tempKey)
{

	dbgnQEFn();
	BYTE* iterData=(BYTE*)data;
	INT32 len,i;
	if(groupBy)
	{
		switch(groupAttr.type)
		{
		case 0:
			memcpy(iterData,&tempKey.num,4);
			dbgnQE("value copied into data",tempKey.num);
			iterData+=4;
			break;
		case 1:
			memcpy(iterData,&tempKey.val,4);
			dbgnQE("float value copied",tempKey.val);
			iterData+=4;
			break;
		case 2:
			len=tempKey.num;
			memcpy(iterData,&len,4);
			iterData+=4;
			for(i=0;i<len;i++,iterData++)
				*iterData=tempKey.name[i];
			dbgnQE("string value copied",tempKey.name);
			break;
		default:
			break;
		}
	}
	switch(opAg)
	{
	case 0:
	case 1:
	case 2:
		if(attr.type==0)
		{
			dbgnQE("max/min/sum value",ans.val);
			INT32 temp=(INT32)ans.val;
			memcpy(iterData,&temp,4);
		}
		else if(attr.type==1)
		{
			dbgnQE("max/min/sum value",ans.val);
			memcpy(iterData,&ans.val,4);
		}
		break;
	case 3:

		ans.val=ans.val/ans.count;
		dbgnQE("average",ans.val);
		memcpy(iterData,&ans.val,4);
		break;
	case 4:
		dbgnQE("count",ans.count);
		memcpy(iterData,&ans.count,4);
		break;
	default:
		break;
	}
	dbgnQEFnc();
	return 0;
}


void Aggregate::setExtremeAns(Answer &ans)
{

	if(attr.type==1)
	{if(opAg==0)
	{
		ans.val=FLTMAX;
		dbgnQE("val initialised to max",ans.val);
	}
	else if(opAg==1)
	{
		ans.val=-FLTMAX;
		dbgnQE("val initialised to min",ans.val);
	}
	}
	else if(attr.type==0)
	{if(opAg==0)
	{
		ans.val=INTMAX;
		dbgnQE("val initialised to max",ans.val);
	}
	else if(opAg==1)
	{
		ans.val=-INTMAX;
		dbgnQE("val initialised to min",ans.val);
	}
	}

}
// ... the rest of your implementations go here

int Project :: readAttribute(const string &attributeName, BYTE * inputData, BYTE* outputData){

	INT32 type2Length = 0;
	bool found=false;
	dbgnQEFn();
	std::vector<Attribute>::const_iterator it = lowerLevelDescriptor.begin();
	while(it != lowerLevelDescriptor.end() && !found)
	{
		if((it->name).compare(attributeName)==0)
		{
			found = true;
			switch(it->type){
			case 0:
				memcpy(outputData,inputData,4);
				dbgnQEFnc();
				return 4;
			case 1:
				memcpy(outputData,inputData,4);
				dbgnQEFnc();
				return 4;
			case 2:
				type2Length = *((INT32 *)inputData);
				memcpy(outputData,inputData,4 + type2Length);
				dbgnQEFnc();
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
	dbgnQEFnc();
	return -1;
}

RC Project :: getNextTuple(void *data) {
	BYTE* outputData = (BYTE*) data;
	dbgnQEFn();
	if(!valid){
		dbgnQE("invalid project attributes","");
		return QE_EOF;
	}
	if(input->getNextTuple(lowerLevelData)==QE_EOF)
	{
		dbgnQEFnc();
		return QE_EOF;
	}
	std::vector<Attribute>::const_iterator it = projAttrs.begin();
	for(unsigned int i=0; i<projAttrs.size(); i++){
		int offset = readAttribute(projAttrs[i].name,lowerLevelData,outputData);
		outputData += offset;
	}
	dbgnQEFnc();
	return 0;
}


Project :: Project(Iterator *input, const vector<string> &attrNames){
	dbgnQEFn();
	valid=true;
	this->input = input;
	this->attrNames = attrNames;
	lowerLevelDescriptor.clear();
	lowerLevelData = (BYTE*)malloc(1024);
	input->getAttributes(this->lowerLevelDescriptor);
	valid=isValidAttr();
	dbgnQEFnc();
}

void Project :: getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	dbgnQEFn();
	attrs=projAttrs;
	dbgnQEFnc();
}

bool Project::isValidAttr(){
	valid = false;
	std::vector<Attribute>::const_iterator it = lowerLevelDescriptor.begin();
		for(unsigned int i=0; i<attrNames.size(); i++){
			while(it != lowerLevelDescriptor.end())
			{
				if((it->name).compare(attrNames[i])==0)
				{
					projAttrs.push_back(*(it));
					it = lowerLevelDescriptor.begin();
					break;
				}
				++it;
			}
			if(it == lowerLevelDescriptor.end()){

			 return false;
			}
	}
	return true;
}

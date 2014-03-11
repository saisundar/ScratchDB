
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
// ... the rest of your implementations go here

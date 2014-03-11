/*
 * kewal_qe.cc
 *
 *  Created on: Mar 11, 2014
 *      Author: Kewal
 */


#include "qe.h"

class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                            // Iterator of input R
                const vector<string> &attrNames){};           // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data) {return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};

Project :: Project(Iterator *input, const vector<string> &attrNames){

};

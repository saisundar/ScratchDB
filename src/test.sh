#!/bin/bash
unzip $1.zip
cd $1
cd codebase
cd rbf
make clean
make
./rbftest
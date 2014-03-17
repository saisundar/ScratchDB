CODEROOT = "$(realpath $(dir $(lastword $(MAKEFILE_LIST))))"
#CC = gcc
#CC = g++

LDLIBS = -lreadline

UNAME := $(shell uname)
ifeq ($(UNAME),Linux)
     CC = g++
endif

ifeq ($(UNAME),Darwin)
     CC = /opt/local/bin/g++-mp-4.7
     CXX = /opt/local/bin/g++-mp-4.7
endif

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
CPPFLAGS = -std=c++0x -Wall -I$(CODEROOT) -DDATABASE_FOLDER=\"$(CODEROOT)/cli/\"  -g  # with debugging info 

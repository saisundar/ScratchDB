This is a functional database system with beta cli.

The cli has been adapted from https://grape.ics.uci.edu/wiki/asterix/wiki/cs222-2014-winter-command-line-interface .

The project consists of four layers

1) PFM - page file manager - responsible for all updates to the database- which happens in units of pages.
2) Record file manager - interprets files as collection of records
3) IX manager - Index manager- implements a b tree index , and inteprets each page as a node of the btree 
4) RM - relation Manager - responsible for providing apis related to relations.
   Encapuslates the underlying RBF layer and the IX layer.
5) CLI - still in beta state . provides a query language .

The above was implemented for course CS222 , Winter 2014, UCI


















- Modify the "CODEROOT" variable in makefile.inc to point to the root
  of your code base

- Implement the Record-based Files (RBF) Component:

   Go to folder "rbf" and type in:

    make clean
    make
    ./rbftest

   The program should work.  But it does nothing.  You are supposed to
   implement the API of the paged file manager defined in pfm.h and some
   of the methods in rbfm.h as explained in the project description.

- By default you should not change those functions of the PagedFileManager,
  FileHandle, and RecordBasedFileManager classes defined in rbf/pfm.h and rbf/rbfm.h.
  If you think some changes are really necessary, please contact us first.

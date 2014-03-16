
include ../makefile.inc

CXXFLAGS =-std=c++11

all: libcli.a clitest start

# lib file dependencies
libcli.a: libcli.a(cli.o)  # and possibly other .o files

# c file dependencies
cli.o: cli.h

clitest.o: cli.h

start.o: cli.h

# binary dependencies
clitest: clitest.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a
start: start.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a $(CODEROOT)/rm/librm.a
$(CODEROOT)/ix/libix.a:
	$(MAKE) -C $(CODEROOT)/ix libix.a


.PHONY: $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a $(CODEROOT)/ix/libix.a
$(CODEROOT)/qe/libqe.a:
	$(MAKE) -C $(CODEROOT)/qe libqe.a

.PHONY: clean
clean:
	-rm clitest start *.a *.o *~
	$(MAKE) -C $(CODEROOT)/rbf clean
	$(MAKE) -C $(CODEROOT)/rm clean
	$(MAKE) -C $(CODEROOT)/ix clean	
	$(MAKE) -C $(CODEROOT)/qe clean	

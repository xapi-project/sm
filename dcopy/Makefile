DESTDIR ?=
COMPAT_PREFIX ?= /opt/xensource/libexec
COMPAT_DEBUGDIR ?= /opt/xensource/debug
DEBUG_LIBEXECDIR ?= /usr/libexec/sm/debug

OPTS := -D _GNU_SOURCE -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -Wall

SRC := dcopy.c
SRC += atomicio.c

BIN := dcopy

all: dcopy tp

dcopy: dcopy.c
	$(CC) $(OPTS) $(SRC) -o $(BIN)

tp: tp.c
	$(CC) $(OPTS) tp.c -o tp

test: tp
	./test.sh

install: install_dcopy install_tp

install_dcopy: dcopy
	install -D -m 755 $^ $(DESTDIR)$(DEBUG_LIBEXECDIR)/$^
	mkdir -p $(DESTDIR)$(COMPAT_PREFIX)
	ln -sf $(DEBUG_LIBEXECDIR)/$^ $(DESTDIR)$(COMPAT_PREFIX)/$^

install_tp: tp
	install -D -m 755 $^ $(DESTDIR)$(DEBUG_LIBEXECDIR)/$^
	mkdir -p $(DESTDIR)$(COMPAT_DEBUGDIR)
	ln -sf $(DEBUG_LIBEXECDIR)/$^ $(DESTDIR)$(COMPAT_DEBUGDIR)/$^

clean: 
	rm -f dcopy tp *.o source_img dest_img

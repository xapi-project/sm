PREFIX ?= /opt/xensource/libexec
DESTDIR ?= 
DEBUGDIR ?= /opt/xensource/debug


OPTS := -Wall -std=gnu99

SRC := linstor-monitord.c

BIN := linstor-monitord

all: daemon

daemon: linstor-monitord.c
	$(CC) $(OPTS) $(SRC) -o $(BIN)

install: linstor-monitord
	mkdir -p $(DESTDIR)$(PREFIX)
	install -m 755 $^ $(DESTDIR)$(PREFIX)

clean: 
	rm -f linstor-monitord

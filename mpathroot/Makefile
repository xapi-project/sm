INITDIR ?= /etc/rc.d/init.d
SM_DEST_DEFAULT = /opt/xensource/sm/
SM_DEST ?= /opt/xensource/sm/
DESTDIR ?= 

.PHONY: all
all:

.PHONY: install
install: mpathroot.init
	cp mpathroot.init $(DESTDIR)$(INITDIR)/mpathroot
	sed -i "s#\(SM_DEST \?\)=\( \?['\"]\)$(SM_DEST_DEFAULT)#\1=\2$(SM_DEST)#" $(DESTDIR)$(INITDIR)/*
	chmod +x $(DESTDIR)$(INITDIR)/mpathroot

.PHONY: clean
clean:


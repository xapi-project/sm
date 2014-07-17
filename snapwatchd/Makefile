XS_INCLUDE ?= /usr/include

SM_DEST_DEFAULT := /opt/xensource/sm/
SM_DEST ?= /opt/xensource/sm/
PREFIX ?= $(SM_DEST)snapwatchd
INITDIR ?= /etc/rc.d/init.d
DESTDIR ?= 

.PHONY: install
install: xslib.py snapwatchd snapdebug.py
	mkdir -p $(DESTDIR)$(PREFIX)
	install -m 644 $^ $(DESTDIR)$(PREFIX)
	find $(DESTDIR)$(PREFIX) -type f | xargs sed -i "s#\(SM_DEST \?\)=\( \?['\"]\)$(SM_DEST_DEFAULT)#\1=\2$(SM_DEST)#"
	chmod +x $(DESTDIR)$(PREFIX)/snapwatchd
	chmod +x $(DESTDIR)$(PREFIX)/xslib.py
	chmod +x $(DESTDIR)$(PREFIX)/snapdebug.py
	mkdir -p $(DESTDIR)$(INITDIR)
	cp snapwatchd.init $(DESTDIR)$(INITDIR)/snapwatchd
	find $(DESTDIR)$(INITDIR) -type f | xargs sed -i "s#\(SM_DEST \?\)=\( \?['\"]\)$(SM_DEST_DEFAULT)#\1=\2$(SM_DEST)#"
	chmod +x $(DESTDIR)$(INITDIR)/snapwatchd

.PHONY: clean
clean:
	rm -f xslib.pyc xslib.pyo snapdebug.pyc snapdebug.pyo


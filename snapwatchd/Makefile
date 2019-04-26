XS_INCLUDE ?= /usr/include

PREFIX ?= /opt/xensource/sm/snapwatchd
SYSTEMD_SERVICE_DIR ?= /usr/lib/systemd/system
DESTDIR ?= 

.PHONY: install
install: xslib.py snapwatchd snapdebug.py
	mkdir -p $(DESTDIR)$(PREFIX)
	install -m 644 $^ $(DESTDIR)$(PREFIX)
	chmod +x $(DESTDIR)$(PREFIX)/snapwatchd
	chmod +x $(DESTDIR)$(PREFIX)/xslib.py
	chmod +x $(DESTDIR)$(PREFIX)/snapdebug.py
	mkdir -p $(DESTDIR)$(SYSTEMD_SERVICE_DIR)
	cp snapwatchd.service $(DESTDIR)$(SYSTEMD_SERVICE_DIR)/.

.PHONY: clean
clean:
	rm -f xslib.pyc xslib.pyo snapdebug.pyc snapdebug.pyo


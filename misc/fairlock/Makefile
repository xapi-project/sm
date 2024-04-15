CC=gcc
CFLAGS=-I.
OBJ = fairlock.o
LIBEXECDIR := /usr/libexec
UNITDIR := /usr/lib/systemd/system
PYTHONLIBDIR = $(shell python3 -c "import sys; print(sys.path.pop())")

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)

fairlock: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

.PHONY: clean
clean:
	rm -rf fairlock $(OBJ)

.PHONY: install
install: fairlock fairlock@.service
	install -D -m 755 fairlock $(DESTDIR)$(LIBEXECDIR)/fairlock
	install -D -m 644 fairlock@.service $(DESTDIR)$(UNITDIR)/fairlock@.service
	install -D -m 644 fairlock.py $(DESTDIR)$(PYTHONLIBDIR)/fairlock.py
	python3 -m compileall $(DESTDIR)$(PYTHONLIBDIR)/fairlock.py

.PHONY: uninstall
uninstall:
	rm -rf $(DESTDIR)$(LIBEXECDIR)/fairlock
	rm -rf $(DESTDIR)$(UNITDIR)/fairlock@.service
	rm -rf $(DESTDIR)$(PYTHONLIBDIR)/fairlock.py
	rm -rf $(DESTDIR)$(PYTHONLIBDIR)/__pycache__/fairlock.*

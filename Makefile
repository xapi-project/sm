PYLINT=$(shell command -v pylint-3 || echo pylint)
PYTHONLIBDIR = $(shell python3 -c "import sys; print(sys.path.pop())")

SM_DRIVERS := File
SM_DRIVERS += NFS
SM_DRIVERS += EXT
SM_DRIVERS += RawISCSI
SM_DRIVERS += Dummy
SM_DRIVERS += udev
SM_DRIVERS += ISO
SM_DRIVERS += HBA
SM_DRIVERS += LVHD
SM_DRIVERS += LVHDoISCSI
SM_DRIVERS += LVHDoHBA
SM_DRIVERS += SHM
SM_DRIVERS += SMB

# Libraries which have moved in to sm.core
SM_CORE_LIBS := util
SM_CORE_LIBS += scsiutil
SM_CORE_LIBS += mpath_dmp
SM_CORE_LIBS += mpath_cli
SM_CORE_LIBS += xs_errors
SM_CORE_LIBS += iscsi
SM_CORE_LIBS += wwid_conf
SM_CORE_LIBS += lock
SM_CORE_LIBS += flock
SM_CORE_LIBS += f_exceptions
# Add a "pretend" core lib to cover the iscsi differences
SM_CORE_LIBS += libiscsi

# Libraries which remain in drivers/ and get installed in
# /opt/xensource/sm as wrappers around sm.core libs for
# backwards compatibility and can hopefully be one day dropped
SM_COMPAT_LIBS := util
SM_COMPAT_LIBS += scsiutil
SM_COMPAT_LIBS += mpath_dmp
SM_COMPAT_LIBS += mpath_cli
SM_COMPAT_LIBS += xs_errors
SM_COMPAT_LIBS += iscsilib
SM_COMPAT_LIBS += wwid_conf
SM_COMPAT_LIBS += lock
SM_COMPAT_LIBS += flock

# Libraries and other code still maintained in
# drivers/ and installed in /opt/xensource/sm which
# has not yet been moved elsewhere.
SM_LIBS := SR
SM_LIBS += SRCommand
SM_LIBS += VDI
SM_LIBS += BaseISCSI
SM_LIBS += cleanup
SM_LIBS += lvutil
SM_LIBS += lvmcache
SM_LIBS += verifyVHDsOnSR
SM_LIBS += scsi_host_rescan
SM_LIBS += vhdutil
SM_LIBS += lvhdutil
SM_LIBS += cifutils
SM_LIBS += nfs
SM_LIBS += devscan
SM_LIBS += sysdevice
SM_LIBS += mpath_null
SM_LIBS += mpathutil
SM_LIBS += LUNperVDI
SM_LIBS += mpathcount
SM_LIBS += refcounter
SM_LIBS += journaler
SM_LIBS += fjournaler
SM_LIBS += lock_queue
SM_LIBS += ipc
SM_LIBS += srmetadata
SM_LIBS += metadata
SM_LIBS += lvmanager
SM_LIBS += blktap2
SM_LIBS += lcache
SM_LIBS += resetvdis
SM_LIBS += trim_util
SM_LIBS += pluginutil
SM_LIBS += constants
SM_LIBS += cbtutil
SM_LIBS += sr_health_check
SM_LIBS += $(SM_COMPAT_LIBS)

UDEV_RULES = 65-multipath 55-xs-mpath-scsidev 57-usb 58-xapi 99-purestorage
MPATH_DAEMON = sm-multipath
MPATH_CONF = multipath.conf
MPATH_CUSTOM_CONF = custom.conf
SMLOG_CONF = SMlog

SM_XML := XE_SR_ERRORCODES

SM_DEST := /opt/xensource/sm/
DEBUG_DEST := /opt/xensource/debug/
BIN_DEST := /opt/xensource/bin/
MASTER_SCRIPT_DEST := /etc/xensource/master.d/
PLUGIN_SCRIPT_DEST := /etc/xapi.d/plugins/
SM_LIBEXEC := /opt/xensource/libexec/
SM_DATADIR := /usr/share/sm
UDEV_RULES_DIR := /etc/udev/rules.d/
UDEV_SCRIPTS_DIR := /etc/udev/scripts/
SYSTEMD_SERVICE_DIR := /usr/lib/systemd/system/
INIT_DIR := /etc/rc.d/init.d/
MPATH_CONF_DIR := /etc/multipath.xenserver/
MPATH_CUSTOM_CONF_DIR := /etc/multipath/conf.d/
MODPROBE_DIR := /etc/modprobe.d/
EXTENSION_SCRIPT_DEST := /etc/xapi.d/extensions/
LOGROTATE_DIR := /etc/logrotate.d/

SM_STAGING := $(DESTDIR)
SM_STAMP := $(MY_OBJ_DIR)/.staging_stamp

SM_PY_FILES = $(foreach LIB, $(SM_LIBS), drivers/$(LIB).py) $(foreach DRIVER, $(SM_DRIVERS), drivers/$(DRIVER)SR.py)
SM_CORE_PY_FILES = $(foreach LIB, $(SM_CORE_LIBS), libs/sm/core/$(LIB).py) libs/sm/core/__init__.py

.PHONY: build
build:
	make -C dcopy 

.PHONY: precommit
precommit: build
	@ QUIT=0; \
	CHANGED=$$(git status --porcelain $(SM_PY_FILES) $(SM_CORE_PY_FILES) | awk '{print $$2}'); \
	for i in $$CHANGED; do \
		echo Checking $${i} ...; \
		PYTHONPATH=./drivers:./libs:./misc/fairlock:$$PYTHONPATH $(PYLINT) --rcfile=tests/pylintrc $${i}; \
		[ $$? -ne 0 ] && QUIT=1 ; \
	done; \
	if [ $$QUIT -ne 0 ]; then \
		exit 1; \
	fi; \
	echo "Precommit succeeded with no outstanding issues found."


.PHONY: precheck
precheck: build
	PYTHONPATH=./drivers:./libs:./misc/fairlock:$$PYTHONPATH $(PYLINT) --rcfile=tests/pylintrc $(SM_PY_FILES) $(SM_CORE_PY_FILES)
	echo "Precheck succeeded with no outstanding issues found."

.PHONY: install
install: precheck
	mkdir -p $(SM_STAGING)
	$(call mkdir_clean,$(SM_STAGING))
	mkdir -p $(SM_STAGING)$(SM_DEST)
	mkdir -p $(SM_STAGING)$(SM_DEST)/plugins
	mkdir -p $(SM_STAGING)$(UDEV_RULES_DIR)
	mkdir -p $(SM_STAGING)$(UDEV_SCRIPTS_DIR)
	mkdir -p $(SM_STAGING)$(INIT_DIR)
	mkdir -p $(SM_STAGING)$(SYSTEMD_SERVICE_DIR)
	mkdir -p $(SM_STAGING)$(MPATH_CONF_DIR)
	mkdir -p $(SM_STAGING)$(MPATH_CUSTOM_CONF_DIR)
	mkdir -p $(SM_STAGING)$(MODPROBE_DIR)
	mkdir -p $(SM_STAGING)$(LOGROTATE_DIR)
	mkdir -p $(SM_STAGING)$(DEBUG_DEST)
	mkdir -p $(SM_STAGING)$(BIN_DEST)
	mkdir -p $(SM_STAGING)$(MASTER_SCRIPT_DEST)
	mkdir -p $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	mkdir -p $(SM_STAGING)$(EXTENSION_SCRIPT_DEST)
	mkdir -p $(SM_STAGING)/sbin
	# Core libs (including XML error definitions)
	mkdir -p $(SM_STAGING)/$(PYTHONLIBDIR)/sm/core
	install -D -m 644 libs/sm/__init__.py $(SM_STAGING)$(PYTHONLIBDIR)/sm/__init__.py
	for i in $(SM_CORE_PY_FILES); do \
	  install -D -m 644 $$i $(SM_STAGING)$(PYTHONLIBDIR)/sm/core/; \
	done
	mkdir -p $(SM_STAGING)$(SM_DATADIR)
	# This should go in SM_DATADIR but that breaks the unit tests.
	# Leave it next to xs_errors.py until we can fix that.
	for i in $(SM_XML); do \
	  install -D -m 644 libs/sm/core/$$i.xml $(SM_STAGING)$(PYTHONLIBDIR)/sm/core/; \
	done
	# Legacy SM python files
	for i in $(SM_PY_FILES); do \
	  install -m 755 $$i $(SM_STAGING)$(SM_DEST); \
	done
	install -m 755 drivers/plugins/__init__.py \
	  $(SM_STAGING)$(SM_DEST)/plugins/
	install -m 755 drivers/plugins/keymanagerutil.py \
	  $(SM_STAGING)$(SM_DEST)/plugins/
	install -m 644 multipath/$(MPATH_CONF) \
	  $(SM_STAGING)/$(MPATH_CONF_DIR)
	install -m 644 multipath/$(MPATH_CUSTOM_CONF) \
	  $(SM_STAGING)/$(MPATH_CUSTOM_CONF_DIR)
	install -m 755 multipath/sm-multipath \
	  $(SM_STAGING)/$(INIT_DIR)
	install -m 755 multipath/multipath-root-setup \
	  $(SM_STAGING)/$(SM_DEST)
	install -m 644 etc/logrotate.d/$(SMLOG_CONF) \
	  $(SM_STAGING)/$(LOGROTATE_DIR)
	install -m 644 etc/make-dummy-sr.service \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/xs-sm.service \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/sm-mpath-root.service \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/usb-scan.* \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/mpathcount.* \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/storage-init.service \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/sr_health_check.service \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/sr_health_check.timer \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	install -m 644 systemd/SMGC@.service \
	  $(SM_STAGING)/$(SYSTEMD_SERVICE_DIR)
	for i in $(UDEV_RULES); do \
	  install -m 644 udev/$$i.rules \
	    $(SM_STAGING)$(UDEV_RULES_DIR); done
	cd $(SM_STAGING)$(SM_DEST) && for i in $(SM_DRIVERS); do \
	  ln -sf $$i"SR.py" $$i"SR"; \
	done
	rm $(SM_STAGING)$(SM_DEST)/SHMSR
	cd $(SM_STAGING)$(SM_DEST) && rm -f LVHDSR && ln -sf LVHDSR.py LVMSR
	cd $(SM_STAGING)$(SM_DEST) && rm -f RawISCSISR && ln -sf RawISCSISR.py ISCSISR
	cd $(SM_STAGING)$(SM_DEST) && rm -f LVHDoISCSISR && ln -sf LVHDoISCSISR.py LVMoISCSISR
	cd $(SM_STAGING)$(SM_DEST) && rm -f LVHDoHBASR && ln -sf LVHDoHBASR.py LVMoHBASR
	ln -sf $(SM_DEST)mpathutil.py $(SM_STAGING)/sbin/mpathutil
	install -m 755 drivers/02-vhdcleanup $(SM_STAGING)$(MASTER_SCRIPT_DEST)
	install -m 755 drivers/lvhd-thin $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	install -m 755 drivers/on_slave.py $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)/on-slave
	install -m 755 drivers/testing-hooks $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	install -m 755 drivers/coalesce-leaf $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	install -m 755 drivers/nfs-on-slave $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	install -m 755 drivers/tapdisk-pause $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	install -m 755 drivers/intellicache-clean $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	install -m 755 drivers/trim $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	mkdir -p $(SM_STAGING)$(SM_LIBEXEC)
	install -m 755 scripts/local-device-change $(SM_STAGING)$(SM_LIBEXEC)
	install -m 755 scripts/check-device-sharing $(SM_STAGING)$(SM_LIBEXEC)
	install -m 755 scripts/usb_change $(SM_STAGING)$(SM_LIBEXEC)
	install -m 755 scripts/kickpipe $(SM_STAGING)$(SM_LIBEXEC)
	install -m 755 scripts/set-iscsi-initiator $(SM_STAGING)$(SM_LIBEXEC)
	mkdir -p $(SM_STAGING)/etc/xapi.d/xapi-pre-shutdown/
	install -m 755 scripts/stop_all_gc $(SM_STAGING)/etc/xapi.d/xapi-pre-shutdown/
	$(MAKE) -C dcopy install DESTDIR=$(SM_STAGING)
	ln -sf $(SM_DEST)blktap2.py $(SM_STAGING)$(BIN_DEST)/blktap2
	ln -sf $(SM_DEST)lcache.py $(SM_STAGING)$(BIN_DEST)tapdisk-cache-stats
	install -m 755 scripts/xs-mpath-scsidev.sh $(SM_STAGING)$(UDEV_SCRIPTS_DIR)
	install -m 755 scripts/make-dummy-sr $(SM_STAGING)$(SM_LIBEXEC)
	install -m 755 scripts/storage-init $(SM_STAGING)$(SM_LIBEXEC)

.PHONY: clean
clean:
	rm -rf $(SM_STAGING)


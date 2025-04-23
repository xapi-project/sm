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
SM_CORE_LIBS += mpath_null
SM_CORE_LIBS += xs_errors
SM_CORE_LIBS += iscsi
SM_CORE_LIBS += wwid_conf
SM_CORE_LIBS += lock
SM_CORE_LIBS += flock
SM_CORE_LIBS += f_exceptions
# Add a "pretend" core lib to cover the iscsi differences
# This uses sm.core.iscsi but provides some methods which
# sm-core-libs provided differently.
SM_CORE_LIBS += libiscsi

SM_LIBS :=
SM_LIBS += BaseISCSI
SM_LIBS += blktap2
SM_LIBS += cbtutil
SM_LIBS += cifutils
SM_LIBS += cleanup
SM_LIBS += constants
SM_LIBS += devscan
SM_LIBS += fjournaler
SM_LIBS += ipc
SM_LIBS += journaler
SM_LIBS += lock_queue
SM_LIBS += LUNperVDI
SM_LIBS += lvhdutil
SM_LIBS += lvmanager
SM_LIBS += lvmcache
SM_LIBS += lvutil
SM_LIBS += metadata
SM_LIBS += mpathcount
SM_LIBS += nfs
SM_LIBS += pluginutil
SM_LIBS += refcounter
SM_LIBS += resetvdis
SM_LIBS += SR
SM_LIBS += SRCommand
SM_LIBS += sr_health_check
SM_LIBS += srmetadata
SM_LIBS += sysdevice
SM_LIBS += trim_util
SM_LIBS += VDI
SM_LIBS += vhdutil

# Things used as commands which install in libexec
# which are in python and need compatibility symlinks from
# /opt
SM_LIBEXEC_PY_CMDS :=
SM_LIBEXEC_PY_CMDS += mpathcount
SM_LIBEXEC_PY_CMDS += cleanup
SM_LIBEXEC_PY_CMDS += sr_health_check
SM_LIBEXEC_PY_CMDS += verifyVHDsOnSR

# Things which are written as commands but have
# a .py extension which may eventually be dropped
# They are installed in libexec under a subdirectory
# and symlinked from /opt using their original names
SM_LIBEXEC_PY_XTRAS :=
SM_LIBEXEC_PY_XTRAS += scsi_host_rescan

# SCRIPTS which install in libexec
SM_LIBEXEC_SCRIPTS := local-device-change
SM_LIBEXEC_SCRIPTS += check-device-sharing
SM_LIBEXEC_SCRIPTS += usb_change
SM_LIBEXEC_SCRIPTS += kickpipe
SM_LIBEXEC_SCRIPTS += set-iscsi-initiator
SM_LIBEXEC_SCRIPTS += make-dummy-sr
SM_LIBEXEC_SCRIPTS += storage-init

SM_PLUGINS := __init__.py
SM_PLUGINS += keymanagerutil.py

SM_UDEV_SCRIPTS := xs-mpath-scsidev.sh

SM_XAPI_SHUTDOWN_SCRIPTS := stop_all_gc

# Libraries which remain in drivers/ and get installed in
# /opt/xensource/sm. All of which will eventually be wrappers around
# sm.core libs for backwards compatibility and can hopefully be one
# day dropped
SM_COMPAT_LIBS := util
SM_COMPAT_LIBS += scsiutil
SM_COMPAT_LIBS += mpath_dmp
SM_COMPAT_LIBS += mpath_cli
SM_COMPAT_LIBS += xs_errors
SM_COMPAT_LIBS += iscsilib
SM_COMPAT_LIBS += wwid_conf
SM_COMPAT_LIBS += lock
SM_COMPAT_LIBS += flock
SM_COMPAT_LIBS += mpath_null
SM_COMPAT_LIBS += SR
SM_COMPAT_LIBS += SRCommand
SM_COMPAT_LIBS += VDI
SM_COMPAT_LIBS += BaseISCSI
SM_COMPAT_LIBS += lvutil
SM_COMPAT_LIBS += lvmcache
SM_COMPAT_LIBS += vhdutil
SM_COMPAT_LIBS += lvhdutil
SM_COMPAT_LIBS += cifutils
SM_COMPAT_LIBS += nfs
SM_COMPAT_LIBS += devscan
SM_COMPAT_LIBS += sysdevice
SM_COMPAT_LIBS += LUNperVDI
SM_COMPAT_LIBS += refcounter
SM_COMPAT_LIBS += journaler
SM_COMPAT_LIBS += fjournaler
SM_COMPAT_LIBS += lock_queue
SM_COMPAT_LIBS += ipc
SM_COMPAT_LIBS += srmetadata
SM_COMPAT_LIBS += metadata
SM_COMPAT_LIBS += lvmanager
SM_COMPAT_LIBS += resetvdis
SM_COMPAT_LIBS += trim_util
SM_COMPAT_LIBS += pluginutil
SM_COMPAT_LIBS += constants
SM_COMPAT_LIBS += cbtutil

UDEV_RULES = 65-multipath 55-xs-mpath-scsidev 57-usb 58-xapi 99-purestorage
MPATH_CUSTOM_CONF = custom.conf
SMLOG_CONF = SMlog

SM_XML := XE_SR_ERRORCODES

OPT_SM_DEST := /opt/xensource/sm/
OPT_DEBUG_DEST := /opt/xensource/debug/
OPT_BIN_DEST := /opt/xensource/bin/
OPT_LIBEXEC := /opt/xensource/libexec/
MASTER_SCRIPT_DEST := /etc/xensource/master.d/
PLUGIN_SCRIPT_DEST := /etc/xapi.d/plugins/
BIN_DEST := /usr/bin/
SM_LIBEXEC := /usr/libexec/sm/
SM_DATADIR := /usr/share/sm/
UDEV_RULES_DIR := /etc/udev/rules.d/
UDEV_SCRIPTS_DIR := /etc/udev/scripts/
SYSTEMD_SERVICE_DIR := /usr/lib/systemd/system/
INIT_DIR := /etc/rc.d/init.d/
MPATH_CUSTOM_CONF_DIR := /etc/multipath/conf.d/
MODPROBE_DIR := /etc/modprobe.d/
EXTENSION_SCRIPT_DEST := /etc/xapi.d/extensions/
LOGROTATE_DIR := /etc/logrotate.d/

SM_STAGING := $(DESTDIR)
SM_STAMP := $(MY_OBJ_DIR)/.staging_stamp

SM_PY_FILES = $(foreach LIB, $(SM_LIBS), libs/sm/$(LIB).py) libs/sm/__init__.py
SM_CORE_PY_FILES = $(foreach LIB, $(SM_CORE_LIBS), libs/sm/core/$(LIB).py) libs/sm/core/__init__.py
SM_COMPAT_PY_FILES = $(foreach LIB, $(SM_COMPAT_LIBS), compat-libs/$(LIB).py) $(foreach DRIVER, $(SM_DRIVERS), drivers/$(DRIVER)SR.py)
# Various bits of python which need to be included in pylint etc, but are installed via other means
SM_XTRA_PY_FILES :=
SM_XTRA_PY_FILES += $(foreach LIB, $(SM_LIBEXEC_PY_CMDS), drivers/$(LIB))
SM_XTRA_PY_FILES += $(foreach LIB, $(SM_LIBEXEC_PY_XTRAS), drivers/$(LIB).py)
SM_XTRA_PY_FILES += drivers/mpathutil.py
SM_XTRA_PY_FILES += drivers/blktap2
SM_XTRA_PY_FILES += drivers/tapdisk-cache-stats

.PHONY: build
build:
	make -C dcopy 

.PHONY: precommit
precommit: build
	@ QUIT=0; \
	CHANGED=$$(git status --porcelain $(SM_PY_FILES) $(SM_CORE_PY_FILES) $(SM_COMPAT_PY_FILES) | awk '{print $$2}'); \
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
	PYTHONPATH=./drivers:./libs:./misc/fairlock:$$PYTHONPATH $(PYLINT) --rcfile=tests/pylintrc $(SM_PY_FILES) $(SM_CORE_PY_FILES) $(SM_XTRA_PY_FILES) $(SM_COMPAT_PY_FILES)
	echo "Precheck succeeded with no outstanding issues found."

.PHONY: install
install: precheck
	mkdir -p $(SM_STAGING)
	$(call mkdir_clean,$(SM_STAGING))
	mkdir -p $(SM_STAGING)$(OPT_SM_DEST)
	mkdir -p $(SM_STAGING)$(UDEV_RULES_DIR)
	mkdir -p $(SM_STAGING)$(UDEV_SCRIPTS_DIR)
	mkdir -p $(SM_STAGING)$(INIT_DIR)
	mkdir -p $(SM_STAGING)$(SYSTEMD_SERVICE_DIR)
	mkdir -p $(SM_STAGING)$(MPATH_CUSTOM_CONF_DIR)
	mkdir -p $(SM_STAGING)$(MODPROBE_DIR)
	mkdir -p $(SM_STAGING)$(LOGROTATE_DIR)
	mkdir -p $(SM_STAGING)$(OPT_DEBUG_DEST)
	mkdir -p $(SM_STAGING)$(OPT_BIN_DEST)
	mkdir -p $(SM_STAGING)$(MASTER_SCRIPT_DEST)
	mkdir -p $(SM_STAGING)$(PLUGIN_SCRIPT_DEST)
	mkdir -p $(SM_STAGING)$(EXTENSION_SCRIPT_DEST)
	mkdir -p $(SM_STAGING)/sbin
	# SM libs
	mkdir -p $(SM_STAGING)/$(PYTHONLIBDIR)/sm
	install -D -m 644 libs/sm/__init__.py $(SM_STAGING)$(PYTHONLIBDIR)/sm/__init__.py
	for i in $(SM_PY_FILES); do \
	  install -D -m 644 $$i $(SM_STAGING)$(PYTHONLIBDIR)/sm/; \
	done
	# Core libs
	mkdir -p $(SM_STAGING)/$(PYTHONLIBDIR)/sm/core
	for i in $(SM_CORE_PY_FILES); do \
	  install -D -m 644 $$i $(SM_STAGING)$(PYTHONLIBDIR)/sm/core/; \
	done
	# Data files (primarily XML error definitions)
	mkdir -p $(SM_STAGING)$(SM_DATADIR)
	for i in $(SM_XML); do \
	  install -D -m 644 libs/sm/core/$$i.xml $(SM_STAGING)$(SM_DATADIR)/; \
	done
	# Legacy SM python files
	for i in $(SM_COMPAT_PY_FILES); do \
	  install -m 755 $$i $(SM_STAGING)$(OPT_SM_DEST); \
	done
	# Plugin directory
	mkdir -p $(SM_STAGING)$(SM_LIBEXEC)/plugins
	ln -sf $(SM_LIBEXEC)/plugins $(SM_STAGING)$(OPT_SM_DEST)/plugins
	for i in $(SM_PLUGINS); do \
	  install -D -m 755 drivers/plugins/$$i $(SM_STAGING)/$(SM_LIBEXEC)/plugins/$$i; \
	done
	install -m 644 multipath/$(MPATH_CUSTOM_CONF) \
	  $(SM_STAGING)/$(MPATH_CUSTOM_CONF_DIR)
	install -m 755 multipath/multipath-root-setup \
	  $(SM_STAGING)/$(SM_LIBEXEC)/multipath-root-setup
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
	cd $(SM_STAGING)$(OPT_SM_DEST) && for i in $(SM_DRIVERS); do \
	  ln -sf $$i"SR.py" $$i"SR"; \
	done
	rm $(SM_STAGING)$(OPT_SM_DEST)/SHMSR
	cd $(SM_STAGING)$(OPT_SM_DEST) && rm -f LVHDSR && ln -sf LVHDSR.py LVMSR
	cd $(SM_STAGING)$(OPT_SM_DEST) && rm -f RawISCSISR && ln -sf RawISCSISR.py ISCSISR
	cd $(SM_STAGING)$(OPT_SM_DEST) && rm -f LVHDoISCSISR && ln -sf LVHDoISCSISR.py LVMoISCSISR
	cd $(SM_STAGING)$(OPT_SM_DEST) && rm -f LVHDoHBASR && ln -sf LVHDoHBASR.py LVMoHBASR
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
	mkdir -p $(SM_STAGING)$(OPT_LIBEXEC)
	# Install libexec scripts with symlinks from the legacy location
	for s in $(SM_LIBEXEC_SCRIPTS); do \
	  install -m 755 scripts/$$s $(SM_STAGING)$(SM_LIBEXEC)/$$s; \
	  ln -sf $(SM_LIBEXEC)$$s $(SM_STAGING)$(OPT_LIBEXEC)/$$s; \
	done
	# Install libexec commands with symlinks from the legacy location
	for s in $(SM_LIBEXEC_PY_CMDS); do \
	  install -m 755 drivers/$$s $(SM_STAGING)$(SM_LIBEXEC)/$$s; \
	  ln -sf $(SM_LIBEXEC)$$s $(SM_STAGING)$(OPT_SM_DEST)/"$$s".py; \
	done
	# Install libexec extras with symlinks from the legacy location
	for s in $(SM_LIBEXEC_PY_XTRAS); do \
	  install -D -m 755 drivers/"$$s".py $(SM_STAGING)$(SM_LIBEXEC)/xtra/$$s; \
	  ln -sf $(SM_LIBEXEC)xtra/$$s $(SM_STAGING)$(OPT_SM_DEST)/"$$s".py; \
	done
	mkdir -p $(SM_STAGING)/etc/xapi.d/xapi-pre-shutdown
	for s in $(SM_XAPI_SHUTDOWN_SCRIPTS); do \
	  install -m 755 scripts/$$s $(SM_STAGING)/etc/xapi.d/xapi-pre-shutdown/$$s; \
	done
	for s in $(SM_UDEV_SCRIPTS); do \
	  install -m 755 scripts/$$s $(SM_STAGING)$(UDEV_SCRIPTS_DIR)/$$s; \
	done
	# Install mpathutil and compatibility symlinks
	install -D -m 755 drivers/mpathutil.py $(SM_STAGING)$(BIN_DEST)/mpathutil
	ln -sf $(BIN_DEST)mpathutil $(SM_STAGING)$(OPT_SM_DEST)/mpathutil.py
	ln -sf $(BIN_DEST)mpathutil $(SM_STAGING)/sbin/mpathutil
	# Install blktap2 and compatibility symlinks
	install -D -m 755 drivers/blktap2 $(SM_STAGING)$(BIN_DEST)/blktap2
	ln -sf $(BIN_DEST)blktap2 $(SM_STAGING)$(OPT_BIN_DEST)/blktap2
	# Install tapdisk-cache-stats and compatibility symlinks
	install -D -m 755 drivers/tapdisk-cache-stats $(SM_STAGING)$(BIN_DEST)/tapdisk-cache-stats
	ln -sf $(BIN_DEST)tapdisk-cache-stats $(SM_STAGING)$(OPT_BIN_DEST)/tapdisk-cache-stats

	$(MAKE) -C dcopy install DESTDIR=$(SM_STAGING)

.PHONY: clean
clean:
	rm -rf $(SM_STAGING)


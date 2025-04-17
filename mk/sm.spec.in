# -*- rpm-spec -*-

Summary: sm - XCP storage managers
Name:    sm
Version: @SM_VERSION@ 
Release: @SM_RELEASE@
Group:   System/Hypervisor
License: LGPL
URL:  http://www.citrix.com
Source0: sm-@SM_VERSION@.tar.bz2

%define __python python3.6

BuildRequires: python3
BuildRequires: python3-devel
BuildRequires: python3-pylint
BuildRequires: python3-coverage
BuildRequires: python3-bitarray

Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
Requires: sm-fairlock = %{version}-%{release}
Requires: xenserver-multipath
Requires: xenserver-lvm2 >= 2.02.180-11.xs+2.0.2
Obsoletes: lvm2-sm-config <= 7:2.02.180-15.xs8
Requires: python3-bitarray
Requires: sm-debugtools = %{version}-%{release}
Requires: python%{python3_pkgversion}-sm-libs = %{version}-%{release}
Requires: sm-compat = %{version}-%{release}
Requires: python%{python3_pkgversion}-sm-compat = %{version}-%{release}
# For cgclassify command
Requires: libcgroup-tools
Requires(post): xs-presets >= 1.3
Requires(preun): xs-presets >= 1.3
Requires(postun): xs-presets >= 1.3
Conflicts: kernel < 4.19.19-5.0.0
Conflicts: blktap < 4.0.0
Requires: sg3_utils

%description
This package contains storage backends used in XCP

%prep
%autosetup -p1

%build
make
make -C misc/fairlock

%install
make -C misc/fairlock install DESTDIR=%{buildroot}
make install DESTDIR=%{buildroot}
mkdir -p %{buildroot}%{_datadir}/%{name}/

%post
%systemd_post make-dummy-sr.service
%systemd_post mpcount.service
%systemd_post sm-mpath-root.service
%systemd_post xs-sm.service
%systemd_post storage-init.service
%systemd_post usb-scan.socket
%systemd_post mpathcount.socket
%systemd_post sr_health_check.timer
%systemd_post sr_health_check.service

# On upgrade, migrate from the old statefile to the new statefile so that
# storage is not reinitialized.
if [ $1 -gt 1 ] ; then
    grep -q ^success "%{_sysconfdir}/firstboot.d/state/10-prepare-storage" 2>/dev/null && touch /var/lib/misc/ran-storage-init || :
fi

rm -f "%{_sysconfdir}/lvm/cache/.cache"
touch "%{_sysconfdir}/lvm/cache/.cache"

systemctl enable sr_health_check.timer
systemctl start sr_health_check.timer

%preun
%systemd_preun make-dummy-sr.service
%systemd_preun mpcount.service
%systemd_preun sm-mpath-root.service
%systemd_preun xs-sm.service
%systemd_preun storage-init.service
%systemd_preun usb-scan.socket
%systemd_preun mpathcount.socket
%systemd_preun sr_health_check.timer
%systemd_preun sr_health_check.service

%postun
%systemd_postun make-dummy-sr.service
%systemd_postun mpcount.service
%systemd_postun sm-mpath-root.service
%systemd_postun xs-sm.service
%systemd_postun storage-init.service
%systemd_postun sr_health_check.timer
%systemd_postun sr_health_check.service

%check
tests/run_python_unittests.sh

%files
%defattr(-,root,root,-)
%{_libexecdir}/sm
%exclude %{_libexecdir}/sm/debug
%exclude %{_libexecdir}/sm/plugins/keymanagerutil.py
%{_sysconfdir}/udev/scripts/xs-mpath-scsidev.sh
%{_sysconfdir}/xapi.d/plugins/coalesce-leaf
%{_sysconfdir}/xapi.d/plugins/lvhd-thin
%{_sysconfdir}/xapi.d/plugins/nfs-on-slave
%{_sysconfdir}/xapi.d/plugins/on-slave
%{_sysconfdir}/xapi.d/plugins/tapdisk-pause
%{_sysconfdir}/xapi.d/plugins/testing-hooks
%{_sysconfdir}/xapi.d/plugins/intellicache-clean
%{_sysconfdir}/xapi.d/plugins/trim
%{_sysconfdir}/xapi.d/xapi-pre-shutdown/*
%{_bindir}/mpathutil
%{_bindir}/blktap2
%{_bindir}/tapdisk-cache-stats
%{_unitdir}/make-dummy-sr.service
%{_unitdir}/xs-sm.service
%{_unitdir}/sm-mpath-root.service
%{_unitdir}/usb-scan.service
%{_unitdir}/usb-scan.socket
%{_unitdir}/mpathcount.service
%{_unitdir}/mpathcount.socket
%{_unitdir}/storage-init.service
%{_unitdir}/sr_health_check.timer
%{_unitdir}/sr_health_check.service
%{_unitdir}/SMGC@.service
%config %{_sysconfdir}/udev/rules.d/65-multipath.rules
%config %{_sysconfdir}/udev/rules.d/55-xs-mpath-scsidev.rules
%config %{_sysconfdir}/udev/rules.d/58-xapi.rules
%dir %{_sysconfdir}/multipath/conf.d
%config(noreplace) %{_sysconfdir}/multipath/conf.d/custom.conf
%config %{_sysconfdir}/logrotate.d/SMlog
%config %{_sysconfdir}/udev/rules.d/57-usb.rules
%config %{_sysconfdir}/udev/rules.d/99-purestorage.rules
%doc CONTRIB LICENSE MAINTAINERS README.md

%package fairlock
Summary: Fair locking subsystem

%description fairlock
This package provides the fair locking subsystem using by the Storage
Manager and some other packages

%files fairlock
%{python3_sitelib}/__pycache__/fairlock*pyc
%{python3_sitelib}/fairlock.py
%{_unitdir}/fairlock@.service
%{_libexecdir}/fairlock

%post fairlock
## On upgrade, shut down existing lock services so new ones will
## be started. There should be no locks held during upgrade operations
## so this is safe.
if [ $1 -gt 1 ];
then
    /usr/bin/systemctl list-units fairlock@* --all --no-legend | /usr/bin/cut -d' ' -f1 | while read service;
    do
        /usr/bin/systemctl stop "$service"
    done
fi

%package debugtools
Summary: SM utilities for debug and testing

%description debugtools
Utilities for debug and testing purposes

%files debugtools
%{_libexecdir}/sm/debug


%package -n python%{python3_pkgversion}-sm-libs
Summary: SM core libraries
BuildArch: noarch
Provides: python%{python3_pkgversion}-sm-core-libs = 1.1.3-1
Obsoletes: python%{python3_pkgversion}-sm-core-libs < 1.1.3-2

%description -n python%{python3_pkgversion}-sm-libs
This package contains common core libraries for SM.

It obsoletes and replaces the old sm-core-libs package.

%files -n python%{python3_pkgversion}-sm-libs
%{python3_sitelib}/sm
%{_datadir}/sm

%package -n python%{python3_pkgversion}-sm-compat
Summary: SM compatibility files for older callers
BuildArch: noarch
Requires: sm = %{version}-%{release}

%description -n python%{python3_pkgversion}-sm-compat
This package contains compatibility wrappers left behind for older
callers which expect to find python files in /opt/xensource


%files -n python%{python3_pkgversion}-sm-compat
/sbin/mpathutil
/opt/xensource/sm
/opt/xensource/bin/blktap2
/opt/xensource/bin/tapdisk-cache-stats
%{_sysconfdir}/xensource/master.d/02-vhdcleanup
/opt/xensource/libexec/check-device-sharing
/opt/xensource/libexec/local-device-change
/opt/xensource/libexec/make-dummy-sr
/opt/xensource/libexec/usb_change
/opt/xensource/libexec/kickpipe
/opt/xensource/libexec/set-iscsi-initiator
/opt/xensource/libexec/storage-init

%package compat
Summary: SM compatibility files for older callers

%description compat
This package contains arch-specific compatibility wrappers left
behind for older callers which expect to find libraries and binaries
in /opt/xensource

%files compat
/opt/xensource/debug/tp
/opt/xensource/libexec/dcopy

%changelog


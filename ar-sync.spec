%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%define pylib %{python_sitelib}/arsync

Name: ar-sync
Summary: A/R Comp Engine sync scripts
Version: 1.0.0
Release: 1%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: noarch
Source0:   %{name}-%{version}.tar.gz
Requires: stomppy >= 3.0.3

%description
Installs the service for syncing A/R Comp Engine
with SAM topology.

%prep
%setup 

%install 
%{__rm} -rf %{buildroot}
install --directory %{buildroot}/etc/cron.daily
install --directory %{buildroot}/usr/bin
install --directory %{buildroot}/etc/ar-sync/
install --directory %{buildroot}/var/lib/ar-sync
install --directory %{buildroot}/var/log/ar-sync
install --mode 644 etc/ar-sync/poem_sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem_profile.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem_server.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/topology_sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/downtime_sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/prefilter.conf %{buildroot}/etc/ar-sync/
install --mode 755 bin/poem-sync %{buildroot}/usr/bin
install --mode 755 bin/topology-sync %{buildroot}/usr/bin
install --mode 755 bin/downtime-sync %{buildroot}/usr/bin
install --mode 755 bin/prefilter %{buildroot}/usr/bin

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/bin/poem-sync
%attr(0755,root,root) /usr/bin/topology-sync
%attr(0755,root,root) /usr/bin/downtime-sync
%attr(0755,root,root) /usr/bin/prefilter
%attr(0755,root,root) /etc/cron.daily/ar-sync
%config(noreplace) /etc/ar-sync/poem_sync.conf
%config(noreplace) /etc/ar-sync/poem.conf
%config(noreplace) /etc/ar-sync/poem_profile.conf
%config(noreplace) /etc/ar-sync/poem_server.conf
%config(noreplace) /etc/ar-sync/topology_sync.conf
%config(noreplace) /etc/ar-sync/downtime_sync.conf
%config(noreplace) /etc/ar-sync/prefilter.conf
%attr(0750,arstats,arstats) /var/lib/ar-sync
%attr(0750,arstats,arstats) /var/log/ar-sync
%{pylib}

%post

%pre
getent group arstats > /dev/null || groupadd -r arstats
getent passwd arstats > /dev/null || \
    useradd -r -g arstats -d /var/lib/ar-sync -s /sbin/nologin \
    -c "AR Comp Engine user" arstats

%preun

%changelog
* Thu Aug 1 2013 Emir Imamagic <eimamagi@srce.hr> - 1.0.0-1%{?dist}
- Initial release

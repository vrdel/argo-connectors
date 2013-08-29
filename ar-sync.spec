Name: ar-sync
Summary: A/R Comp Engine sync scripts
Version: 1.0.0
Release: 2%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: noarch
Source0:   %{name}-%{version}.tar.gz

%description
Installs the service for syncing A/R Comp Engine
with SAM topology.

%prep
%setup 

%install 
%{__rm} -rf %{buildroot}
#install --directory %{buildroot}/etc/cron.daily
install --directory %{buildroot}/usr/libexec/ar-sync
install --directory %{buildroot}/etc/ar-sync/
install --directory %{buildroot}/var/lib/ar-sync
install --directory %{buildroot}/var/log/ar-sync
#install --mode 644 etc/cron.daily/ar-sync %{buildroot}/etc/cron.daily
install --mode 644 etc/ar-sync/poem-sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem-profile.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem-server.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/topology-sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/downtime-sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/prefilter.conf %{buildroot}/etc/ar-sync/
install --mode 755 bin/poem-sync %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/topology-sync %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/downtime-sync %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/prefilter %{buildroot}/usr/libexec/ar-sync

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/libexec/ar-sync/poem-sync
%attr(0755,root,root) /usr/libexec/ar-sync/topology-sync
%attr(0755,root,root) /usr/libexec/ar-sync/downtime-sync
%attr(0755,root,root) /usr/libexec/ar-sync/prefilter
#%attr(0755,root,root) /etc/cron.daily/ar-sync
%config(noreplace) /etc/ar-sync/poem-sync.conf
%config(noreplace) /etc/ar-sync/poem.conf
%config(noreplace) /etc/ar-sync/poem-profile.conf
%config(noreplace) /etc/ar-sync/poem-server.conf
%config(noreplace) /etc/ar-sync/topology-sync.conf
%config(noreplace) /etc/ar-sync/downtime-sync.conf
%config(noreplace) /etc/ar-sync/prefilter.conf
%attr(0750,root,root) /var/lib/ar-sync
%attr(0750,root,root) /var/log/ar-sync

%changelog
* Thu Aug 29 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-2%{?dist}
- Minor change in prefilter script
* Thu Aug 1 2013 Luko Gjenero <lgjenero@srce.hr> - 1.0.0-1%{?dist}
- Initial release

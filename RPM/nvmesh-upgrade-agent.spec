Name:                           nvmesh-upgrade-agent
Version:                        %{version}
Release:                        %{release}
Group:                          System Environment/Kernel
Summary:                        "nvmesh-upgrade-agent" by Nvidia

License:                        Commercial Non OSI
URL:                            http://www.nvidia.com
Source0:                        %{name}
AutoReqProv:                    no

%define _build_id_links none

%description

© Copyright 2025 Nvidia Corporation. All rights reserved. This document contains the confidential and proprietary information of Nvidia Corporation, Inc. Do not reproduce or distribute without the prior written consent of Nvidia.

"Nvidia nvmesh-upgrade-agent" components.
        Branch: %{branch}
        Commit: %{commit_id}
        ChangeId: %{change_id}

%prep
cp -rf %{_sourcedir}/%{name} %{_builddir}/

%build

%install
mkdir -pv %{buildroot}/etc/nvmesh
mkdir -pv %{buildroot}/opt/nvmesh/upgradeagent
mkdir -pv %{buildroot}/lib/systemd/system
mkdir -pv %{buildroot}/var/opt/nvmesh/upgradeagent

cp -rf %{_builddir}/%{name}/main.py %{buildroot}/opt/nvmesh/upgradeagent/
cp -rf %{_builddir}/%{name}/upgradeagent.py %{buildroot}/opt/nvmesh/upgradeagent/
cp -rf %{_builddir}/%{name}/utils.py %{buildroot}/opt/nvmesh/upgradeagent/
cp -rf %{_builddir}/%{name}/fsatomic.py %{buildroot}/opt/nvmesh/upgradeagent/
cp -rf %{_builddir}/%{name}/src %{buildroot}/opt/nvmesh/upgradeagent/
cp -rf %{_builddir}/%{name}/dist %{buildroot}/opt/nvmesh/upgradeagent/
cp %{_builddir}/%{name}/upgradeagent.conf %{buildroot}/etc/nvmesh/
cp -rf %{_builddir}/%{name}/systemd/nvmeshupgradeagent.service %{buildroot}/lib/systemd/system/

echo "version=\"%{version}-%{release}\"" > %{buildroot}/opt/nvmesh/upgradeagent/version
echo "commit=\"%{commit_id}\"" >> %{buildroot}/opt/nvmesh/upgradeagent/version
echo "branch=\"%{branch}\"" >> %{buildroot}/opt/nvmesh/upgradeagent/version

%post
systemctl enable nvmeshupgradeagent > /dev/null 2>&1

%preun
# not an upgrade
if [ "$1" = "0" ]; then
    systemctl stop nvmeshupgradeagent > /dev/null 2>&1
    systemctl disable nvmeshupgradeagent > /dev/null 2>&1
fi

%files
/opt/nvmesh/upgradeagent
/lib/systemd/system/nvmeshupgradeagent.service
/var/opt/nvmesh/upgradeagent

%config(noreplace) /etc/nvmesh/upgradeagent.conf

%changelog
* Sun May 18 2025 Nvidia
- Installing Nvidia nvmesh-upgrade-agent


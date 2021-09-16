FROM centos:7

RUN yum -y install epel-release
RUN yum -y makecache; yum -y update

RUN yum install -y \
        gcc \
        git \
        libffi \
        libffi-devel \
        modules \
        openssl-devel \
        python \
        python-argparse \
        python-devel \
        python3-devel \
        python-pip \
        python3-pip \
        python-requests \
        tar \
        wget

RUN wget https://raw.githubusercontent.com/vrdel/my-vm-customize/master/centos7/argo-devel.repo -O /etc/yum.repos.d/argo-devel.repo
RUN yum install -y argo-egi-connectors

RUN pip3 install --upgrade pip
RUN pip install \
        argo_ams_library \
        avro \
        cffi \
        coverage==4.5.4 \
        cryptography==2.1.4 \
        discover \
        httmock \
        mock==2.0.0 \
        pyOpenSSL \
        setuptools \
        unittest2

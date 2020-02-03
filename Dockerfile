FROM centos:6.9
MAINTAINER Themis Zamani themiszamani@gmail.com

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
        python-pip \
        tar \
        wget
RUN pip install \
        argo_ams_library \
        avro \
        cffi \
        coverage==4.5.4 \
        cryptography==2.1.4 \
        discover \
        httmock \
        mock \
        pyOpenSSL \
        setuptools \
        requests \
        unittest2

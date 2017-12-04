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
        python-setuptools \
        tar \
        wget
RUN pip install \
        argo_ams_library \
        avro \
        coverage \
		cffi \
        cryptography \
        discover \
        httmock \
        mock \
        pyOpenSSL \
        requests \
        unittest2

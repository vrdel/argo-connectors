FROM centos:centos6
MAINTAINER Themis Zamani themiszamani@gmail.com

RUN yum -y update && \
    yum install -y \
        gcc \
        git \
        libffi \
        libffi-devel \
        modules \
        openssl-devel \
        python \
        python-argparse \
        python-devel \
        python-setuptools \
        tar \
        wget

RUN easy_install pip

RUN pip install \
        argo_ams_library \
        avro \
        coverage \
        cryptography \
        discover \
        httmock \
        pyOpenSSL \
        requests \
        unittest2

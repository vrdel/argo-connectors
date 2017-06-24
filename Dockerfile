FROM centos:centos6
MAINTAINER Themis Zamani themiszamani@gmail.com

RUN yum -y update && \
    yum install -y \
    git \
    tar \
    wget \
    python \
    python-argparse \
    python-setuptools \
    modules

RUN yum install -y gcc
RUN yum provides -y libffi
RUN yum install -y libffi
RUN yum install -y openssl-devel
RUN yum install -y libffi-devel
RUN yum install -y python-devel
RUN easy_install pip
RUN pip install cryptography
RUN pip install coverage
RUN pip install unittest2
RUN pip install discover
RUN pip install pyOpenSSL
RUN pip install httmock
RUN pip install avro
RUN pip install argo_ams_library
RUN pip install requests

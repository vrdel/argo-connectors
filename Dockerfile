FROM centos:centos6
MAINTAINER Themis Zamani themiszamani@gmail.com

RUN yum -y update && \
    yum install -y \
    git \
    tar \
    wget \
    python \
    python-setuptools \
    modules

RUN easy_install pip
RUN pip install coverage
RUN pip install unittest2
RUN pip install discover


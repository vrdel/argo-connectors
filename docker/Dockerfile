FROM rockylinux:9
MAINTAINER dvrcic@srce.hr
RUN groupadd user -g 1000 && useradd -u 1000 -g 1000 user -m -d /home/user -s /bin/zsh
RUN dnf -y update; dnf clean all
RUN dnf -y install epel-release rocky-repos && \
    dnf -y install \
    procps-ng \
    ack \
    ctags \
    fortune-mod \
    git \
    gcc \
    glibc-all-langpacks \
    iproute \
    make \
    mc \
    net-tools \
    passwd \
    python3-devel \
    python3-pip \
    python3-setuptools \
    rpmdevtools \
    openldap-devel \
    sudo \
    the_silver_searcher \
    tmux \
    vim \
    wget \
    yum-utils \
    zsh
RUN dnf --enablerepo=devel install -y ncurses-compat-libs
RUN pip install -U pip; pip3 install -U pip
RUN python3.9 -m venv /opt/poetry-tool/
RUN sh -c '\
  . /opt/poetry-tool/bin/activate \
  && pip install -U pip \
  && pip install -U pip setuptools==69.2.0 \
  && pip install -U poetry'
ADD poetry-path.sh /etc/profile.d
RUN passwd -d root
VOLUME [ "/sys/fs/cgroup" ]
RUN echo "user ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN echo "Defaults	env_keep += \"PYTHONPATH\"" >> /etc/sudoers
USER user
COPY chezmoi-config-apply.sh /home/user
WORKDIR /home/user
RUN curl -sfL https://git.io/chezmoi | sh
CMD ["/bin/zsh"]

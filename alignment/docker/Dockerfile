FROM ubuntu:14.04
MAINTAINER Daniel Miller <dmiller15@uchicago.edu>

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --force-yes \
    curl \
    g++ \
    make \
    python \
    libboost-dev \
    libboost-thread-dev \
    libboost-system-dev \
    zlib1g-dev \
    ncurses-dev \
    unzip \
    gzip \
    bzip2 \
    libxml2-dev \
    libxslt-dev \
    python-pip \
    python-dev \
    git \
    s3cmd \
    time \
    wget \
    samtools \
    python-virtualenv \
    default-jre \
    default-jdk

RUN adduser --disabled-password --gecos '' ubuntu && adduser ubuntu sudo && echo "ubuntu    ALL=(ALL)   NOPASSWD:ALL" >> /etc/sudoers.d/ubuntu

ENV HOME /home/ubuntu

USER ubuntu
RUN mkdir ${HOME}/bin
WORKDIR ${HOME}/bin

# Download BWA
RUN wget https://github.com/lh3/bwa/archive/0.7.12.tar.gz && tar xvzf *.tar.gz

# Get and install genetorrent
RUN wget https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.7/genetorrent-common_3.8.7-ubuntu2.207-14.04_amd64.deb \
&& wget https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.7/genetorrent-download_3.8.7-ubuntu2.207-14.04_amd64.deb

# Remove compressed files
RUN rm *.gz

WORKDIR ${HOME}/bin/bwa-0.7.12

RUN make

ENV PATH ${PATH}:${HOME}/bin/bwa-0.7.12

USER root
RUN dpkg -i --force-depends ${HOME}/bin/genetorrent-*.deb \
    && apt-get update \
    && apt-get -f install -y

RUN pip install s3cmd --user

ENV mirna_aligner 0.01
WORKDIR ${HOME}/bin

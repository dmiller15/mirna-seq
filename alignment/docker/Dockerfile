FROM ubuntu:14.04
MAINTAINER Daniel Miller <dmiller15@uchicago.edu>

USER root
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get dist-upgrade -y --force-yes \
    && apt-get install software-properties-common -y

RUN apt-get update && apt-get install -y --force-yes \
    ant \
    cmake \
    default-jdk \
    gcc \
    git \
    libpq-dev \
    openjdk-7-jre-headless \
    poppler-utils \
    postgresql-client \
    python-pip \
    python-virtualenv \
    # python3-dev \
    python3.4=3.4.0-2ubuntu1.1 \
    python3.4-minimal=3.4.0-2ubuntu1.1 \
    libpython3.4-stdlib=3.4.0-2ubuntu1.1 \
    libpython3.4-minimal=3.4.0-2ubuntu1.1 \
    r-base \
    s3cmd \
    time \
    wget

RUN apt-get install --reinstall libpython3.4-stdlib

RUN adduser --disabled-password --gecos '' ubuntu && adduser ubuntu sudo && echo "ubuntu    ALL=(ALL)   NOPASSWD:ALL" >> /etc/sudoers.d/ubuntu

ENV HOME /home/ubuntu

USER ubuntu

RUN mkdir ${HOME}/tools

WORKDIR ${HOME}/tools

RUN wget https://github.com/samtools/samtools/releases/download/1.2/samtools-1.2.tar.bz2 \
    && wget https://github.com/broadinstitute/picard/releases/download/1.139/picard-tools-1.139.zip \
    && git clone https://github.com/broadinstitute/picard.git \
    && mv ${HOME}/tools/picard/src ${HOME}/ \
    && wget http://downloads.sourceforge.net/project/bio-bwa/bwakit/bwakit-0.7.12_x64-linux.tar.bz2 \
    && wget https://github.com/gt1/biobambam2/releases/download/2.0.18-release-20150914145735/biobambam2-2.0.18-release-20150914145735-x86_64-etch-linux-gnu.tar.gz \
    && wget http://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.11.3.zip \
    && wget https://github.com/pezmaster31/bamtools/archive/v2.4.0.tar.gz \
    && wget http://www.usadellab.org/cms/uploads/supplementary/Trimmomatic/Trimmomatic-Src-0.33.zip \
    && wget https://github.com/lomereiter/sambamba/releases/download/v0.5.8/sambamba_v0.5.8_linux.tar.bz2 \
    && tar xf samtools-1.2.tar.bz2 \
    && unzip picard-tools-1.139.zip \
    && tar xf bwakit-0.7.12_x64-linux.tar.bz2 \
    && tar xf biobambam2-2.0.18-release-20150914145735-x86_64-etch-linux-gnu.tar.gz \
    && unzip fastqc_v0.11.3.zip \
    && tar xf v2.4.0.tar.gz \
    && unzip Trimmomatic-Src-0.33.zip \
    && tar xf sambamba_v0.5.8_linux.tar.bz2 \
    && mv samtools-1.2 samtools \
    && mv biobambam2-2.0.18-release-20150914145735-x86_64-etch-linux-gnu biobambam \
    && mv picard-tools-1.139 picard-tools \
    && mv bamtools-2.4.0 bamtools \
    && mv trimmomatic-0.33 trimmomatic \
    && mv sambamba_v0.5.8 sambamba \
    && chmod +x FastQC/fastqc \
    && chmod +x sambamba \
    && rm *.gz *.bz2 *.zip 

WORKDIR ${HOME}/tools/samtools/
RUN make
WORKDIR ${HOME}/tools/bamtools/
RUN mkdir build
WORKDIR ${HOME}/tools/bamtools/build/
RUN cmake ..
RUN make
WORKDIR ${HOME}/tools/trimmomatic/
RUN ant
RUN mv ${HOME}/tools/trimmomatic/dist/jar/trimmomatic-0.33.jar ${HOME}/tools/trimmomatic/dist/jar/trimmomatic.jar
WORKDIR ${HOME}

# more ubuntu break
USER root
RUN apt-get install python3.4-dev=3.4.0-2ubuntu1.1 -y
USER ubuntu

RUN pip install --user virtualenvwrapper \
    && /bin/bash -c "source ${HOME}/.local/bin/virtualenvwrapper.sh \
    && mkvirtualenv --python=/usr/bin/python3 p3 \
    && pip install ipython numpy pandas psycopg2 pysam sqlalchemy biopython \
    && echo source ${HOME}/.local/bin/virtualenvwrapper.sh >> ${HOME}/.bashrc \
    && echo source ${HOME}/.virtualenvs/p3/bin/activate >> ${HOME}/.bashrc"

RUN pip install s3cmd --user

ENV mirna_aligner 0.16

RUN git clone https://github.com/dmiller15/mirna-seq.git


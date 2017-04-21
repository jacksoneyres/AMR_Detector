FROM ubuntu:16.04
MAINTAINER Jackson Eyres, jackson.eyres@gmail.com

RUN apt-get update && apt-get install -y \
		python \
		python-dev \
		python-setuptools \
		python-pip \
		gcc \
		bash \
		libpq-dev \
		ncbi-blast+ \
		wget \
		unzip \
		git

RUN apt-get install -y libblas-dev liblapack-dev libatlas-base-dev gfortran
RUN apt-get install -y python-scipy bowtie2 libncurses5-dev
RUN pip install scipy

COPY requirements.txt /
RUN pip install -r requirements.txt
RUN pip install pysam==0.8.4
RUN pip install pysamstats

RUN git clone https://github.com/OLC-Bioinformatics/GeneSeekr.git
RUN cd GeneSeekr && python setup.py install

RUN apt-get install -y autoconf
# install htslib
RUN \
  wget -c https://github.com/samtools/htslib/archive/1.3.2.tar.gz && \
  tar -zxvf 1.3.2.tar.gz && \
  mv htslib-1.3.2 htslib && \
  cd htslib && \
  autoreconf && \
  ./configure && \
  make && \
  make install

# install samtools
RUN \
  wget -c https://github.com/samtools/samtools/archive/1.3.1.tar.gz && \
  tar -zxvf 1.3.1.tar.gz && \
  cd samtools-1.3.1 && \
  make && \
make install

RUN git clone https://github.com/katholt/srst2
RUN pip install srst2/
RUN pip install subprocess32
COPY . /app

EXPOSE 8000
ENV C_FORCE_ROOT 1
ENTRYPOINT /bin/bash /app/init.sh


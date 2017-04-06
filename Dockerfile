FROM ubuntu:16.04
MAINTAINER Jackson Eyres, jackson.eyres@gmail.com

#COPY sources.list /etc/apt/sources.list

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

RUN git clone  https://github.com/OLC-Bioinformatics/GeneSeekr.git
RUN cd GeneSeekr && python setup.py install
 
COPY samtools-1.3.1 /samtools
RUN cd /samtools  && ./configure && make

RUN git clone https://github.com/katholt/srst2
RUN pip install srst2/
RUN pip install subprocess32
COPY . /app

EXPOSE 8000
ENV PATH /samtools/:$PATH

ENV C_FORCE_ROOT 1
ENTRYPOINT /bin/bash /app/init.sh


FROM ubuntu:16.04
RUN apt-get update
RUN apt-get install -y \
    gcc \
    git \
    curl \
    sudo \
    sudo \
    flex \
    libcsv-dev \
    python-dev \
    python-pip 

RUN pip install python-dateutil

RUN mkdir build && cd build && \
    git clone https://github.com/facebook/zstd.git

RUN cd build && cd zstd && \
  make && make install && cd .. 

RUN cd build && \
    git clone https://github.com/toschoo/tsalgo.git

RUN cd build && cd tsalgo && mkdir lib && \
  env LD_LIBRARY_PATH="./lib:/usr/lib:/usr/local/lib" \
  make all && make install && cd .. 

ADD beet build/beet
RUN cd build && cd beet && \
  env LD_LIBRARY_PATH="./lib:/usr/lib:/usr/local/lib" \
  make all && make install && cd .. 

ADD nowdb build/nowdb
RUN cd build && cd nowdb && \
  env LD_LIBRARY_PATH="./lib:/usr/lib:/usr/local/lib" make lemon && \
  env LD_LIBRARY_PATH="./lib:/usr/lib:/usr/local/lib" \
  make all && make install && cd .. 

# shall be nowdb
RUN groupadd -g 1000 nowdb
RUN useradd -u 1000 -g 1000 nowdb

COPY nowstart.sh .
COPY nowstop.sh .

RUN chown nowdb.nowdb nowstart.sh && chmod u+x nowstart.sh
RUN chown nowdb.nowdb nowstop.sh && chmod u+x nowstop.sh

USER nowdb

# sra2json
FROM python:3.7
LABEL omicidx_version='0.2.0'
LABEL author='Sean Davis'
LABEL app='omicidx'
LABEL repository='https://github.com/seandavi/omicidx'

RUN apt-get update
RUN apt-get install -y wget

WORKDIR /tmp

RUN git clone https://github.com/seandavi/omicidx.git

WORKDIR /tmp/omicidx
RUN git pull origin master

RUN pip install .

RUN mkdir /data
WORKDIR /data
RUN rm -rf /tmp/omicidx

RUN pip install awscli google

RUN echo "alias omicidx-cli='python -m omicidx.scripts.cli'" > /root/.bashrc

CMD ["/bin/bash"]

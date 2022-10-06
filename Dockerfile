FROM ubuntu:20.04 as base
ENV TZ=Europe/Berlin
ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

######################################################################
## build essential libraries

FROM base as libs
USER root
WORKDIR /opt/pynock

RUN set -eux; \
	apt-get update ; \
	apt-get upgrade -y ; \
	apt-get install -y --no-install-recommends \
		tzdata build-essential software-properties-common \		
		wget git gpg-agent apt-transport-https ca-certificates apt-utils \
		python3.8 python3-pytest python3.8-distutils python3.8-dev python3.8-venv ; \
	rm -rf /var/lib/apt/lists/*

## setup Python 3.8 and Pip
RUN set -eux; \
	wget https://bootstrap.pypa.io/get-pip.py -O get-pip.py ; \
	python3.8 get-pip.py ; \
	python3.8 -m pip install -U pip

######################################################################
## build pynock

FROM libs as pynock

## copy source
COPY ./pynock /opt/pynock/pynock
COPY ./dat /opt/pynock/dat
COPY ./requirements*.txt /opt/pynock/
COPY ./tests /opt/pynock/tests

## create a known user ID
RUN set -eux; \
	groupadd -g 999 appuser ; \
	useradd -r -u 999 -g appuser appuser ; \
	usermod -d /opt/pynock appuser ; \
	chown -R appuser:appuser /opt/pynock ; \
	chmod -R u+rw /opt/pynock

USER appuser

## install Python dependencies in a venv to maintain same binary path as system
WORKDIR /opt/pynock

RUN set -eux; \
	python3.8 -m venv /opt/pynock/venv ; \
	source /opt/pynock/venv/bin/activate ; \
	/opt/pynock/venv/bin/python3.8 -m pip install -U pip wheel setuptools ; \
	/opt/pynock/venv/bin/python3.8 -m pip install -r /opt/pynock/requirements.txt

######################################################################
## specific for test suite:

FROM pynock as testsuite

WORKDIR /opt/pynock
USER appuser

RUN set -eux; \
	source /opt/pynock/venv/bin/activate ; \
	/opt/pynock/venv/bin/python3.8 -m pip install -r /opt/pynock/requirements-dev.txt

CMD /opt/pynock/venv/bin/python3.8 -m pytest tests/
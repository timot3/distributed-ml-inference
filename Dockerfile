# dockerfile to create a container with either the introducer or a node

FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    apt-utils \
    python3 \
    python3-pip


COPY . /app

WORKDIR /app

RUN pip3 install -r requirements.txt


# if the environment variable NODE_TYPE is set to introducer, run the introducer
# otherwise, run the node
# by default, run the node

VAR $NODE_TYPE
CMD if [ "$NODE_TYPE" = "introducer" ]; then \
        python3 introducer.py; \
    else \
        python3 node.py; \
    fi

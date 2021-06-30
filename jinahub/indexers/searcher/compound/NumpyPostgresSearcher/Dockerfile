FROM jinaai/jina:2.0 as base

# install and upgrade pip
RUN apt-get update && apt-get install -y python3.7 python3.7-dev python3-pip git
RUN python3.7 -m pip install --upgrade pip

# setup the workspace
COPY requirements.txt /requirements.txt

# install Jina and third-party requirements
RUN python3.7 -m pip install -r requirements.txt
RUN python3.7 -m pip install pytest

COPY . /workspace
WORKDIR /workspace

ENV POSTGRES_PASSWORD=123456

RUN nohup bash -c "docker-entrypoint.sh postgres &" && sleep 3

FROM base as test
RUN echo no tests here. Check integration tests

FROM base as entrypoint
EXPOSE 5432
ENTRYPOINT ["bash", "start.sh"]

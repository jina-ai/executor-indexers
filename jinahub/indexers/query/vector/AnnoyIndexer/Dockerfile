FROM jinaai/jina:master

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

ENTRYPOINT ["jina", "pod", "--uses", "config.yml"]

FROM jinaai/jina:master

# install the third-party requirements
RUN apt-get update && apt-get install --no-install-recommends -y gcc build-essential git

# setup the workspace
COPY . /workspace
WORKDIR /workspace

RUN pip install -r requirements.txt

# no tests here. Check integration tests

ENTRYPOINT ["jina", "pod", "--uses", "config.yml"]

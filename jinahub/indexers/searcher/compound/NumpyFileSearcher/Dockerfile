FROM jinaai/jina:2.0.0rc9

# install the third-party requirements
RUN apt-get update && apt-get install --no-install-recommends -y gcc build-essential git

# setup the workspace
COPY . /workspace
WORKDIR /workspace

RUN pip install -r requirements.txt

# no tests here. Check integration tests

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]

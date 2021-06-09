FROM jinaai/jina:1.2.0

# install the third-party requirements
RUN apt-get update && apt-get install --no-install-recommends -y gcc build-essential git

# setup the workspace
COPY . /workspace
WORKDIR /workspace

RUN pip install -r requirements.txt

# for testing the image
RUN pip install pytest && pytest -s -v tests

ENTRYPOINT ["jina", "pod", "--uses", "config.yml"]

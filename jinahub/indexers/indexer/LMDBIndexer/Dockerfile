FROM jinaai/jina:1.2.0

# install the third-party requirements
RUN apt-get update && apt-get install -y python3.7 python3.7-dev python3-pip git

# setup the workspace
COPY . /workspace
WORKDIR /workspace

RUN python3.7 -m pip install -r requirements.txt

# for testing the image
RUN python3.7 -m pip install pytest && python3.7 -m pytest -s -v tests/

ENTRYPOINT ["jina", "pod", "--uses", "config.yml"]

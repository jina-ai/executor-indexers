FROM jinaai/jina:2.0.0rc9 as base
# install the third-party requirements
RUN apt-get update && apt-get install -y python3.7 python3.7-dev python3-pip git

# setup the workspace
COPY . /workspace
WORKDIR /workspace

RUN python3.7 -m pip install -r requirements.txt

# for testing the image
FROM base as test
RUN python3.7 -m pip install pytest && python3.7 -m pytest -s -v tests/

FROM base as entrypoint
ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]

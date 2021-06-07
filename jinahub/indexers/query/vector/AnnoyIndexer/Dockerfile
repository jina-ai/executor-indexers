FROM jinaai/jina:2.0.0

# install the third-party requirements
RUN apt-get update && apt-get install --no-install-recommends -y gcc build-essential
RUN pip install -r requirements.txt

# setup the workspace
COPY . /workspace
WORKDIR /workspace

# for testing the image
RUN pip install pytest && pytest

ENTRYPOINT ["jina", "pod", "--uses", "config.yml"]
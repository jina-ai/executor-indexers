nohup bash -c "/docker-entrypoint.sh postgres &" && \
sleep 3 && \
jina executor --uses config.yml $@

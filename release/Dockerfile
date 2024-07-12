ARG BASE_IMAGE
FROM $BASE_IMAGE

EXPOSE 22
EXPOSE 8088

USER root
RUN apt-get update && apt-get install -qqy openssh-server
USER dev

ARG TURING_PACKAGE
COPY --chown=dev:dev $TURING_PACKAGE.tar.gz /home/dev

USER dev
WORKDIR /home/dev

RUN tar -zxvf $TURING_PACKAGE.tar.gz
RUN rm -rf $TURING_PACKAGE.tar.gz

COPY --chown=dev:dev image/setup.sh /turing/
COPY --chown=dev:dev image/turing.setup.sh /turing/
COPY --chown=dev:dev image/run_jupyter.sh /turing/
COPY --chown=dev:dev turing_key.pub /home/dev/.ssh/authorized_keys
COPY --chown=dev:dev image/run.sh /home/dev/

USER dev
WORKDIR /home/dev
ENTRYPOINT ["/home/dev/run.sh"]

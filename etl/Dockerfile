FROM python:3.7.2

# install mongo import and other required tools
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4
RUN echo "deb http://repo.mongodb.org/apt/debian stretch/mongodb-org/4.0 main" | tee /etc/apt/sources.list.d/mongodb-org-4.0.list
RUN apt-get update && apt-get install -y mongodb-org-tools parallel curl jq default-jre default-jre-headless
RUN pip3 install pandas pyyaml ujson flatten_json

# install neo4j
ENV NEO4J_SHA256=2fdb29816bc72894b10f082b5d542fdaa0cf5d9cfdb899d9aac1e34bc2006250 \
    NEO4J_TARBALL=neo4j-community-3.5.13-unix.tar.gz \
    NEO4J_EDITION=community \
    NEO4J_HOME="/var/lib/neo4j"
ARG NEO4J_URI=https://dist.neo4j.org/neo4j-community-3.5.13-unix.tar.gz
RUN addgroup --system neo4j && adduser --system --no-create-home --home "${NEO4J_HOME}" --ingroup neo4j neo4j
RUN curl --fail --silent --show-error --location --remote-name ${NEO4J_URI} \
    && echo "${NEO4J_SHA256}  ${NEO4J_TARBALL}" | sha256sum -c --strict --quiet \
    && tar --extract --file ${NEO4J_TARBALL} --directory /var/lib \
    && mv /var/lib/neo4j-* "${NEO4J_HOME}" \
    && rm ${NEO4J_TARBALL} \
    && mv "${NEO4J_HOME}"/data /data \
    && mv "${NEO4J_HOME}"/logs /logs \
    && chown -R neo4j:neo4j /data \
    && chmod -R 777 /data \
    && chown -R neo4j:neo4j /logs \
    && chmod -R 777 /logs \
    && chown -R neo4j:neo4j "${NEO4J_HOME}" \
    && chmod -R 777 "${NEO4J_HOME}" \
    && ln -s /data "${NEO4J_HOME}"/data \
    && ln -s /logs "${NEO4J_HOME}"/logs
ENV PATH "${NEO4J_HOME}"/bin:$PATH

# install go lang, silence wget and tar
RUN wget -q  https://dl.google.com/go/go1.12.4.linux-amd64.tar.gz && \
  tar -xf go1.12.4.linux-amd64.tar.gz

# install grip
RUN mkdir -p /go/src/github.com/bmeg
WORKDIR /go/src/github.com/bmeg
ENV GOPATH=/go
ENV PATH="/go/bin:${PATH}"
RUN git clone https://github.com/bmeg/grip
ENV GOPROXY="https://proxy.golang.org"
RUN cd grip && \
  make

COPY docker-start.sh /docker-start.sh
COPY load_database.sh /etl/load_database.sh
WORKDIR /etl
ENTRYPOINT ["/docker-start.sh"]

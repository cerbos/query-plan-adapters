# The test image for the Sequel adapter.
#
# The harnesses read the shared corpus at ../conformance/. Thus you must mount the root
# directory of the repository and not only this directory. Refer to docker-compose.yaml and to
# the instructions in CLAUDE.md.
ARG RUBY_VERSION=4.0
FROM ruby:${RUBY_VERSION}-slim-bookworm

# The version of Sequel to use, as a Gemfile requirement (`= 5.69.0`). CI tests the first
# version of the range in the gemspec and the newest one. Empty, the default, resolves the
# newest 5.x.
ARG SEQUEL_VERSION=
ENV SEQUEL_VERSION=${SEQUEL_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV BUNDLE_PATH=/bundle \
    BUNDLE_APP_CONFIG=/bundle/.bundle \
    LANG=C.UTF-8

WORKDIR /repo/sequel

# Install the gems in a layer of their own. Thus a change to the source does not cause a new
# installation of the gems.
COPY Gemfile cerbos-sequel.gemspec ./
COPY lib/cerbos/sequel/version.rb lib/cerbos/sequel/version.rb
RUN bundle install

COPY . .

COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["bundle", "exec", "rspec"]

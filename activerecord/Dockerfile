# The test image for the ActiveRecord adapter.
#
# The harnesses read the shared corpus at ../conformance/. Thus you must mount the root
# directory of the repository and not only this directory. Refer to docker-compose.yaml and to
# the instructions in CLAUDE.md.
ARG RUBY_VERSION=3.4
FROM ruby:${RUBY_VERSION}-slim-bookworm

# The version of ActiveRecord to use. CI tests the first version and the last version of the
# range in the gemspec. On your computer, the default value is sufficient.
ARG ACTIVERECORD_VERSION=8.0
ENV ACTIVERECORD_VERSION=${ACTIVERECORD_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV BUNDLE_PATH=/bundle \
    BUNDLE_APP_CONFIG=/bundle/.bundle \
    LANG=C.UTF-8

WORKDIR /repo/activerecord

# Install the gems in a layer of their own. Thus a change to the source does not cause a new
# installation of the gems.
COPY Gemfile cerbos-activerecord.gemspec ./
COPY lib/cerbos/active_record/version.rb lib/cerbos/active_record/version.rb
RUN bundle install

COPY . .

COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["bundle", "exec", "rspec"]

# Test image for the ActiveRecord adapter.
# Mount the whole repository, not just this directory: the suites read ../conformance/.
# See docker-compose.yaml.
ARG RUBY_VERSION=4.0
FROM ruby:${RUBY_VERSION}-slim-bookworm

# CI sets this to each end of the gemspec's range. The default is fine locally.
ARG ACTIVERECORD_VERSION=8.0
ENV ACTIVERECORD_VERSION=${ACTIVERECORD_VERSION}

# libpq-dev and default-libmysqlclient-dev build the pg and mysql2 gems, for the conformance
# suite's PostgreSQL and MySQL legs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libpq-dev \
        default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

ENV BUNDLE_PATH=/bundle \
    BUNDLE_APP_CONFIG=/bundle/.bundle \
    LANG=C.UTF-8

WORKDIR /repo/activerecord

# Install gems in their own layer so source changes don't reinstall them.
COPY Gemfile cerbos-activerecord.gemspec ./
COPY lib/cerbos/active_record/version.rb lib/cerbos/active_record/version.rb
RUN bundle install

COPY . .

COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["bundle", "exec", "rspec"]

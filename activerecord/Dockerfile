# Test image for the ActiveRecord adapter.
#
# The harnesses read the shared corpus at ../conformance/, so the REPOSITORY ROOT must be
# mounted (see docker-compose.yaml and the recipe in the repo's CLAUDE.md), not just this
# directory.
ARG RUBY_VERSION=3.4
FROM ruby:${RUBY_VERSION}-slim-bookworm

# Which ActiveRecord to resolve against. CI exercises both ends of the range the gemspec
# declares; locally the default is enough.
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

# Warm the bundle in its own layer so source edits don't re-resolve gems.
COPY Gemfile cerbos-activerecord.gemspec ./
COPY lib/cerbos/active_record/version.rb lib/cerbos/active_record/version.rb
RUN bundle install

COPY . .

COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["bundle", "exec", "rspec"]

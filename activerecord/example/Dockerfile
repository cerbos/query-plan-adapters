# The image for the example application.
#
# The Gemfile gets the adapter from the parent directory. Thus you must mount the root
# directory of the repository. Refer to docker-compose.yaml.
ARG RUBY_VERSION=3.4
FROM ruby:${RUBY_VERSION}-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV BUNDLE_PATH=/bundle \
    BUNDLE_APP_CONFIG=/bundle/.bundle \
    LANG=C.UTF-8

WORKDIR /repo/activerecord/example

# Install the gems in a layer of their own. The adapter has a path dependency. Thus its
# gemspec and its version file must be present during the installation.
COPY example/Gemfile ./
COPY cerbos-activerecord.gemspec /repo/activerecord/
COPY lib/cerbos/active_record/version.rb /repo/activerecord/lib/cerbos/active_record/version.rb
RUN bundle install

COPY . /repo/activerecord/

EXPOSE 4567

CMD ["bundle", "exec", "ruby", "app.rb"]

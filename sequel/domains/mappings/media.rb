# frozen_string_literal: true

module SequelDomains
  module Media
    DB = SequelDomains.db

    class Author < Sequel::Model(DB[:authors]); end

    class Section < Sequel::Model(DB[:sections]); end

    class Article < Sequel::Model(DB[:articles])
      many_to_one :section, class: "SequelDomains::Media::Section", key: :section_slug
      many_to_one :author, class: "SequelDomains::Media::Author"
      many_to_many :contributors, class: "SequelDomains::Media::Author",
        join_table: :article_contributors, left_key: :article_id, right_key: :author_id
    end

    MODEL = Article
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}slug" => F.call("slug"),
      "#{R}status" => F.call("status"),
      "#{R}language" => F.call("language"),
      "#{R}embargoUntil" => F.call("embargo_until"),
      "#{R}publishedAt" => F.call("published_at"),
      "#{R}paywalled" => F.call("paywalled"),
      "#{R}wordCount" => F.call("word_count"),
      # text[] on PostgreSQL, a JSON array elsewhere: no association to map it onto.
      "#{R}keywords" => F.call("keywords"),
      "#{R}section.slug" => F.call("section.slug"),
      "#{R}section.desk" => F.call("section.desk"),
      "#{R}author.id" => F.call("author.id"),
      "#{R}author.staff" => F.call("author.staff"),
      "#{R}contributors" => Cerbos::Sequel.association(:contributors,
        fields: {"id" => F.call("id"), "staff" => F.call("staff")})
    }.freeze
  end
end

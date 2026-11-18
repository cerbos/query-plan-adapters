# frozen_string_literal: true

module ArDomains
  module Media
    class Author < ActiveRecord::Base; end

    class Section < ActiveRecord::Base
      self.primary_key = "slug"
    end

    class ArticleContributor < ActiveRecord::Base
      belongs_to :author
    end

    class Article < ActiveRecord::Base
      belongs_to :section, foreign_key: :section_slug
      belongs_to :author
      has_many :article_contributors
      has_many :contributors, through: :article_contributors, source: :author
    end

    MODEL = Article
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}slug" => F.call("slug"),
      "#{R}status" => F.call("status"),
      "#{R}language" => F.call("language"),
      "#{R}embargoUntil" => F.call("embargo_until"),
      "#{R}publishedAt" => F.call("published_at"),
      "#{R}paywalled" => F.call("paywalled"),
      "#{R}wordCount" => F.call("word_count"),
      # text[] on PostgreSQL, a JSON array elsewhere: no relation to map it onto.
      "#{R}keywords" => F.call("keywords"),
      "#{R}section.slug" => F.call("section.slug"),
      "#{R}section.desk" => F.call("section.desk"),
      "#{R}author.id" => F.call("author.id"),
      "#{R}author.staff" => F.call("author.staff"),
      "#{R}contributors" => Cerbos::ActiveRecord.relation(:contributors, order_field: "id",
        fields: {"id" => F.call("id"), "staff" => F.call("staff")})
    }.freeze
  end
end

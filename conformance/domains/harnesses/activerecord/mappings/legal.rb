# frozen_string_literal: true

module ArDomains
  module Legal
    class Client < ActiveRecord::Base; end

    class MatterTeam < ActiveRecord::Base
      self.table_name = "matter_team"
    end

    class MatterScreen < ActiveRecord::Base; end

    class Matter < ActiveRecord::Base
      belongs_to :client
      has_many :team, class_name: "MatterTeam"
      has_many :screens, class_name: "MatterScreen"
    end

    class DocumentShare < ActiveRecord::Base; end

    class Document < ActiveRecord::Base
      belongs_to :matter
      has_many :shares, class_name: "DocumentShare"
      has_many :matter_team, through: :matter, source: :team
      has_many :matter_screens, through: :matter, source: :screens
    end

    MODEL = Document
    NULLS = :explicit
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}title" => F.call("title"),
      "#{R}privilege" => F.call("privilege"),
      "#{R}classification" => F.call("classification"),
      "#{R}legalHold" => F.call("legal_hold"),
      "#{R}createdBy" => F.call("created_by"),
      "#{R}retentionUntil" => F.call("retention_until"),
      "#{R}shares" => Cerbos::ActiveRecord.relation(:shares, order_field: "id",
        fields: {"partyType" => F.call("party_type"), "partyId" => F.call("party_id")}),
      "#{R}matter.status" => F.call("matter.status"),
      "#{R}matter.practiceArea" => F.call("matter.practice_area"),
      "#{R}matter.leadPartnerId" => F.call("matter.lead_partner_id"),
      "#{R}matter.team" => Cerbos::ActiveRecord.relation(:matter_team, member_field: "user_id", order_field: "user_id"),
      "#{R}matter.screened" => Cerbos::ActiveRecord.relation(:matter_screens, member_field: "user_id", order_field: "user_id"),
      "#{R}matter.client.id" => F.call("matter.client.id"),
      "#{R}matter.client.industry" => F.call("matter.client.industry")
    }.freeze
  end
end

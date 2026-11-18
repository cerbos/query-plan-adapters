# frozen_string_literal: true

module SequelDomains
  module Legal
    DB = SequelDomains.db

    class Client < Sequel::Model(DB[:clients]); end

    class MatterTeam < Sequel::Model(DB[:matter_team]); end

    class MatterScreen < Sequel::Model(DB[:matter_screens]); end

    class Matter < Sequel::Model(DB[:matters])
      many_to_one :client, class: "SequelDomains::Legal::Client"
    end

    class DocumentShare < Sequel::Model(DB[:document_shares]); end

    class Document < Sequel::Model(DB[:documents])
      many_to_one :matter, class: "SequelDomains::Legal::Matter"
      one_to_many :shares, class: "SequelDomains::Legal::DocumentShare", key: :document_id
      # The matter's lists, keyed straight off the document's foreign key.
      one_to_many :matter_team, class: "SequelDomains::Legal::MatterTeam", key: :matter_id, primary_key: :matter_id
      one_to_many :matter_screens, class: "SequelDomains::Legal::MatterScreen", key: :matter_id, primary_key: :matter_id
    end

    MODEL = Document
    NULLS = :explicit
    F = Cerbos::Sequel.method(:field)
    # The call-level :explicit does not reach an undeclared attribute's eq/ne/in here, so each
    # nullable column declares it.
    X = ->(path) { Cerbos::Sequel.field(path, null_representation: :explicit) }
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}title" => F.call("title"),
      "#{R}privilege" => X.call("privilege"),
      "#{R}classification" => F.call("classification"),
      "#{R}legalHold" => F.call("legal_hold"),
      "#{R}createdBy" => F.call("created_by"),
      "#{R}retentionUntil" => X.call("retention_until"),
      "#{R}shares" => Cerbos::Sequel.association(:shares,
        fields: {"partyType" => F.call("party_type"), "partyId" => F.call("party_id")}),
      "#{R}matter.status" => F.call("matter.status"),
      "#{R}matter.practiceArea" => F.call("matter.practice_area"),
      "#{R}matter.leadPartnerId" => X.call("matter.lead_partner_id"),
      "#{R}matter.team" => Cerbos::Sequel.association(:matter_team, member_field: "user_id"),
      "#{R}matter.screened" => Cerbos::Sequel.association(:matter_screens, member_field: "user_id"),
      "#{R}matter.client.id" => F.call("matter.client.id"),
      "#{R}matter.client.industry" => X.call("matter.client.industry")
    }.freeze
  end
end

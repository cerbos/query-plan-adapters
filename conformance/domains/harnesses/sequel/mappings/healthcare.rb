# frozen_string_literal: true

module SequelDomains
  module Healthcare
    DB = SequelDomains.db

    class Hospital < Sequel::Model(DB[:hospitals]); end

    class Ward < Sequel::Model(DB[:wards])
      many_to_one :hospital, class: "SequelDomains::Healthcare::Hospital"
    end

    class Patient < Sequel::Model(DB[:patients])
      many_to_one :ward, class: "SequelDomains::Healthcare::Ward"
    end

    class NoteCareTeam < Sequel::Model(DB[:note_care_team]); end

    class NoteCode < Sequel::Model(DB[:note_codes]); end

    class ClinicalNote < Sequel::Model(DB[:clinical_notes])
      many_to_one :patient, class: "SequelDomains::Healthcare::Patient"
      one_to_many :care_team, class: "SequelDomains::Healthcare::NoteCareTeam", key: :note_id
      one_to_many :codes, class: "SequelDomains::Healthcare::NoteCode", key: :note_id
    end

    MODEL = ClinicalNote
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}noteType" => F.call("type"),
      "#{R}authorId" => F.call("author_id"),
      "#{R}sensitivity" => F.call("sensitivity"),
      "#{R}signedAt" => F.call("signed_at"),
      "#{R}deleted" => F.call("is_deleted"),
      "#{R}careTeam" => Cerbos::Sequel.association(:care_team, member_field: "clinician_id"),
      "#{R}codes" => Cerbos::Sequel.association(:codes, fields: {"system" => F.call("system"), "code" => F.call("code")}),
      "#{R}patient.mrn" => F.call("patient.mrn"),
      "#{R}patient.consentResearch" => F.call("patient.consent_research"),
      "#{R}patient.vip" => F.call("patient.vip"),
      "#{R}patient.birthYear" => F.call("patient.birth_year"),
      "#{R}patient.ward.id" => F.call("patient.ward.id"),
      "#{R}patient.ward.secure" => F.call("patient.ward.secure"),
      "#{R}patient.ward.hospital.region" => F.call("patient.ward.hospital.region")
    }.freeze
  end
end

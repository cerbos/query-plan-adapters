# frozen_string_literal: true

module ArDomains
  module Healthcare
    class Hospital < ActiveRecord::Base; end

    class Ward < ActiveRecord::Base
      belongs_to :hospital
    end

    class Patient < ActiveRecord::Base
      belongs_to :ward, optional: true
    end

    class NoteCareTeam < ActiveRecord::Base
      self.table_name = "note_care_team"
    end

    class NoteCode < ActiveRecord::Base; end

    class ClinicalNote < ActiveRecord::Base
      self.primary_key = "note_id"
      self.inheritance_column = nil
      belongs_to :patient
      has_many :care_team, class_name: "NoteCareTeam", foreign_key: :note_id
      has_many :codes, class_name: "NoteCode", foreign_key: :note_id
    end

    MODEL = ClinicalNote
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}noteType" => F.call("type"),
      "#{R}authorId" => F.call("author_id"),
      "#{R}sensitivity" => F.call("sensitivity"),
      "#{R}signedAt" => F.call("signed_at"),
      "#{R}deleted" => F.call("is_deleted"),
      "#{R}careTeam" => Cerbos::ActiveRecord.relation(:care_team, member_field: "clinician_id"),
      "#{R}codes" => Cerbos::ActiveRecord.relation(:codes, order_field: "id",
        fields: {"system" => F.call("system"), "code" => F.call("code")}),
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

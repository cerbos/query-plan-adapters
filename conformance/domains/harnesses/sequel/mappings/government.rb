# frozen_string_literal: true

module SequelDomains
  module Government
    DB = SequelDomains.db

    class Agency < Sequel::Model(DB[:agencies]); end

    class Applicant < Sequel::Model(DB[:applicants]); end

    class CaseFlag < Sequel::Model(DB[:case_flags]); end

    class Case < Sequel::Model(DB[:cases])
      many_to_one :agency, class: "SequelDomains::Government::Agency", key: :agency_code
      many_to_one :applicant, class: "SequelDomains::Government::Applicant"
      one_to_many :flags, class: "SequelDomains::Government::CaseFlag", key: :case_id
    end

    MODEL = Case
    NULLS = :explicit
    F = Cerbos::Sequel.method(:field)
    # The call-level :explicit does not reach an undeclared attribute's eq/ne/in here, so each
    # nullable column declares it.
    X = ->(path) { Cerbos::Sequel.field(path, null_representation: :explicit) }
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}caseNumber" => F.call("case_number"),
      "#{R}status" => F.call("status"),
      "#{R}clearanceLevel" => F.call("clearance_level"),
      "#{R}program" => F.call("program"),
      "#{R}openedOn" => F.call("opened_on"),
      "#{R}closedOn" => X.call("closed_on"),
      "#{R}pii" => F.call("pii"),
      "#{R}assignedTo" => X.call("assigned_to"),
      "#{R}monthlyAmount" => X.call("monthly_amount"),
      "#{R}flags" => Cerbos::Sequel.association(:flags, member_field: "flag"),
      "#{R}agency.code" => F.call("agency.code"),
      "#{R}agency.state" => F.call("agency.state"),
      "#{R}applicant.countyFips" => F.call("applicant.county_fips"),
      "#{R}applicant.householdSize" => F.call("applicant.household_size"),
      "#{R}applicant.citizenship" => X.call("applicant.citizenship")
    }.freeze
  end
end

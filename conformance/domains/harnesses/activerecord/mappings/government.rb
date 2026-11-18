# frozen_string_literal: true

module ArDomains
  module Government
    class Agency < ActiveRecord::Base
      self.table_name = "agencies"
      self.primary_key = "code"
    end

    class Applicant < ActiveRecord::Base; end

    class CaseFlag < ActiveRecord::Base; end

    class Case < ActiveRecord::Base
      belongs_to :agency, foreign_key: :agency_code
      belongs_to :applicant
      has_many :flags, class_name: "CaseFlag"
    end

    MODEL = Case
    NULLS = :explicit
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}caseNumber" => F.call("case_number"),
      "#{R}status" => F.call("status"),
      "#{R}clearanceLevel" => F.call("clearance_level"),
      "#{R}program" => F.call("program"),
      "#{R}openedOn" => F.call("opened_on"),
      "#{R}closedOn" => F.call("closed_on"),
      "#{R}pii" => F.call("pii"),
      "#{R}assignedTo" => F.call("assigned_to"),
      "#{R}monthlyAmount" => F.call("monthly_amount"),
      "#{R}flags" => Cerbos::ActiveRecord.relation(:flags, member_field: "flag", order_field: "flag"),
      "#{R}agency.code" => F.call("agency.code"),
      "#{R}agency.state" => F.call("agency.state"),
      "#{R}applicant.countyFips" => F.call("applicant.county_fips"),
      "#{R}applicant.householdSize" => F.call("applicant.household_size"),
      "#{R}applicant.citizenship" => F.call("applicant.citizenship")
    }.freeze
  end
end

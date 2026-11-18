# frozen_string_literal: true

module SequelDomains
  module Hr
    DB = SequelDomains.db

    class Department < Sequel::Model(DB[:departments]); end

    class Location < Sequel::Model(DB[:locations]); end

    class EmployeeSkill < Sequel::Model(DB[:employee_skills]); end

    class Employee < Sequel::Model(DB[:employees])
      many_to_one :manager, class: "SequelDomains::Hr::Employee"
      many_to_one :department, class: "SequelDomains::Hr::Department"
      many_to_one :location, class: "SequelDomains::Hr::Location", key: :location_code
      one_to_many :skills, class: "SequelDomains::Hr::EmployeeSkill", key: :employee_id
    end

    MODEL = Employee
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "request.resource.id" => F.call("id"),
      "#{R}level" => F.call("level"),
      "#{R}employmentType" => F.call("employment_type"),
      "#{R}salaryBand" => F.call("salary_band"),
      "#{R}hiredOn" => F.call("hired_on"),
      "#{R}terminatedOn" => F.call("terminated_on"),
      "#{R}email" => F.call("work_email"),
      "#{R}skills" => Cerbos::Sequel.association(:skills, member_field: "skill"),
      "#{R}department.id" => F.call("department.id"),
      "#{R}department.costCenter" => F.call("department.cost_center"),
      "#{R}location.country" => F.call("location.country"),
      "#{R}location.city" => F.call("location.city"),
      "#{R}manager.id" => F.call("manager.id"),
      "#{R}manager.manager.id" => F.call("manager.manager.id")
    }.freeze
  end
end

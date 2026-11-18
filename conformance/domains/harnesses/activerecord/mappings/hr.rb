# frozen_string_literal: true

module ArDomains
  module Hr
    class Department < ActiveRecord::Base; end

    class Location < ActiveRecord::Base
      self.primary_key = "code"
    end

    class EmployeeSkill < ActiveRecord::Base; end

    class Employee < ActiveRecord::Base
      belongs_to :manager, class_name: "Employee", optional: true
      belongs_to :department
      belongs_to :location, foreign_key: :location_code
      has_many :skills, class_name: "EmployeeSkill"
    end

    MODEL = Employee
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "request.resource.id" => F.call("id"),
      "#{R}level" => F.call("level"),
      "#{R}employmentType" => F.call("employment_type"),
      "#{R}salaryBand" => F.call("salary_band"),
      "#{R}hiredOn" => F.call("hired_on"),
      "#{R}terminatedOn" => F.call("terminated_on"),
      "#{R}email" => F.call("work_email"),
      "#{R}skills" => Cerbos::ActiveRecord.relation(:skills, member_field: "skill", order_field: "skill"),
      "#{R}department.id" => F.call("department.id"),
      "#{R}department.costCenter" => F.call("department.cost_center"),
      "#{R}location.country" => F.call("location.country"),
      "#{R}location.city" => F.call("location.city"),
      "#{R}manager.id" => F.call("manager.id"),
      "#{R}manager.manager.id" => F.call("manager.manager.id")
    }.freeze
  end
end

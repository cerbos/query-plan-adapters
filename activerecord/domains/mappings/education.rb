# frozen_string_literal: true

module ArDomains
  module Education
    class Department < ActiveRecord::Base; end

    class CourseStaff < ActiveRecord::Base
      self.table_name = "course_staff"
    end

    class Course < ActiveRecord::Base
      belongs_to :department
      has_many :staff, class_name: "CourseStaff"
    end

    class Assignment < ActiveRecord::Base
      belongs_to :course
      has_many :staff, through: :course
    end

    class SubmissionMember < ActiveRecord::Base; end

    class Submission < ActiveRecord::Base
      belongs_to :assignment
      has_many :members, class_name: "SubmissionMember"
      # The adapter's top-level relation must be a collection, so the to-one chain
      # assignment -> course -> staff is one has_many :through.
      has_many :course_staff, through: :assignment, source: :staff
    end

    MODEL = Submission
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}studentId" => F.call("student_id"),
      "#{R}submittedAt" => F.call("submitted_at"),
      "#{R}score" => F.call("score"),
      "#{R}late" => F.call("late"),
      "#{R}plagiarismScore" => F.call("plagiarism_score"),
      "#{R}status" => F.call("status"),
      "#{R}locked" => F.call("locked"),
      "#{R}members" => Cerbos::ActiveRecord.relation(:members, member_field: "student_id", order_field: "student_id"),
      "#{R}assignment.dueAt" => F.call("assignment.due_at"),
      "#{R}assignment.maxPoints" => F.call("assignment.max_points"),
      "#{R}assignment.groupWork" => F.call("assignment.group_work"),
      "#{R}assignment.course.code" => F.call("assignment.course.code"),
      "#{R}assignment.course.term" => F.call("assignment.course.term"),
      "#{R}assignment.course.department.id" => F.call("assignment.course.department.id"),
      "#{R}assignment.course.staff" => Cerbos::ActiveRecord.relation(:course_staff, order_field: "id",
        fields: {"userId" => F.call("user_id"), "role" => F.call("role")})
    }.freeze
  end
end

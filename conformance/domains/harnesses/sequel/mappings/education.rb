# frozen_string_literal: true

module SequelDomains
  module Education
    DB = SequelDomains.db

    class Department < Sequel::Model(DB[:departments]); end

    class CourseStaff < Sequel::Model(DB[:course_staff]); end

    class Course < Sequel::Model(DB[:courses])
      many_to_one :department, class: "SequelDomains::Education::Department"
    end

    class Assignment < Sequel::Model(DB[:assignments])
      many_to_one :course, class: "SequelDomains::Education::Course"
    end

    class SubmissionMember < Sequel::Model(DB[:submission_members]); end

    class Submission < Sequel::Model(DB[:submissions])
      many_to_one :assignment, class: "SequelDomains::Education::Assignment"
      one_to_many :members, class: "SequelDomains::Education::SubmissionMember", key: :submission_id
      # A collection mapping must start at a collection, so the to-one chain
      # assignment -> course -> staff is spelled as a many_to_many through `assignments`.
      many_to_many :course_staff, class: "SequelDomains::Education::CourseStaff",
        join_table: :assignments, left_key: :id, left_primary_key: :assignment_id,
        right_key: :course_id, right_primary_key: :course_id
    end

    MODEL = Submission
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}studentId" => F.call("student_id"),
      "#{R}submittedAt" => F.call("submitted_at"),
      "#{R}score" => F.call("score"),
      "#{R}late" => F.call("late"),
      "#{R}plagiarismScore" => F.call("plagiarism_score"),
      "#{R}status" => F.call("status"),
      "#{R}locked" => F.call("locked"),
      "#{R}members" => Cerbos::Sequel.association(:members, member_field: "student_id"),
      "#{R}assignment.dueAt" => F.call("assignment.due_at"),
      "#{R}assignment.maxPoints" => F.call("assignment.max_points"),
      "#{R}assignment.groupWork" => F.call("assignment.group_work"),
      "#{R}assignment.course.code" => F.call("assignment.course.code"),
      "#{R}assignment.course.term" => F.call("assignment.course.term"),
      "#{R}assignment.course.department.id" => F.call("assignment.course.department.id"),
      "#{R}assignment.course.staff" => Cerbos::Sequel.association(:course_staff,
        fields: {"userId" => F.call("user_id"), "role" => F.call("role")})
    }.freeze
  end
end

# frozen_string_literal: true

module ArDomains
  module Fintech
    class Branch < ActiveRecord::Base
      self.primary_key = "code"
    end

    class Customer < ActiveRecord::Base
      belongs_to :branch, foreign_key: :branch_code
    end

    class Account < ActiveRecord::Base
      belongs_to :customer
    end

    class PaymentFlag < ActiveRecord::Base; end

    class PaymentApproval < ActiveRecord::Base; end

    class Payment < ActiveRecord::Base
      belongs_to :account
      has_many :flags, class_name: "PaymentFlag"
      has_many :approvals, class_name: "PaymentApproval"
    end

    MODEL = Payment
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}amount" => F.call("amount"),
      "#{R}currency" => F.call("currency"),
      "#{R}status" => F.call("status"),
      "#{R}riskScore" => F.call("risk_score"),
      "#{R}createdAt" => F.call("created_at"),
      "#{R}initiatedBy" => F.call("initiated_by"),
      "#{R}counterparty.name" => F.call("cp_name"),
      "#{R}counterparty.iban" => F.call("cp_iban"),
      "#{R}counterparty.country" => F.call("cp_country"),
      "#{R}flags" => Cerbos::ActiveRecord.relation(:flags, member_field: "flag", order_field: "flag"),
      "#{R}approvals" => Cerbos::ActiveRecord.relation(:approvals, order_field: "id",
        fields: {"approverId" => F.call("approver_id"), "decision" => F.call("decision")}),
      "#{R}account.currency" => F.call("account.currency"),
      "#{R}account.status" => F.call("account.status"),
      "#{R}account.overdraftLimit" => F.call("account.overdraft_limit"),
      "#{R}account.customer.id" => F.call("account.customer.id"),
      "#{R}account.customer.kycLevel" => F.call("account.customer.kyc_level"),
      "#{R}account.customer.pep" => F.call("account.customer.pep"),
      "#{R}account.customer.branch.code" => F.call("account.customer.branch.code"),
      "#{R}account.customer.branch.country" => F.call("account.customer.branch.country")
    }.freeze
  end
end

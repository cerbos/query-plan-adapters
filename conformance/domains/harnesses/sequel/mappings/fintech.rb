# frozen_string_literal: true

module SequelDomains
  module Fintech
    DB = SequelDomains.db

    class Branch < Sequel::Model(DB[:branches]); end

    class Customer < Sequel::Model(DB[:customers])
      many_to_one :branch, class: "SequelDomains::Fintech::Branch", key: :branch_code
    end

    class Account < Sequel::Model(DB[:accounts])
      many_to_one :customer, class: "SequelDomains::Fintech::Customer"
    end

    class PaymentFlag < Sequel::Model(DB[:payment_flags]); end

    class PaymentApproval < Sequel::Model(DB[:payment_approvals]); end

    class Payment < Sequel::Model(DB[:payments])
      many_to_one :account, class: "SequelDomains::Fintech::Account"
      one_to_many :flags, class: "SequelDomains::Fintech::PaymentFlag", key: :payment_id
      one_to_many :approvals, class: "SequelDomains::Fintech::PaymentApproval", key: :payment_id
    end

    MODEL = Payment
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
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
      "#{R}flags" => Cerbos::Sequel.association(:flags, member_field: "flag"),
      "#{R}approvals" => Cerbos::Sequel.association(:approvals,
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

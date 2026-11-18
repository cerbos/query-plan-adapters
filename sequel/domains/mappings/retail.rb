# frozen_string_literal: true

module SequelDomains
  module Retail
    DB = SequelDomains.db

    class Store < Sequel::Model(DB[:stores]); end

    class Customer < Sequel::Model(DB[:customers]); end

    class OrderLine < Sequel::Model(DB[:order_line]); end

    class Order < Sequel::Model(DB[:order])
      many_to_one :customer, class: "SequelDomains::Retail::Customer"
      many_to_one :store, class: "SequelDomains::Retail::Store"
      one_to_many :lines, class: "SequelDomains::Retail::OrderLine", key: :order_id
    end

    MODEL = Order
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}status" => F.call("status"),
      "#{R}channel" => F.call("channel"),
      "#{R}totalCents" => F.call("total_cents"),
      "#{R}currency" => F.call("currency"),
      "#{R}couponCode" => F.call("coupon_code"),
      "#{R}placedAt" => F.call("placed_at"),
      "#{R}isGift" => F.call("gift"),
      "#{R}customer.id" => F.call("customer.id"),
      "#{R}customer.tier" => F.call("customer.tier"),
      "#{R}customer.loyaltyPoints" => F.call("customer.loyalty_points"),
      "#{R}store.region" => F.call("store.region"),
      "#{R}store.country" => F.call("store.country"),
      "#{R}lines" => Cerbos::Sequel.association(:lines, fields: {
        "sku" => F.call("sku"), "category" => F.call("category"),
        "quantity" => F.call("quantity"), "unitPriceCents" => F.call("unit_price_cents")
      })
    }.freeze
  end
end

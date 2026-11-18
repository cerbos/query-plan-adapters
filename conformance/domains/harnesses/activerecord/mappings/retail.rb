# frozen_string_literal: true

module ArDomains
  module Retail
    class Store < ActiveRecord::Base; end

    class Customer < ActiveRecord::Base; end

    class OrderLine < ActiveRecord::Base
      self.table_name = "order_line"
    end

    class Order < ActiveRecord::Base
      self.table_name = "order"
      belongs_to :customer
      belongs_to :store, optional: true
      has_many :lines, class_name: "OrderLine"
    end

    MODEL = Order
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
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
      "#{R}lines" => Cerbos::ActiveRecord.relation(:lines, order_field: "line_no", fields: {
        "sku" => F.call("sku"), "category" => F.call("category"),
        "quantity" => F.call("quantity"), "unitPriceCents" => F.call("unit_price_cents")
      })
    }.freeze
  end
end

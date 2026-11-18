# frozen_string_literal: true

module ArDomains
  module Logistics
    class Hub < ActiveRecord::Base; end

    class Carrier < ActiveRecord::Base; end

    class ShipmentHandling < ActiveRecord::Base
      self.table_name = "shipment_handling"
    end

    class Checkpoint < ActiveRecord::Base
      belongs_to :hub
    end

    class Shipment < ActiveRecord::Base
      belongs_to :origin, class_name: "Hub", foreign_key: :origin_hub_id
      belongs_to :destination, class_name: "Hub", foreign_key: :destination_hub_id
      belongs_to :carrier, optional: true
      has_many :handling, class_name: "ShipmentHandling"
      has_many :checkpoints
    end

    MODEL = Shipment
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}trackingNumber" => F.call("tracking_no"),
      "#{R}status" => F.call("status"),
      "#{R}weightKg" => F.call("weight_kg"),
      "#{R}hazmat" => F.call("hazmat"),
      "#{R}customsHold" => F.call("customs_hold"),
      "#{R}shipper" => F.call("shipper_account"),
      "#{R}eta" => F.call("eta"),
      "#{R}declaredValue" => F.call("declared_value"),
      "#{R}origin.code" => F.call("origin.code"),
      "#{R}origin.country" => F.call("origin.country"),
      "#{R}origin.region" => F.call("origin.region"),
      "#{R}destination.code" => F.call("destination.code"),
      "#{R}destination.country" => F.call("destination.country"),
      "#{R}destination.region" => F.call("destination.region"),
      "#{R}carrier.scac" => F.call("carrier.scac"),
      "#{R}handling" => Cerbos::ActiveRecord.relation(:handling, member_field: "code", order_field: "code"),
      "#{R}checkpoints" => Cerbos::ActiveRecord.relation(:checkpoints, order_field: "scanned_at", fields: {
        "event" => F.call("event"), "scannedAt" => F.call("scanned_at"), "hub.code" => F.call("hub.code")
      })
    }.freeze
  end
end

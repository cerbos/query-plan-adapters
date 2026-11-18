# frozen_string_literal: true

module SequelDomains
  module Logistics
    DB = SequelDomains.db

    class Hub < Sequel::Model(DB[:hubs]); end

    class Carrier < Sequel::Model(DB[:carriers]); end

    class ShipmentHandling < Sequel::Model(DB[:shipment_handling]); end

    class Checkpoint < Sequel::Model(DB[:checkpoints])
      many_to_one :hub, class: "SequelDomains::Logistics::Hub"
    end

    class Shipment < Sequel::Model(DB[:shipments])
      many_to_one :origin, class: "SequelDomains::Logistics::Hub", key: :origin_hub_id
      many_to_one :destination, class: "SequelDomains::Logistics::Hub", key: :destination_hub_id
      many_to_one :carrier, class: "SequelDomains::Logistics::Carrier"
      one_to_many :handling, class: "SequelDomains::Logistics::ShipmentHandling", key: :shipment_id
      one_to_many :checkpoints, class: "SequelDomains::Logistics::Checkpoint", key: :shipment_id
    end

    MODEL = Shipment
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
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
      "#{R}handling" => Cerbos::Sequel.association(:handling, member_field: "code"),
      "#{R}checkpoints" => Cerbos::Sequel.association(:checkpoints, fields: {
        "event" => F.call("event"), "scannedAt" => F.call("scanned_at"), "hub.code" => F.call("hub.code")
      })
    }.freeze
  end
end

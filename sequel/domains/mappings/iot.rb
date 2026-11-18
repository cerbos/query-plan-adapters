# frozen_string_literal: true

module SequelDomains
  module Iot
    DB = SequelDomains.db

    class Org < Sequel::Model(DB[:orgs]); end

    class Site < Sequel::Model(DB[:sites])
      many_to_one :org, class: "SequelDomains::Iot::Org"
    end

    class DeviceAlert < Sequel::Model(DB[:device_alerts]); end

    class Device < Sequel::Model(DB[:devices])
      many_to_one :site, class: "SequelDomains::Iot::Site"
      one_to_many :alerts, class: "SequelDomains::Iot::DeviceAlert", key: :device_id
    end

    MODEL = Device
    NULLS = :omitted
    F = Cerbos::Sequel.method(:field)
    R = "request.resource.attr."

    ATTRIBUTES = {
      "#{R}serial" => F.call("serial"),
      "#{R}model" => F.call("model"),
      "#{R}firmware" => F.call("firmware"),
      "#{R}lastSeenAt" => F.call("last_seen_at"),
      "#{R}online" => F.call("online"),
      "#{R}batteryPct" => F.call("battery_pct"),
      # A JSON object column. The adapter has no JSON-path mapping, so `labels.env` and
      # `labels.restricted` cannot be mapped.
      "#{R}labels" => F.call("labels"),
      "#{R}decommissioned" => F.call("decommissioned"),
      "#{R}site.region" => F.call("site.region"),
      "#{R}site.org.id" => F.call("site.org.id"),
      "#{R}site.org.plan" => F.call("site.org.plan"),
      "#{R}alerts" => Cerbos::Sequel.association(:alerts, fields: {
        "severity" => F.call("severity"), "acknowledged" => F.call("acknowledged"), "code" => F.call("code")
      })
    }.freeze
  end
end

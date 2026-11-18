# frozen_string_literal: true

module ArDomains
  module Iot
    class Org < ActiveRecord::Base; end

    class Site < ActiveRecord::Base
      belongs_to :org
    end

    class DeviceAlert < ActiveRecord::Base; end

    class Device < ActiveRecord::Base
      belongs_to :site
      has_many :alerts, class_name: "DeviceAlert"
    end

    MODEL = Device
    NULLS = :omitted
    F = Cerbos::ActiveRecord.method(:field)
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
      "#{R}alerts" => Cerbos::ActiveRecord.relation(:alerts, order_field: "id", fields: {
        "severity" => F.call("severity"), "acknowledged" => F.call("acknowledged"), "code" => F.call("code")
      })
    }.freeze
  end
end

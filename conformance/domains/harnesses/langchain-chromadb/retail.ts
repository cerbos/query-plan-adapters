import { bool, flatten, id, int, json, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("status"),
    req("channel"),
    req("totalCents", int),
    req("currency"),
    opt("couponCode"),
    req("placedAt"),
    req("isGift", bool),
    req("customer.id"),
    req("customer.tier"),
    req("customer.loyaltyPoints", int),
    opt("store.region"),
    opt("store.country"),
    json("lines", { required: true }),
  ]),
  toMetadata: (resource) => flatten(resource),
};

export default mapping;

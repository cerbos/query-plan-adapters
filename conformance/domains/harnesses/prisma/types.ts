import type { Mapper, NullAttributeRepresentation } from "../src";

export interface DomainMapping {
  model: string;
  mapper: Mapper;
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

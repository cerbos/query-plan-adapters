import { PlanKind, PlanResourcesResponse, Value } from "@cerbos/core";
import type { AnyColumn, SQLWrapper } from "drizzle-orm";
type RelationTable = unknown;
export { PlanKind };
export type DrizzleFilter = SQLWrapper;
type ComparisonOperator = "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "in" | "contains" | "startsWith" | "endsWith" | "isSet";
type MapperTransform = (args: {
    operator: ComparisonOperator;
    value: Value;
}) => SQLWrapper;
export type MapperEntry = AnyColumn | SQLWrapper | {
    column?: AnyColumn | SQLWrapper;
    transform?: MapperTransform;
    relation?: RelationMapping;
} | MapperTransform;
export type Mapper = {
    [key: string]: MapperEntry | undefined;
} | ((reference: string) => MapperEntry | undefined);
export interface QueryPlanToDrizzleArgs {
    queryPlan: PlanResourcesResponse;
    mapper: Mapper;
}
export type QueryPlanToDrizzleResult = {
    kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
} | {
    kind: PlanKind.CONDITIONAL;
    filter: DrizzleFilter;
};
export interface RelationMapping {
    type: "one" | "many";
    table: RelationTable;
    sourceColumn: AnyColumn;
    targetColumn: AnyColumn;
    field?: MapperEntry;
    fields?: {
        [key: string]: MapperEntry;
    };
}
export declare function queryPlanToDrizzle({ queryPlan, mapper, }: QueryPlanToDrizzleArgs): QueryPlanToDrizzleResult;
//# sourceMappingURL=index.d.ts.map
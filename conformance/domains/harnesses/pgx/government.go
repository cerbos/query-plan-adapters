package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var Government = func() Domain {
	agency := join{"agencies", "agency_code", "code"}
	applicant := join{"applicants", "applicant_id", "id"}
	a := newAttrs(cerbospgx.NullConventionExplicit).
		col("id", "id", str).
		col("attr.caseNumber", "case_number", str).
		col("attr.status", "status", str).
		col("attr.clearanceLevel", "clearance_level", num).
		col("attr.program", "program", str).
		col("attr.openedOn", "opened_on", ts).
		col("attr.closedOn", "closed_on", ts).
		col("attr.pii", "pii", boo).
		col("attr.assignedTo", "assigned_to", str).
		col("attr.monthlyAmount", "monthly_amount", num).
		list("flags", path(join{"case_flags", "id", "case_id"}), "flag", str).
		via("agency.code", path(agency), "code", str).
		via("agency.state", path(agency), "state", str).
		via("applicant.countyFips", path(applicant), "county_fips", str).
		via("applicant.householdSize", path(applicant), "household_size", num).
		via("applicant.citizenship", path(applicant), "citizenship", str)
	return Domain{Name: "government", Table: "cases", ID: "id", Nulls: cerbospgx.NullExplicit, Mapper: a.m}
}()

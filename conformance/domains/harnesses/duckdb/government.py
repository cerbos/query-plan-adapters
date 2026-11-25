from _kit import BOOL, NUM, STR, TS, Attrs, path

ROOT, ID = "cases_v", "id"

VIEWS = """
CREATE VIEW cases_v AS
SELECT c.*,
    (SELECT coalesce(list(x.flag ORDER BY x.flag), []) FROM case_flags x WHERE x.case_id = c.id)
        AS flags
FROM cases c
"""


def mapping(nulls):
    agency = ("agencies", "agency_code", "code")
    applicant = ("applicants", "applicant_id", "id")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.caseNumber", "case_number", STR)
        .col("attr.status", "status", STR)
        .col("attr.clearanceLevel", "clearance_level", NUM)
        .col("attr.program", "program", STR)
        .col("attr.openedOn", "opened_on", TS)
        .col("attr.closedOn", "closed_on", TS)
        .col("attr.pii", "pii", BOOL)
        .col("attr.assignedTo", "assigned_to", STR)
        .col("attr.monthlyAmount", "monthly_amount", NUM)
        .values("flags", "flags", STR)
        .via("agency.code", path(agency), "code", STR)
        .via("agency.state", path(agency), "state", STR)
        .via("applicant.countyFips", path(applicant), "county_fips", STR)
        .via("applicant.householdSize", path(applicant), "household_size", NUM)
        .via("applicant.citizenship", path(applicant), "citizenship", STR)
        .m
    )

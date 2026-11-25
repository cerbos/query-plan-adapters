from _frames import many, one

R = "request.resource.attr."
ID = "id"
NULLS = "explicit"


def frame(t):
    c = one(
        t["cases"],
        "agency_code",
        t["agencies"],
        "code",
        "agency",
        code="code",
        state="state",
    )
    c = one(
        c,
        "applicant_id",
        t["applicants"],
        "id",
        "applicant",
        countyFips="county_fips",
        householdSize="household_size",
        citizenship="citizenship",
    )
    return many(c, "id", t["case_flags"], "case_id", "flags", "flag", "flag")


ATTR_MAP = {
    R + "caseNumber": "case_number",
    R + "clearanceLevel": "clearance_level",
    R + "openedOn": "opened_on",
    R + "closedOn": "closed_on",
    R + "assignedTo": "assigned_to",
    R + "monthlyAmount": "monthly_amount",
}

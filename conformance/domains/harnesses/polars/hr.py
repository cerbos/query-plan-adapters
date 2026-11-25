from _frames import many, one

R = "request.resource.attr."
ID = "id"


def frame(t):
    employees = t["employees"]
    managers = one(employees, "manager_id", employees, "id", "manager", id="id")
    e = one(
        employees, "manager_id", managers, "id", "manager", id="id", manager="manager"
    )
    e = one(
        e,
        "department_id",
        t["departments"],
        "id",
        "department",
        id="id",
        costCenter="cost_center",
    )
    e = one(
        e,
        "location_code",
        t["locations"],
        "code",
        "location",
        country="country",
        city="city",
    )
    return many(
        e, "id", t["employee_skills"], "employee_id", "skills", "skill", "skill"
    )


ATTR_MAP = {
    R + "employmentType": "employment_type",
    R + "salaryBand": "salary_band",
    R + "hiredOn": "hired_on",
    R + "terminatedOn": "terminated_on",
    R + "email": "work_email",
}

from _kit import NUM, STR, TS, Attrs, path

ROOT, ID = "employees_v", "id"

VIEWS = """
CREATE VIEW employees_v AS
SELECT e.*,
    (SELECT coalesce(list(s.skill ORDER BY s.skill), []) FROM employee_skills s
        WHERE s.employee_id = e.id) AS skills
FROM employees e
"""


def mapping(nulls):
    department = ("departments", "department_id", "id")
    location = ("locations", "location_code", "code")
    manager = ("employees", "manager_id", "id")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.level", "level", NUM)
        .col("attr.employmentType", "employment_type", STR)
        .col("attr.salaryBand", "salary_band", STR)
        .col("attr.hiredOn", "hired_on", TS)
        .col("attr.terminatedOn", "terminated_on", TS)
        .col("attr.email", "work_email", STR)
        .values("skills", "skills", STR)
        .via("department.id", path(department), "id", STR)
        .via("department.costCenter", path(department), "cost_center", STR)
        .via("location.country", path(location), "country", STR)
        .via("location.city", path(location), "city", STR)
        .via("manager.id", path(manager), "id", STR)
        .via("manager.manager.id", path(manager, manager), "id", STR)
        .m
    )

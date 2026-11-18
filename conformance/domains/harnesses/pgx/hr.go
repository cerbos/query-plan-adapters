package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var HR = func() Domain {
	manager := join{"employees", "manager_id", "id"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "id", str).
		col("attr.level", "level", num).
		col("attr.employmentType", "employment_type", str).
		col("attr.salaryBand", "salary_band", str).
		col("attr.hiredOn", "hired_on", ts).
		col("attr.terminatedOn", "terminated_on", ts).
		col("attr.email", "work_email", str).
		list("skills", path(join{"employee_skills", "id", "employee_id"}), "skill", str).
		via("department.id", path(join{"departments", "department_id", "id"}), "id", str).
		via("department.costCenter", path(join{"departments", "department_id", "id"}), "cost_center", str).
		via("location.country", path(join{"locations", "location_code", "code"}), "country", str).
		via("location.city", path(join{"locations", "location_code", "code"}), "city", str).
		via("manager.id", path(manager), "id", str).
		via("manager.manager.id", path(manager, manager), "id", str)
	return Domain{Name: "hr", Table: "employees", ID: "id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()

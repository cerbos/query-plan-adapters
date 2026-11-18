import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function hr(k: Kit): DomainMapping {
  const departments = k.table("departments", {
    id: k.text("id").primaryKey(),
    name: k.text("name").notNull(),
    costCenter: k.text("cost_center").notNull(),
  });

  const locations = k.table("locations", {
    code: k.text("code").primaryKey(),
    country: k.text("country").notNull(),
    city: k.text("city").notNull(),
  });

  const employees: any = k.table("employees", {
    id: k.text("id").primaryKey(),
    managerId: k.text("manager_id").references(() => employees.id),
    departmentId: k.text("department_id").notNull().references(() => departments.id),
    locationCode: k.text("location_code").notNull().references(() => locations.code),
    level: k.int("level").notNull(),
    employmentType: k.text("employment_type").notNull(),
    salaryBand: k.text("salary_band"),
    hiredOn: k.timestamp("hired_on").notNull(),
    terminatedOn: k.timestamp("terminated_on"),
    workEmail: k.text("work_email").notNull(),
  });

  const employeeSkills = k.table(
    "employee_skills",
    { employeeId: k.text("employee_id").notNull().references(() => employees.id), skill: k.text("skill").notNull() },
    ["employeeId", "skill"]
  );

  const schema = {
    departments,
    locations,
    employees,
    employeeSkills,
    employeesRelations: relations(employees, ({ one, many }) => ({
      manager: one(employees, { fields: [employees.managerId], references: [employees.id] }),
      department: one(departments, { fields: [employees.departmentId], references: [departments.id] }),
      location: one(locations, { fields: [employees.locationCode], references: [locations.code] }),
      skills: many(employeeSkills),
    })),
    employeeSkillsRelations: relations(employeeSkills, ({ one }) => ({
      employee: one(employees, { fields: [employeeSkills.employeeId], references: [employees.id] }),
    })),
  };

  return {
    table: employees,
    id: employees.id,
    schema,
    mapper: {
      "request.resource.attr.email": employees.workEmail,
      "request.resource.attr.hiredOn": { column: employees.hiredOn, valueType: "timestamp" },
      "request.resource.attr.terminatedOn": { column: employees.terminatedOn, valueType: "timestamp" },
      "request.resource.attr.skills": {
        collectionValueType: "scalar",
        relation: { name: "skills", field: employeeSkills.skill },
      },
    },
  };
}

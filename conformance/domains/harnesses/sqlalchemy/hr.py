from _types import KEY, INT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import aliased, declarative_base, relationship

from cerbos_sqlalchemy import Relation, related, relation_overrides

Base = declarative_base()


class Department(Base):
    __tablename__ = "departments"
    id = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    cost_center = Column(STR, nullable=False)


class Location(Base):
    __tablename__ = "locations"
    code = Column(KEY, primary_key=True)
    country = Column(STR, nullable=False)
    city = Column(STR, nullable=False)


class Employee(Base):
    __tablename__ = "employees"
    id = Column(KEY, primary_key=True)
    manager_id = Column(KEY, ForeignKey("employees.id"), nullable=True)
    department_id = Column(KEY, ForeignKey("departments.id"), nullable=False)
    location_code = Column(KEY, ForeignKey("locations.code"), nullable=False)
    level = Column(INT, nullable=False)
    employment_type = Column(STR, nullable=False)
    salary_band = Column(STR, nullable=True)
    hired_on = Column(TS, nullable=False)
    terminated_on = Column(TS, nullable=True)
    work_email = Column(STR, nullable=False)
    manager = relationship("Employee", remote_side=[id], viewonly=True)
    department = relationship(Department, viewonly=True)
    location = relationship(Location, viewonly=True)
    skills = relationship("EmployeeSkill", viewonly=True)


class EmployeeSkill(Base):
    __tablename__ = "employee_skills"
    employee_id = Column(KEY, ForeignKey("employees.id"), primary_key=True)
    skill = Column(KEY, primary_key=True)


ROOT = Employee
R = "request.resource.attr."
E = Employee
Manager = aliased(Employee)

INFERRED = {
    R + "email": E.work_email,
    R + "skills": related(E.skills, EmployeeSkill.skill),
    R + "manager.id": E.manager_id,
    R + "manager.manager.id": scalar(E, Manager.manager_id, Manager.id == E.manager_id),
}

DECLARED = {
    "request.resource.id": E.id,
    R + "level": E.level,
    R + "employmentType": E.employment_type,
    R + "salaryBand": E.salary_band,
    R + "hiredOn": E.hired_on,
    R + "terminatedOn": E.terminated_on,
    R + "email": E.work_email,
    R + "skills": Relation(
        [EmployeeSkill.employee_id == E.id], [E], member=EmployeeSkill.skill
    ),
    R + "department.id": E.department_id,
    R + "department.costCenter": scalar(
        E, Department.cost_center, Department.id == E.department_id
    ),
    R + "location.country": scalar(E, Location.country, Location.code == E.location_code),
    R + "location.city": scalar(E, Location.city, Location.code == E.location_code),
    R + "manager.id": E.manager_id,
    R + "manager.manager.id": scalar(E, Manager.manager_id, Manager.id == E.manager_id),
}


def mapping(style, dialect):
    if style == "inferred":
        return {"attr_map": INFERRED}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides()}

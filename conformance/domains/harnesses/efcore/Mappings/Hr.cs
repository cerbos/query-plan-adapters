using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Hr : Domain<Employee>
{
    public override string Name => "hr";
    public override DbContext CreateContext(DbContextOptions options) => new HrContext(options);
    public override Expression<Func<Employee, string>> Id => e => e.Id;

    public override AttributeMap<Employee> Map { get; } = new AttributeMap<Employee>()
        .Field("request.resource.id", e => e.Id)
        .Field("request.resource.attr.level", e => e.Level)
        .Field("request.resource.attr.employmentType", e => e.EmploymentType)
        .Field("request.resource.attr.salaryBand", e => e.SalaryBand)
        .Field("request.resource.attr.hiredOn", e => e.HiredOn)
        .Field("request.resource.attr.terminatedOn", e => e.TerminatedOn)
        .Field("request.resource.attr.email", e => e.WorkEmail)
        .List("request.resource.attr.skills", e => e.Skills, s => s.Skill)
        .Field("request.resource.attr.department.id", e => e.Department.Id)
        .Field("request.resource.attr.department.costCenter", e => e.Department.CostCenter)
        .Field("request.resource.attr.location.country", e => e.Location.Country)
        .Field("request.resource.attr.location.city", e => e.Location.City)
        .Field("request.resource.attr.manager.id", e => e.Manager!.Id)
        .Field("request.resource.attr.manager.manager.id", e => e.Manager!.Manager!.Id);
}

public class HrDepartment
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string CostCenter { get; set; } = "";
}

public class Location
{
    public string Code { get; set; } = "";
    public string Country { get; set; } = "";
    public string City { get; set; } = "";
}

public class Employee
{
    public string Id { get; set; } = "";
    public string? ManagerId { get; set; }
    public Employee? Manager { get; set; }
    public string DepartmentId { get; set; } = "";
    public HrDepartment Department { get; set; } = null!;
    public string LocationCode { get; set; } = "";
    public Location Location { get; set; } = null!;
    public int Level { get; set; }
    public string EmploymentType { get; set; } = "";
    public string? SalaryBand { get; set; }
    public DateTime HiredOn { get; set; }
    public DateTime? TerminatedOn { get; set; }
    public string WorkEmail { get; set; } = "";
    public List<EmployeeSkill> Skills { get; set; } = [];
}

public class EmployeeSkill
{
    public string EmployeeId { get; set; } = "";
    public string Skill { get; set; } = "";
}

public sealed class HrContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<HrDepartment>().ToTable("departments");
        b.Entity<Location>().ToTable("locations").HasKey(x => x.Code);
        b.Entity<Employee>(e =>
        {
            e.ToTable("employees");
            e.HasOne(x => x.Manager).WithMany().HasForeignKey(x => x.ManagerId).OnDelete(DeleteBehavior.NoAction);
            e.HasMany(x => x.Skills).WithOne().HasForeignKey(x => x.EmployeeId);
        });
        b.Entity<EmployeeSkill>().ToTable("employee_skills").HasKey(x => new { x.EmployeeId, x.Skill });
    }
}

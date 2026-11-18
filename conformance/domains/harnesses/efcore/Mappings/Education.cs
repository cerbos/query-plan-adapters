using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Education : Domain<Submission>
{
    public override string Name => "education";
    public override DbContext CreateContext(DbContextOptions options) => new EducationContext(options);
    public override Expression<Func<Submission, string>> Id => s => s.Id;

    public override AttributeMap<Submission> Map { get; } = new AttributeMap<Submission>()
        .Field("request.resource.id", s => s.Id)
        .Field("request.resource.attr.studentId", s => s.StudentId)
        .Field("request.resource.attr.submittedAt", s => s.SubmittedAt)
        .Field("request.resource.attr.score", s => s.Score)
        .Field("request.resource.attr.late", s => s.Late)
        .Field("request.resource.attr.plagiarismScore", s => s.PlagiarismScore)
        .Field("request.resource.attr.status", s => s.Status)
        .Field("request.resource.attr.locked", s => s.Locked)
        .List("request.resource.attr.members", s => s.Members, m => m.StudentId)
        .Field("request.resource.attr.assignment.dueAt", s => s.Assignment.DueAt)
        .Field("request.resource.attr.assignment.maxPoints", s => s.Assignment.MaxPoints)
        .Field("request.resource.attr.assignment.groupWork", s => s.Assignment.GroupWork)
        .Field("request.resource.attr.assignment.course.code", s => s.Assignment.Course.Code)
        .Field("request.resource.attr.assignment.course.term", s => s.Assignment.Course.Term)
        .Field("request.resource.attr.assignment.course.department.id", s => s.Assignment.Course.Department.Id)
        .List("request.resource.attr.assignment.course.staff", s => s.Assignment.Course.Staff, c => c
            .Field("userId", x => x.UserId)
            .Field("role", x => x.Role));
}

public class EduDepartment
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}

public class Course
{
    public string Id { get; set; } = "";
    public string Code { get; set; } = "";
    public string Term { get; set; } = "";
    public string DepartmentId { get; set; } = "";
    public EduDepartment Department { get; set; } = null!;
    public List<CourseStaff> Staff { get; set; } = [];
}

public class CourseStaff
{
    public string Id { get; set; } = "";
    public string CourseId { get; set; } = "";
    public string UserId { get; set; } = "";
    public string Role { get; set; } = "";
}

public class Assignment
{
    public string Id { get; set; } = "";
    public string CourseId { get; set; } = "";
    public Course Course { get; set; } = null!;
    public string Title { get; set; } = "";
    public DateTime DueAt { get; set; }
    public int MaxPoints { get; set; }
    public bool GroupWork { get; set; }
}

public class Submission
{
    public string Id { get; set; } = "";
    public string AssignmentId { get; set; } = "";
    public Assignment Assignment { get; set; } = null!;
    public string StudentId { get; set; } = "";
    public DateTime? SubmittedAt { get; set; }
    public double? Score { get; set; }
    public bool Late { get; set; }
    public double? PlagiarismScore { get; set; }
    public string Status { get; set; } = "";
    public bool Locked { get; set; }
    public List<SubmissionMember> Members { get; set; } = [];
}

public class SubmissionMember
{
    public string SubmissionId { get; set; } = "";
    public string StudentId { get; set; } = "";
}

public sealed class EducationContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<EduDepartment>().ToTable("departments");
        b.Entity<Course>().ToTable("courses").HasMany(x => x.Staff).WithOne().HasForeignKey(x => x.CourseId);
        b.Entity<CourseStaff>().ToTable("course_staff");
        b.Entity<Assignment>().ToTable("assignments");
        b.Entity<Submission>().ToTable("submissions").HasMany(x => x.Members).WithOne().HasForeignKey(x => x.SubmissionId);
        b.Entity<SubmissionMember>().ToTable("submission_members").HasKey(x => new { x.SubmissionId, x.StudentId });
    }
}

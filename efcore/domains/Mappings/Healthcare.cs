using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Healthcare : Domain<ClinicalNote>
{
    public override string Name => "healthcare";
    public override DbContext CreateContext(DbContextOptions options) => new HealthcareContext(options);
    public override Expression<Func<ClinicalNote, string>> Id => n => n.NoteId;

    public override AttributeMap<ClinicalNote> Map { get; } = new AttributeMap<ClinicalNote>()
        .Field("request.resource.id", n => n.NoteId)
        .Field("request.resource.attr.noteType", n => n.Type)
        .Field("request.resource.attr.authorId", n => n.AuthorId)
        .Field("request.resource.attr.sensitivity", n => n.Sensitivity)
        .Field("request.resource.attr.signedAt", n => n.SignedAt)
        .Field("request.resource.attr.deleted", n => n.IsDeleted)
        .List("request.resource.attr.careTeam", n => n.CareTeam, c => c.ClinicianId)
        .List("request.resource.attr.codes", n => n.Codes, c => c
            .Field("system", x => x.System)
            .Field("code", x => x.Code))
        .Field("request.resource.attr.patient.mrn", n => n.Patient.Mrn)
        .Field("request.resource.attr.patient.consentResearch", n => n.Patient.ConsentResearch)
        .Field("request.resource.attr.patient.vip", n => n.Patient.Vip)
        .Field("request.resource.attr.patient.birthYear", n => n.Patient.BirthYear)
        .Field("request.resource.attr.patient.ward.id", n => n.Patient.Ward!.Id)
        .Field("request.resource.attr.patient.ward.secure", n => n.Patient.Ward!.Secure)
        .Field("request.resource.attr.patient.ward.hospital.region", n => n.Patient.Ward!.Hospital.Region);
}

public class Hospital
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Region { get; set; } = "";
}

public class Ward
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string HospitalId { get; set; } = "";
    public Hospital Hospital { get; set; } = null!;
    public bool Secure { get; set; }
}

public class Patient
{
    public string Id { get; set; } = "";
    public string Mrn { get; set; } = "";
    public string? WardId { get; set; }
    public Ward? Ward { get; set; }
    public bool ConsentResearch { get; set; }
    public bool? Vip { get; set; }
    public int BirthYear { get; set; }
}

public class ClinicalNote
{
    public string NoteId { get; set; } = "";
    public string PatientId { get; set; } = "";
    public Patient Patient { get; set; } = null!;
    public string AuthorId { get; set; } = "";
    public string Type { get; set; } = "";
    public int? Sensitivity { get; set; }
    public DateTime? SignedAt { get; set; }
    public bool IsDeleted { get; set; }
    public List<NoteCareTeam> CareTeam { get; set; } = [];
    public List<NoteCode> Codes { get; set; } = [];
}

public class NoteCareTeam
{
    public string NoteId { get; set; } = "";
    public string ClinicianId { get; set; } = "";
}

public class NoteCode
{
    public string Id { get; set; } = "";
    public string NoteId { get; set; } = "";
    public string System { get; set; } = "";
    public string Code { get; set; } = "";
}

public sealed class HealthcareContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<Hospital>().ToTable("hospitals");
        b.Entity<Ward>().ToTable("wards");
        b.Entity<Patient>().ToTable("patients");
        b.Entity<ClinicalNote>(e =>
        {
            e.ToTable("clinical_notes").HasKey(x => x.NoteId);
            e.HasMany(x => x.CareTeam).WithOne().HasForeignKey(x => x.NoteId);
            e.HasMany(x => x.Codes).WithOne().HasForeignKey(x => x.NoteId);
        });
        b.Entity<NoteCareTeam>().ToTable("note_care_team").HasKey(x => new { x.NoteId, x.ClinicianId });
        b.Entity<NoteCode>().ToTable("note_codes");
    }
}

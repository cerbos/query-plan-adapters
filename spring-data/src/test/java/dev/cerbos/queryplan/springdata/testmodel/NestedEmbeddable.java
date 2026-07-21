package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;

@Embeddable
public class NestedEmbeddable {

    @Column(name = "nested_a_bool")
    private Boolean aBool;

    @Column(name = "nested_a_string")
    private String aString;

    @Column(name = "nested_a_number")
    private Integer aNumber;

    @Column(name = "nested_optional_string")
    private String aOptionalString;

    @Embedded
    private NextLevelEmbeddable nextlevel;

    public NestedEmbeddable() {}

    public Boolean getaBool() { return aBool; }
    public void setaBool(Boolean aBool) { this.aBool = aBool; }
    public String getaString() { return aString; }
    public void setaString(String aString) { this.aString = aString; }
    public Integer getaNumber() { return aNumber; }
    public void setaNumber(Integer aNumber) { this.aNumber = aNumber; }
    public String getaOptionalString() { return aOptionalString; }
    public void setaOptionalString(String aOptionalString) { this.aOptionalString = aOptionalString; }
    public NextLevelEmbeddable getNextlevel() { return nextlevel; }
    public void setNextlevel(NextLevelEmbeddable nextlevel) { this.nextlevel = nextlevel; }
}

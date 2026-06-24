package dev.cerbos.queryplan.springdata.testmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
public class NextLevelEmbeddable {

    @Column(name = "next_a_bool")
    private Boolean aBool;

    @Column(name = "next_a_string")
    private String aString;

    public NextLevelEmbeddable() {}

    public Boolean getaBool() { return aBool; }
    public void setaBool(Boolean aBool) { this.aBool = aBool; }
    public String getaString() { return aString; }
    public void setaString(String aString) { this.aString = aString; }
}

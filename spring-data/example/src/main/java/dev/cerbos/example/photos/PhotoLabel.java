package dev.cerbos.example.photos;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "photo_labels")
public class PhotoLabel {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "label_name", nullable = false)
    private String labelName;

    @Column(name = "confidence", nullable = false)
    private double confidence;

    @Column(name = "reviewed", nullable = false)
    private boolean reviewed;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "photo_id", nullable = false)
    private Photo photo;

    protected PhotoLabel() {}

    PhotoLabel(Photo photo, String labelName, double confidence, boolean reviewed) {
        this.photo = photo;
        this.labelName = labelName;
        this.confidence = confidence;
        this.reviewed = reviewed;
    }

    public String getLabelName() { return labelName; }
    public double getConfidence() { return confidence; }
    public boolean isReviewed() { return reviewed; }
}

/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.example.photos;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
public class PhotoDetails {

    @Column(name = "pixel_width", nullable = false)
    private int pixelWidth;

    @Column(name = "pixel_height", nullable = false)
    private int pixelHeight;

    protected PhotoDetails() {}

    public PhotoDetails(int pixelWidth, int pixelHeight) {
        this.pixelWidth = pixelWidth;
        this.pixelHeight = pixelHeight;
    }

    public int getPixelWidth() { return pixelWidth; }
    public int getPixelHeight() { return pixelHeight; }
}

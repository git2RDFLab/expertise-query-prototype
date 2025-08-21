package de.leipzig.htwk.gitrdf.expertise.model.Requests;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;

@Data
public class FindMetricExamplesRequest {
    
    @NotNull(message = "Example count is required")
    @Positive(message = "Example count must be positive")
    private Integer exampleCount;
    
    @NotBlank(message = "Version is required")
    private String version;
    
    @NotBlank(message = "URI is required")
    private String uri;
    
    @NotBlank(message = "Metric ID is required")
    private String metricId;
}
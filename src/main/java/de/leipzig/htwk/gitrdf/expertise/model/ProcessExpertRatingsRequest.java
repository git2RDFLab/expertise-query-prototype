package de.leipzig.htwk.gitrdf.expertise.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessExpertRatingsRequest {
    
    @NotBlank(message = "URI is required")
    private String uri;
    
    @NotBlank(message = "Version is required")
    private String version;
    
    @NotNull(message = "Similar entity count is required")
    @Positive(message = "Similar entity count must be positive")
    private Integer exampleCount;
}
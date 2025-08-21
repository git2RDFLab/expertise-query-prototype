package de.leipzig.htwk.gitrdf.expertise.model.Requests;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FetchExamplesByUriRequest {
    
    @NotBlank(message = "URI is required")
    private String uri;
    
    @NotBlank(message = "Version is required")
    private String version;
    
    @NotNull(message = "Example count is required")
    @Positive(message = "Example count must be positive")
    private Integer exampleCount;
}
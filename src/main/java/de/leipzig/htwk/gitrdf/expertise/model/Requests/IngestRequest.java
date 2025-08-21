package de.leipzig.htwk.gitrdf.expertise.model.Requests;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IngestRequest {
    
    @NotNull(message = "storeToDatabase flag is required")
    private Boolean storeToDatabase;
    
    @NotNull(message = "saveToFile flag is required")
    private Boolean saveToFile;
}
package de.leipzig.htwk.gitrdf.expertise.model.Expert;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExpertSheet {
    private String expertName;
    private String targetRepository;
    private Integer targetId;
    private String metricName; // Sheet name (for backward compatibility)
    private String metricId;   // Actual metric ID from Excel
    private String metricLabel; // Metric label from Excel
    private String description;
    private List<ExpertRating> ratings;
    private Integer ratingsCount;
}
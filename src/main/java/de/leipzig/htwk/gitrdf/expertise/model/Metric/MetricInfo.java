package de.leipzig.htwk.gitrdf.expertise.model.Metric;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricInfo {
    private String metricId;
    private String metricLabel;
}
package de.leipzig.htwk.gitrdf.expertise.model.metrics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricWordCountStats {
    private String metricName;
    private int maxWordCount;
    private int minWordCount;
    private double avgWordCount;
    private int totalEntries;
    private List<Integer> wordCounts;
    private double standardDeviation;
}
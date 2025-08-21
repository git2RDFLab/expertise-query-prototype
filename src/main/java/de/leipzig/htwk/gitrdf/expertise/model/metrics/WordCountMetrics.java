package de.leipzig.htwk.gitrdf.expertise.model.metrics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WordCountMetrics {
    private String sessionId;
    private Integer targetId;
    private int maxWordCount;
    private int minWordCount;
    private double avgWordCount;
    private int totalEntries;
    private Map<String, AuthorWordCountStats> authorStats;
    private Map<String, MetricWordCountStats> metricStats;
}
package de.leipzig.htwk.gitrdf.expertise.service.metrics;

import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertFile;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertSheet;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertRating;
import de.leipzig.htwk.gitrdf.expertise.model.metrics.WordCountMetrics;
import de.leipzig.htwk.gitrdf.expertise.model.metrics.AuthorWordCountStats;
import de.leipzig.htwk.gitrdf.expertise.model.metrics.MetricWordCountStats;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class WordCountMetricsService {

    private final Map<String, Map<Integer, List<WordCountEntry>>> sessionCache = new HashMap<>();

    /**
     * Collects word count metrics during Excel processing
     */
    public void collectWordCountMetrics(ExpertFile expertFile, String sessionId) {
        if (expertFile == null || expertFile.getSheets() == null || expertFile.getSheets().isEmpty()) {
            log.warn("Skipping word count metrics collection - no expert sheets found");
            return;
        }

        Integer targetId = expertFile.getTargetId();
        if (targetId == null) {
            log.warn("Skipping word count metrics collection - no target ID found");
            return;
        }

        String authorName = expertFile.getExpertName();
        if (authorName == null || authorName.trim().isEmpty()) {
            log.warn("Skipping word count metrics collection - no author name found");
            return;
        }

        // Ensure session cache exists
        sessionCache.computeIfAbsent(sessionId, k -> new HashMap<>());
        sessionCache.get(sessionId).computeIfAbsent(targetId, k -> new ArrayList<>());

        List<WordCountEntry> entries = sessionCache.get(sessionId).get(targetId);

        // Process each sheet and collect word counts
        for (ExpertSheet sheet : expertFile.getSheets()) {
            if (sheet.getRatings() == null || sheet.getRatings().isEmpty()) {
                continue;
            }

            String metricName = sheet.getMetricName();
            if (metricName == null) {
                metricName = sheet.getMetricId();
            }

            for (ExpertRating rating : sheet.getRatings()) {
                if (rating.getEntity() != null && !rating.getEntity().trim().isEmpty()) {
                    int wordCount = countWords(rating.getEntity());
                    
                    WordCountEntry entry = WordCountEntry.builder()
                            .authorName(authorName)
                            .metricName(metricName)
                            .entityText(rating.getEntity())
                            .wordCount(wordCount)
                            .build();
                    
                    entries.add(entry);
                    
                    log.debug("Collected word count entry: author={}, metric={}, words={}", 
                            authorName, metricName, wordCount);
                }
            }
        }

        log.info("Collected {} word count entries for session {} and target {}", 
                entries.size(), sessionId, targetId);
    }

    /**
     * Calculates final metrics and saves to file after all Excel processing is complete
     */
    public WordCountMetrics calculateAndSaveMetrics(String sessionId, Integer targetId, Path rdfOutputDirectory) {
        if (!sessionCache.containsKey(sessionId) || !sessionCache.get(sessionId).containsKey(targetId)) {
            log.warn("No word count data found for session {} and target {}", sessionId, targetId);
            return null;
        }

        List<WordCountEntry> entries = sessionCache.get(sessionId).get(targetId);
        if (entries.isEmpty()) {
            log.warn("No word count entries found for session {} and target {}", sessionId, targetId);
            return null;
        }

        log.info("Calculating word count metrics for {} entries", entries.size());

        // Calculate overall metrics
        List<Integer> allWordCounts = entries.stream()
                .map(WordCountEntry::getWordCount)
                .collect(Collectors.toList());

        int maxWordCount = allWordCounts.stream().mapToInt(Integer::intValue).max().orElse(0);
        int minWordCount = allWordCounts.stream().mapToInt(Integer::intValue).min().orElse(0);
        double avgWordCount = allWordCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);

        // Calculate per-author metrics
        Map<String, AuthorWordCountStats> authorStats = calculateAuthorStats(entries);

        // Calculate per-metric metrics
        Map<String, MetricWordCountStats> metricStats = calculateMetricStats(entries);

        WordCountMetrics metrics = WordCountMetrics.builder()
                .sessionId(sessionId)
                .targetId(targetId)
                .maxWordCount(maxWordCount)
                .minWordCount(minWordCount)
                .avgWordCount(avgWordCount)
                .totalEntries(entries.size())
                .authorStats(authorStats)
                .metricStats(metricStats)
                .build();

        // Save metrics to file
        try {
            saveMetricsToFile(metrics, rdfOutputDirectory);
            log.info("Successfully saved word count metrics to file for session {} and target {}", 
                    sessionId, targetId);
        } catch (IOException e) {
            log.error("Failed to save word count metrics to file: {}", e.getMessage(), e);
        }

        // Clean up cache for this session/target
        sessionCache.get(sessionId).remove(targetId);
        if (sessionCache.get(sessionId).isEmpty()) {
            sessionCache.remove(sessionId);
        }

        return metrics;
    }

    /**
     * Calculates metrics for all target IDs in a session
     */
    public List<WordCountMetrics> calculateAndSaveAllMetrics(String sessionId, Path rdfOutputDirectory) {
        if (!sessionCache.containsKey(sessionId)) {
            log.warn("No word count data found for session {}", sessionId);
            return Collections.emptyList();
        }

        List<WordCountMetrics> allMetrics = new ArrayList<>();
        Set<Integer> targetIds = new HashSet<>(sessionCache.get(sessionId).keySet());

        for (Integer targetId : targetIds) {
            WordCountMetrics metrics = calculateAndSaveMetrics(sessionId, targetId, rdfOutputDirectory);
            if (metrics != null) {
                allMetrics.add(metrics);
            }
        }

        log.info("Calculated and saved word count metrics for {} targets in session {}", 
                allMetrics.size(), sessionId);

        // Create and save aggregated summary file across all orderIds
        if (!allMetrics.isEmpty()) {
            try {
                saveAggregatedSummaryFile(allMetrics, sessionId, rdfOutputDirectory);
            } catch (IOException e) {
                log.error("Failed to save aggregated summary file for session {}: {}", sessionId, e.getMessage(), e);
            }
        }

        return allMetrics;
    }

    private Map<String, AuthorWordCountStats> calculateAuthorStats(List<WordCountEntry> entries) {
        Map<String, List<WordCountEntry>> entriesByAuthor = entries.stream()
                .collect(Collectors.groupingBy(WordCountEntry::getAuthorName));

        Map<String, AuthorWordCountStats> authorStats = new HashMap<>();

        for (Map.Entry<String, List<WordCountEntry>> entry : entriesByAuthor.entrySet()) {
            String authorName = entry.getKey();
            List<Integer> wordCounts = entry.getValue().stream()
                    .map(WordCountEntry::getWordCount)
                    .collect(Collectors.toList());

            int max = wordCounts.stream().mapToInt(Integer::intValue).max().orElse(0);
            int min = wordCounts.stream().mapToInt(Integer::intValue).min().orElse(0);
            double avg = wordCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
            double stdDev = calculateStandardDeviation(wordCounts, avg);

            AuthorWordCountStats stats = AuthorWordCountStats.builder()
                    .authorName(authorName)
                    .maxWordCount(max)
                    .minWordCount(min)
                    .avgWordCount(avg)
                    .totalEntries(wordCounts.size())
                    .wordCounts(wordCounts)
                    .standardDeviation(stdDev)
                    .build();

            authorStats.put(authorName, stats);
        }

        return authorStats;
    }

    private Map<String, MetricWordCountStats> calculateMetricStats(List<WordCountEntry> entries) {
        Map<String, List<WordCountEntry>> entriesByMetric = entries.stream()
                .collect(Collectors.groupingBy(WordCountEntry::getMetricName));

        Map<String, MetricWordCountStats> metricStats = new HashMap<>();

        for (Map.Entry<String, List<WordCountEntry>> entry : entriesByMetric.entrySet()) {
            String metricName = entry.getKey();
            List<Integer> wordCounts = entry.getValue().stream()
                    .map(WordCountEntry::getWordCount)
                    .collect(Collectors.toList());

            int max = wordCounts.stream().mapToInt(Integer::intValue).max().orElse(0);
            int min = wordCounts.stream().mapToInt(Integer::intValue).min().orElse(0);
            double avg = wordCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
            double stdDev = calculateStandardDeviation(wordCounts, avg);

            MetricWordCountStats stats = MetricWordCountStats.builder()
                    .metricName(metricName)
                    .maxWordCount(max)
                    .minWordCount(min)
                    .avgWordCount(avg)
                    .totalEntries(wordCounts.size())
                    .wordCounts(wordCounts)
                    .standardDeviation(stdDev)
                    .build();

            metricStats.put(metricName, stats);
        }

        return metricStats;
    }

    private double calculateStandardDeviation(List<Integer> values, double mean) {
        if (values.size() <= 1) return 0.0;

        double sumSquaredDifferences = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .sum();

        return Math.sqrt(sumSquaredDifferences / (values.size() - 1));
    }

    public int countWords(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0;
        }

        // Simple word counting - split by whitespace and filter out empty strings
        String[] words = text.trim().split("\\s+");
        return (int) Arrays.stream(words)
                .filter(word -> !word.trim().isEmpty())
                .count();
    }

    private void saveMetricsToFile(WordCountMetrics metrics, Path rdfOutputDirectory) throws IOException {
        // Ensure output directory exists
        Files.createDirectories(rdfOutputDirectory);

        // Create filename with timestamp
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String filename = String.format("word_count_metrics_target_%d_session_%s_%s.json", 
                metrics.getTargetId(), 
                metrics.getSessionId().substring(0, Math.min(8, metrics.getSessionId().length())),
                timestamp);

        Path outputFile = rdfOutputDirectory.resolve(filename);

        // Create JSON content manually (simple approach)
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"sessionId\": \"").append(metrics.getSessionId()).append("\",\n");
        json.append("  \"targetId\": ").append(metrics.getTargetId()).append(",\n");
        json.append("  \"overallMetrics\": {\n");
        json.append("    \"maxWordCount\": ").append(metrics.getMaxWordCount()).append(",\n");
        json.append("    \"minWordCount\": ").append(metrics.getMinWordCount()).append(",\n");
        json.append("    \"avgWordCount\": ").append(String.format("%.2f", metrics.getAvgWordCount())).append(",\n");
        json.append("    \"totalEntries\": ").append(metrics.getTotalEntries()).append("\n");
        json.append("  },\n");

        // Author stats
        json.append("  \"authorStats\": {\n");
        boolean firstAuthor = true;
        for (Map.Entry<String, AuthorWordCountStats> entry : metrics.getAuthorStats().entrySet()) {
            if (!firstAuthor) json.append(",\n");
            AuthorWordCountStats stats = entry.getValue();
            json.append("    \"").append(entry.getKey()).append("\": {\n");
            json.append("      \"maxWordCount\": ").append(stats.getMaxWordCount()).append(",\n");
            json.append("      \"minWordCount\": ").append(stats.getMinWordCount()).append(",\n");
            json.append("      \"avgWordCount\": ").append(String.format("%.2f", stats.getAvgWordCount())).append(",\n");
            json.append("      \"totalEntries\": ").append(stats.getTotalEntries()).append(",\n");
            json.append("      \"standardDeviation\": ").append(String.format("%.2f", stats.getStandardDeviation())).append("\n");
            json.append("    }");
            firstAuthor = false;
        }
        json.append("\n  },\n");

        // Metric stats
        json.append("  \"metricStats\": {\n");
        boolean firstMetric = true;
        for (Map.Entry<String, MetricWordCountStats> entry : metrics.getMetricStats().entrySet()) {
            if (!firstMetric) json.append(",\n");
            MetricWordCountStats stats = entry.getValue();
            json.append("    \"").append(entry.getKey()).append("\": {\n");
            json.append("      \"maxWordCount\": ").append(stats.getMaxWordCount()).append(",\n");
            json.append("      \"minWordCount\": ").append(stats.getMinWordCount()).append(",\n");
            json.append("      \"avgWordCount\": ").append(String.format("%.2f", stats.getAvgWordCount())).append(",\n");
            json.append("      \"totalEntries\": ").append(stats.getTotalEntries()).append(",\n");
            json.append("      \"standardDeviation\": ").append(String.format("%.2f", stats.getStandardDeviation())).append("\n");
            json.append("    }");
            firstMetric = false;
        }
        json.append("\n  },\n");
        
        json.append("  \"generatedAt\": \"").append(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date())).append("\"\n");
        json.append("}\n");

        Files.write(outputFile, json.toString().getBytes());
        log.info("Word count metrics saved to: {}", outputFile.toAbsolutePath());
    }

    /**
     * Creates and saves an aggregated summary file combining metrics from all orderIds
     */
    private void saveAggregatedSummaryFile(List<WordCountMetrics> allMetrics, String sessionId, Path rdfOutputDirectory) throws IOException {
        // Ensure output directory exists
        Files.createDirectories(rdfOutputDirectory);

        // Create filename for aggregated summary
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String filename = String.format("aggregated_word_count_summary_session_%s_%s.json", 
                sessionId.substring(0, Math.min(8, sessionId.length())), timestamp);

        Path outputFile = rdfOutputDirectory.resolve(filename);

        // Calculate aggregated statistics
        int totalTargets = allMetrics.size();
        int totalEntries = allMetrics.stream().mapToInt(WordCountMetrics::getTotalEntries).sum();
        
        // Overall aggregated stats
        List<Integer> allWordCounts = new ArrayList<>();
        Map<String, List<Integer>> allAuthorWordCounts = new HashMap<>();
        Map<String, List<Integer>> allMetricWordCounts = new HashMap<>();
        
        for (WordCountMetrics metrics : allMetrics) {
            // Collect all word counts for overall stats
            allWordCounts.addAll(getAllWordCountsFromMetrics(metrics));
            
            // Collect author stats across all targets
            for (AuthorWordCountStats authorStats : metrics.getAuthorStats().values()) {
                allAuthorWordCounts.computeIfAbsent(authorStats.getAuthorName(), k -> new ArrayList<>())
                    .addAll(authorStats.getWordCounts());
            }
            
            // Collect metric stats across all targets
            for (MetricWordCountStats metricStats : metrics.getMetricStats().values()) {
                allMetricWordCounts.computeIfAbsent(metricStats.getMetricName(), k -> new ArrayList<>())
                    .addAll(metricStats.getWordCounts());
            }
        }
        
        // Calculate aggregated overall stats
        int aggregatedMax = allWordCounts.stream().mapToInt(Integer::intValue).max().orElse(0);
        int aggregatedMin = allWordCounts.stream().mapToInt(Integer::intValue).min().orElse(0);
        double aggregatedAvg = allWordCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
        double aggregatedStdDev = calculateStandardDeviation(allWordCounts, aggregatedAvg);

        // Build JSON content
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"sessionId\": \"").append(sessionId).append("\",\n");
        json.append("  \"aggregationType\": \"CROSS_ORDER_SUMMARY\",\n");
        json.append("  \"totalTargets\": ").append(totalTargets).append(",\n");
        json.append("  \"targetIds\": [").append(
            allMetrics.stream()
                .map(m -> String.valueOf(m.getTargetId()))
                .collect(Collectors.joining(", "))
        ).append("],\n");
        
        // Aggregated overall metrics
        json.append("  \"aggregatedOverallMetrics\": {\n");
        json.append("    \"maxWordCount\": ").append(aggregatedMax).append(",\n");
        json.append("    \"minWordCount\": ").append(aggregatedMin).append(",\n");
        json.append("    \"avgWordCount\": ").append(String.format("%.2f", aggregatedAvg)).append(",\n");
        json.append("    \"totalEntries\": ").append(totalEntries).append(",\n");
        json.append("    \"standardDeviation\": ").append(String.format("%.2f", aggregatedStdDev)).append("\n");
        json.append("  },\n");

        // Aggregated author stats across all targets
        json.append("  \"aggregatedAuthorStats\": {\n");
        boolean firstAuthor = true;
        for (Map.Entry<String, List<Integer>> entry : allAuthorWordCounts.entrySet()) {
            if (!firstAuthor) json.append(",\n");
            String authorName = entry.getKey();
            List<Integer> wordCounts = entry.getValue();
            
            int max = wordCounts.stream().mapToInt(Integer::intValue).max().orElse(0);
            int min = wordCounts.stream().mapToInt(Integer::intValue).min().orElse(0);
            double avg = wordCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
            double stdDev = calculateStandardDeviation(wordCounts, avg);
            
            json.append("    \"").append(authorName).append("\": {\n");
            json.append("      \"maxWordCount\": ").append(max).append(",\n");
            json.append("      \"minWordCount\": ").append(min).append(",\n");
            json.append("      \"avgWordCount\": ").append(String.format("%.2f", avg)).append(",\n");
            json.append("      \"totalEntries\": ").append(wordCounts.size()).append(",\n");
            json.append("      \"standardDeviation\": ").append(String.format("%.2f", stdDev)).append(",\n");
            json.append("      \"targetsAppearingIn\": ").append(countTargetsForAuthor(authorName, allMetrics)).append("\n");
            json.append("    }");
            firstAuthor = false;
        }
        json.append("\n  },\n");

        // Aggregated metric stats across all targets
        json.append("  \"aggregatedMetricStats\": {\n");
        boolean firstMetric = true;
        for (Map.Entry<String, List<Integer>> entry : allMetricWordCounts.entrySet()) {
            if (!firstMetric) json.append(",\n");
            String metricName = entry.getKey();
            List<Integer> wordCounts = entry.getValue();
            
            int max = wordCounts.stream().mapToInt(Integer::intValue).max().orElse(0);
            int min = wordCounts.stream().mapToInt(Integer::intValue).min().orElse(0);
            double avg = wordCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
            double stdDev = calculateStandardDeviation(wordCounts, avg);
            
            json.append("    \"").append(metricName).append("\": {\n");
            json.append("      \"maxWordCount\": ").append(max).append(",\n");
            json.append("      \"minWordCount\": ").append(min).append(",\n");
            json.append("      \"avgWordCount\": ").append(String.format("%.2f", avg)).append(",\n");
            json.append("      \"totalEntries\": ").append(wordCounts.size()).append(",\n");
            json.append("      \"standardDeviation\": ").append(String.format("%.2f", stdDev)).append(",\n");
            json.append("      \"targetsAppearingIn\": ").append(countTargetsForMetric(metricName, allMetrics)).append("\n");
            json.append("    }");
            firstMetric = false;
        }
        json.append("\n  },\n");
        
        // Individual target summaries
        json.append("  \"individualTargetSummaries\": {\n");
        boolean firstTarget = true;
        for (WordCountMetrics metrics : allMetrics) {
            if (!firstTarget) json.append(",\n");
            json.append("    \"").append(metrics.getTargetId()).append("\": {\n");
            json.append("      \"maxWordCount\": ").append(metrics.getMaxWordCount()).append(",\n");
            json.append("      \"minWordCount\": ").append(metrics.getMinWordCount()).append(",\n");
            json.append("      \"avgWordCount\": ").append(String.format("%.2f", metrics.getAvgWordCount())).append(",\n");
            json.append("      \"totalEntries\": ").append(metrics.getTotalEntries()).append(",\n");
            json.append("      \"authorCount\": ").append(metrics.getAuthorStats().size()).append(",\n");
            json.append("      \"metricCount\": ").append(metrics.getMetricStats().size()).append("\n");
            json.append("    }");
            firstTarget = false;
        }
        json.append("\n  },\n");
        
        json.append("  \"generatedAt\": \"").append(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date())).append("\"\n");
        json.append("}\n");

        Files.write(outputFile, json.toString().getBytes());
        log.info("Aggregated word count summary saved to: {} (covering {} targets)", 
                outputFile.toAbsolutePath(), totalTargets);
    }

    /**
     * Extract all word counts from a WordCountMetrics object
     */
    private List<Integer> getAllWordCountsFromMetrics(WordCountMetrics metrics) {
        List<Integer> allCounts = new ArrayList<>();
        
        // Add from author stats
        for (AuthorWordCountStats authorStats : metrics.getAuthorStats().values()) {
            allCounts.addAll(authorStats.getWordCounts());
        }
        
        return allCounts;
    }

    /**
     * Count how many targets an author appears in
     */
    private int countTargetsForAuthor(String authorName, List<WordCountMetrics> allMetrics) {
        return (int) allMetrics.stream()
            .filter(metrics -> metrics.getAuthorStats().containsKey(authorName))
            .count();
    }

    /**
     * Count how many targets a metric appears in
     */
    private int countTargetsForMetric(String metricName, List<WordCountMetrics> allMetrics) {
        return (int) allMetrics.stream()
            .filter(metrics -> metrics.getMetricStats().containsKey(metricName))
            .count();
    }

    /**
     * Internal class for caching word count entries
     */
    @lombok.Data
    @lombok.Builder
    @lombok.AllArgsConstructor
    @lombok.NoArgsConstructor
    private static class WordCountEntry {
        private String authorName;
        private String metricName;
        private String entityText;
        private int wordCount;
    }
}
package de.leipzig.htwk.gitrdf.expertise.service.metrics;

import de.leipzig.htwk.gitrdf.expertise.model.EntityType;
import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for analyzing word count and statistical metrics across all orders
 * Provides both individual order analysis and comprehensive aggregated summaries
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsStatisticsService {
    
    private final SparqlQueryService sparqlQueryService;
    private final WordCountMetricsService wordCountService;
    
    /**
     * Generate comprehensive metrics statistics for all orders
     * 
     * @param orderIds List of order IDs to analyze (if empty, analyzes all available orders)
     * @param entityTypes List of entity types to include (if empty, includes all types)
     * @return Comprehensive statistics report
     */
    public MetricsStatisticsReport generateComprehensiveReport(
            List<Integer> orderIds, 
            List<EntityType> entityTypes) {
        
        log.info("Generating comprehensive metrics statistics for {} orders, {} entity types", 
                orderIds.size(), entityTypes.size());
        
        long startTime = System.currentTimeMillis();
        
        // If no entity types specified, include all
        if (entityTypes.isEmpty()) {
            entityTypes = Arrays.asList(EntityType.values());
        }
        
        MetricsStatisticsReport report = new MetricsStatisticsReport();
        report.setGeneratedAt(LocalDateTime.now());
        report.setOrderIds(new ArrayList<>(orderIds));
        report.setEntityTypes(entityTypes.stream().map(EntityType::name).collect(Collectors.toList()));
        
        // Analyze each order individually
        Map<Integer, OrderStatistics> orderStats = new HashMap<>();
        Map<String, MetricAggregateStats> aggregateStats = new HashMap<>();
        
        for (Integer orderId : orderIds) {
            try {
                log.info("Analyzing order {}", orderId);
                OrderStatistics orderStat = analyzeOrder(orderId, entityTypes);
                orderStats.put(orderId, orderStat);
                
                // Accumulate for aggregate statistics
                accumulateAggregateStats(orderStat, aggregateStats);
                
            } catch (Exception e) {
                log.error("Failed to analyze order {}: {}", orderId, e.getMessage());
            }
        }
        
        report.setOrderStatistics(orderStats);
        report.setAggregateStatistics(finalizeAggregateStats(aggregateStats, orderIds.size()));
        
        // Generate summary insights
        report.setSummaryInsights(generateSummaryInsights(report));
        
        long executionTime = System.currentTimeMillis() - startTime;
        report.setExecutionTimeMs(executionTime);
        
        log.info("Comprehensive metrics report generated in {}ms for {} orders", 
                executionTime, orderIds.size());
        
        return report;
    }
    
    /**
     * Analyze metrics statistics for a single order
     */
    private OrderStatistics analyzeOrder(Integer orderId, List<EntityType> entityTypes) {
        OrderStatistics stats = new OrderStatistics();
        stats.setOrderId(orderId);
        stats.setEntityTypeStats(new HashMap<>());
        
        int totalEntities = 0;
        Map<String, List<Double>> allMetricValues = new HashMap<>();
        
        for (EntityType entityType : entityTypes) {
            try {
                // Load entities with metrics for this order and entity type
                List<SparqlQueryService.EntityData> entities = 
                    sparqlQueryService.loadEntitiesWithMetrics(orderId, entityType);
                
                if (entities.isEmpty()) {
                    log.debug("No {} entities found for order {}", entityType, orderId);
                    continue;
                }
                
                EntityTypeStatistics entityStats = analyzeEntityTypeMetrics(entities, entityType);
                stats.getEntityTypeStats().put(entityType.name(), entityStats);
                
                totalEntities += entities.size();
                
                // Accumulate metric values across entity types
                accumulateMetricValues(entities, allMetricValues);
                
            } catch (Exception e) {
                log.warn("Failed to analyze {} entities for order {}: {}", entityType, orderId, e.getMessage());
            }
        }
        
        stats.setTotalEntities(totalEntities);
        stats.setOrderMetricStats(calculateOrderMetricStats(allMetricValues));
        
        return stats;
    }
    
    /**
     * Analyze metrics for a specific entity type
     */
    private EntityTypeStatistics analyzeEntityTypeMetrics(
            List<SparqlQueryService.EntityData> entities, EntityType entityType) {
        
        EntityTypeStatistics stats = new EntityTypeStatistics();
        stats.setEntityType(entityType.name());
        stats.setEntityCount(entities.size());
        
        // Text statistics
        TextStatistics textStats = calculateTextStatistics(entities, entityType);
        stats.setTextStatistics(textStats);
        
        // Metric value statistics
        Map<String, MetricStatistics> metricStats = calculateMetricStatistics(entities);
        stats.setMetricStatistics(metricStats);
        
        return stats;
    }
    
    /**
     * Calculate text-based statistics (word counts, lengths, etc.)
     */
    private TextStatistics calculateTextStatistics(
            List<SparqlQueryService.EntityData> entities, EntityType entityType) {
        
        List<Integer> wordCounts = new ArrayList<>();
        List<Integer> charCounts = new ArrayList<>();
        int emptyTexts = 0;
        
        for (SparqlQueryService.EntityData entity : entities) {
            String text = extractTextContent(entity, entityType);
            
            if (text == null || text.trim().isEmpty()) {
                emptyTexts++;
                continue;
            }
            
            // Calculate word and character counts
            int wordCount = wordCountService.countWords(text);
            int charCount = text.length();
            
            wordCounts.add(wordCount);
            charCounts.add(charCount);
        }
        
        TextStatistics textStats = new TextStatistics();
        textStats.setTotalEntities(entities.size());
        textStats.setEntitiesWithText(entities.size() - emptyTexts);
        textStats.setEmptyTextEntities(emptyTexts);
        
        if (!wordCounts.isEmpty()) {
            textStats.setWordCountStats(calculateNumericStats(wordCounts));
            textStats.setCharCountStats(calculateNumericStats(charCounts));
        }
        
        return textStats;
    }
    
    /**
     * Extract relevant text content based on entity type
     */
    private String extractTextContent(SparqlQueryService.EntityData entity, EntityType entityType) {
        StringBuilder text = new StringBuilder();
        
        switch (entityType) {
            case COMMIT:
                String message = (String) entity.properties().get("message");
                if (message != null) text.append(message);
                break;
            case ISSUE:
                String title = (String) entity.properties().get("title");
                String body = (String) entity.properties().get("body");
                if (title != null) text.append(title).append(" ");
                if (body != null) text.append(body);
                break;
            case PULL_REQUEST:
                String prTitle = (String) entity.properties().get("title");
                String prBody = (String) entity.properties().get("body");
                if (prTitle != null) text.append(prTitle).append(" ");
                if (prBody != null) text.append(prBody);
                break;
            case COMMENT:
                String commentBody = (String) entity.properties().get("commentBody");
                if (commentBody != null) text.append(commentBody);
                break;
        }
        
        return text.toString().trim();
    }
    
    /**
     * Calculate statistics for metric values
     */
    private Map<String, MetricStatistics> calculateMetricStatistics(
            List<SparqlQueryService.EntityData> entities) {
        
        Map<String, List<Double>> metricValues = new HashMap<>();
        
        // Collect all metric values
        for (SparqlQueryService.EntityData entity : entities) {
            for (Map.Entry<String, Object> entry : entity.properties().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                
                // Skip non-metric properties
                if (!isMetricProperty(key)) continue;
                
                if (value instanceof Number) {
                    double numValue = ((Number) value).doubleValue();
                    metricValues.computeIfAbsent(key, k -> new ArrayList<>()).add(numValue);
                }
            }
        }
        
        // Calculate statistics for each metric
        Map<String, MetricStatistics> metricStats = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : metricValues.entrySet()) {
            String metricName = entry.getKey();
            List<Double> values = entry.getValue();
            
            MetricStatistics stats = new MetricStatistics();
            stats.setMetricName(metricName);
            stats.setEntityCount(values.size());
            stats.setNumericStats(calculateNumericStats(values.stream().mapToInt(Double::intValue).boxed().collect(Collectors.toList())));
            stats.setValueDistribution(calculateValueDistribution(values));
            
            metricStats.put(metricName, stats);
        }
        
        return metricStats;
    }
    
    /**
     * Check if a property name represents a metric
     */
    private boolean isMetricProperty(String propertyName) {
        return propertyName.toLowerCase().contains("score") || 
               propertyName.toLowerCase().contains("rating") ||
               propertyName.toLowerCase().contains("metric");
    }
    
    /**
     * Calculate numeric statistics (mean, median, std dev, etc.)
     */
    private NumericStatistics calculateNumericStats(List<Integer> values) {
        if (values.isEmpty()) {
            return new NumericStatistics();
        }
        
        Collections.sort(values);
        
        NumericStatistics stats = new NumericStatistics();
        stats.setCount(values.size());
        stats.setMin(values.get(0));
        stats.setMax(values.get(values.size() - 1));
        
        // Mean
        double mean = values.stream().mapToDouble(Integer::doubleValue).average().orElse(0.0);
        stats.setMean(mean);
        
        // Median
        double median;
        int size = values.size();
        if (size % 2 == 0) {
            median = (values.get(size/2 - 1) + values.get(size/2)) / 2.0;
        } else {
            median = values.get(size/2);
        }
        stats.setMedian(median);
        
        // Standard deviation
        double variance = values.stream()
            .mapToDouble(v -> Math.pow(v - mean, 2))
            .average().orElse(0.0);
        stats.setStandardDeviation(Math.sqrt(variance));
        
        // Percentiles
        stats.setP25(values.get((int) (size * 0.25)));
        stats.setP75(values.get((int) (size * 0.75)));
        
        return stats;
    }
    
    /**
     * Calculate value distribution (histogram)
     */
    private Map<String, Integer> calculateValueDistribution(List<Double> values) {
        Map<String, Integer> distribution = new HashMap<>();
        
        if (values.isEmpty()) return distribution;
        
        // Create 10 bins for distribution
        double min = values.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
        double max = values.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
        double binSize = (max - min) / 10.0;
        
        if (binSize == 0) {
            distribution.put(String.format("%.1f", min), values.size());
            return distribution;
        }
        
        for (double value : values) {
            int binIndex = Math.min(9, (int) ((value - min) / binSize));
            double binStart = min + binIndex * binSize;
            double binEnd = binStart + binSize;
            String binLabel = String.format("%.1f-%.1f", binStart, binEnd);
            distribution.merge(binLabel, 1, Integer::sum);
        }
        
        return distribution;
    }
    
    /**
     * Accumulate metric values for aggregate statistics
     */
    private void accumulateMetricValues(
            List<SparqlQueryService.EntityData> entities, 
            Map<String, List<Double>> allMetricValues) {
        
        for (SparqlQueryService.EntityData entity : entities) {
            for (Map.Entry<String, Object> entry : entity.properties().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                
                if (!isMetricProperty(key)) continue;
                
                if (value instanceof Number) {
                    double numValue = ((Number) value).doubleValue();
                    allMetricValues.computeIfAbsent(key, k -> new ArrayList<>()).add(numValue);
                }
            }
        }
    }
    
    /**
     * Calculate order-level metric statistics
     */
    private Map<String, MetricStatistics> calculateOrderMetricStats(Map<String, List<Double>> metricValues) {
        Map<String, MetricStatistics> orderStats = new HashMap<>();
        
        for (Map.Entry<String, List<Double>> entry : metricValues.entrySet()) {
            String metricName = entry.getKey();
            List<Double> values = entry.getValue();
            
            MetricStatistics stats = new MetricStatistics();
            stats.setMetricName(metricName);
            stats.setEntityCount(values.size());
            stats.setNumericStats(calculateNumericStats(values.stream().mapToInt(Double::intValue).boxed().collect(Collectors.toList())));
            stats.setValueDistribution(calculateValueDistribution(values));
            
            orderStats.put(metricName, stats);
        }
        
        return orderStats;
    }
    
    /**
     * Accumulate statistics across orders for aggregate analysis
     */
    private void accumulateAggregateStats(
            OrderStatistics orderStats, 
            Map<String, MetricAggregateStats> aggregateStats) {
        
        // Accumulate from order-level metric stats
        for (Map.Entry<String, MetricStatistics> entry : orderStats.getOrderMetricStats().entrySet()) {
            String metricName = entry.getKey();
            MetricStatistics metricStats = entry.getValue();
            
            MetricAggregateStats aggregate = aggregateStats.computeIfAbsent(metricName, 
                k -> new MetricAggregateStats(metricName));
            
            aggregate.addOrderStats(orderStats.getOrderId(), metricStats);
        }
    }
    
    /**
     * Finalize aggregate statistics calculations
     */
    private Map<String, MetricAggregateStats> finalizeAggregateStats(
            Map<String, MetricAggregateStats> aggregateStats, int totalOrders) {
        
        for (MetricAggregateStats stats : aggregateStats.values()) {
            stats.finalizeCalculations(totalOrders);
        }
        
        return aggregateStats;
    }
    
    /**
     * Generate summary insights from the complete report
     */
    private SummaryInsights generateSummaryInsights(MetricsStatisticsReport report) {
        SummaryInsights insights = new SummaryInsights();
        
        // Overall statistics
        int totalEntities = report.getOrderStatistics().values().stream()
            .mapToInt(OrderStatistics::getTotalEntities).sum();
        insights.setTotalEntitiesAnalyzed(totalEntities);
        insights.setTotalOrdersAnalyzed(report.getOrderIds().size());
        insights.setEntityTypesAnalyzed(report.getEntityTypes());
        
        // Most common metrics
        Set<String> allMetrics = report.getAggregateStatistics().keySet();
        insights.setMetricsFound(new ArrayList<>(allMetrics));
        insights.setTotalMetricsFound(allMetrics.size());
        
        // Data quality insights
        insights.setDataQualityInsights(generateDataQualityInsights(report));
        
        // Metric distribution insights
        insights.setMetricDistributionInsights(generateMetricDistributionInsights(report));
        
        return insights;
    }
    
    private List<String> generateDataQualityInsights(MetricsStatisticsReport report) {
        List<String> insights = new ArrayList<>();
        
        // Analyze empty text rates
        for (OrderStatistics orderStats : report.getOrderStatistics().values()) {
            for (EntityTypeStatistics entityStats : orderStats.getEntityTypeStats().values()) {
                TextStatistics textStats = entityStats.getTextStatistics();
                if (textStats.getEmptyTextEntities() > 0) {
                    double emptyRate = (double) textStats.getEmptyTextEntities() / textStats.getTotalEntities() * 100;
                    insights.add(String.format("Order %d %s: %.1f%% entities have empty text content", 
                        orderStats.getOrderId(), entityStats.getEntityType(), emptyRate));
                }
            }
        }
        
        return insights;
    }
    
    private List<String> generateMetricDistributionInsights(MetricsStatisticsReport report) {
        List<String> insights = new ArrayList<>();
        
        for (MetricAggregateStats aggStats : report.getAggregateStatistics().values()) {
            insights.add(String.format("Metric '%s': Found in %d orders, total %d entities", 
                aggStats.getMetricName(), aggStats.getOrdersWithMetric(), aggStats.getTotalEntities()));
        }
        
        return insights;
    }
    
    // Data classes for the statistics report
    
    public static class MetricsStatisticsReport {
        private LocalDateTime generatedAt;
        private List<Integer> orderIds;
        private List<String> entityTypes;
        private Map<Integer, OrderStatistics> orderStatistics;
        private Map<String, MetricAggregateStats> aggregateStatistics;
        private SummaryInsights summaryInsights;
        private long executionTimeMs;
        
        // Getters and setters
        public LocalDateTime getGeneratedAt() { return generatedAt; }
        public void setGeneratedAt(LocalDateTime generatedAt) { this.generatedAt = generatedAt; }
        public List<Integer> getOrderIds() { return orderIds; }
        public void setOrderIds(List<Integer> orderIds) { this.orderIds = orderIds; }
        public List<String> getEntityTypes() { return entityTypes; }
        public void setEntityTypes(List<String> entityTypes) { this.entityTypes = entityTypes; }
        public Map<Integer, OrderStatistics> getOrderStatistics() { return orderStatistics; }
        public void setOrderStatistics(Map<Integer, OrderStatistics> orderStatistics) { this.orderStatistics = orderStatistics; }
        public Map<String, MetricAggregateStats> getAggregateStatistics() { return aggregateStatistics; }
        public void setAggregateStatistics(Map<String, MetricAggregateStats> aggregateStatistics) { this.aggregateStatistics = aggregateStatistics; }
        public SummaryInsights getSummaryInsights() { return summaryInsights; }
        public void setSummaryInsights(SummaryInsights summaryInsights) { this.summaryInsights = summaryInsights; }
        public long getExecutionTimeMs() { return executionTimeMs; }
        public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }
    }
    
    public static class OrderStatistics {
        private Integer orderId;
        private int totalEntities;
        private Map<String, EntityTypeStatistics> entityTypeStats;
        private Map<String, MetricStatistics> orderMetricStats;
        
        // Getters and setters
        public Integer getOrderId() { return orderId; }
        public void setOrderId(Integer orderId) { this.orderId = orderId; }
        public int getTotalEntities() { return totalEntities; }
        public void setTotalEntities(int totalEntities) { this.totalEntities = totalEntities; }
        public Map<String, EntityTypeStatistics> getEntityTypeStats() { return entityTypeStats; }
        public void setEntityTypeStats(Map<String, EntityTypeStatistics> entityTypeStats) { this.entityTypeStats = entityTypeStats; }
        public Map<String, MetricStatistics> getOrderMetricStats() { return orderMetricStats; }
        public void setOrderMetricStats(Map<String, MetricStatistics> orderMetricStats) { this.orderMetricStats = orderMetricStats; }
    }
    
    public static class EntityTypeStatistics {
        private String entityType;
        private int entityCount;
        private TextStatistics textStatistics;
        private Map<String, MetricStatistics> metricStatistics;
        
        // Getters and setters
        public String getEntityType() { return entityType; }
        public void setEntityType(String entityType) { this.entityType = entityType; }
        public int getEntityCount() { return entityCount; }
        public void setEntityCount(int entityCount) { this.entityCount = entityCount; }
        public TextStatistics getTextStatistics() { return textStatistics; }
        public void setTextStatistics(TextStatistics textStatistics) { this.textStatistics = textStatistics; }
        public Map<String, MetricStatistics> getMetricStatistics() { return metricStatistics; }
        public void setMetricStatistics(Map<String, MetricStatistics> metricStatistics) { this.metricStatistics = metricStatistics; }
    }
    
    public static class TextStatistics {
        private int totalEntities;
        private int entitiesWithText;
        private int emptyTextEntities;
        private NumericStatistics wordCountStats;
        private NumericStatistics charCountStats;
        
        // Getters and setters
        public int getTotalEntities() { return totalEntities; }
        public void setTotalEntities(int totalEntities) { this.totalEntities = totalEntities; }
        public int getEntitiesWithText() { return entitiesWithText; }
        public void setEntitiesWithText(int entitiesWithText) { this.entitiesWithText = entitiesWithText; }
        public int getEmptyTextEntities() { return emptyTextEntities; }
        public void setEmptyTextEntities(int emptyTextEntities) { this.emptyTextEntities = emptyTextEntities; }
        public NumericStatistics getWordCountStats() { return wordCountStats; }
        public void setWordCountStats(NumericStatistics wordCountStats) { this.wordCountStats = wordCountStats; }
        public NumericStatistics getCharCountStats() { return charCountStats; }
        public void setCharCountStats(NumericStatistics charCountStats) { this.charCountStats = charCountStats; }
    }
    
    public static class MetricStatistics {
        private String metricName;
        private int entityCount;
        private NumericStatistics numericStats;
        private Map<String, Integer> valueDistribution;
        
        // Getters and setters
        public String getMetricName() { return metricName; }
        public void setMetricName(String metricName) { this.metricName = metricName; }
        public int getEntityCount() { return entityCount; }
        public void setEntityCount(int entityCount) { this.entityCount = entityCount; }
        public NumericStatistics getNumericStats() { return numericStats; }
        public void setNumericStats(NumericStatistics numericStats) { this.numericStats = numericStats; }
        public Map<String, Integer> getValueDistribution() { return valueDistribution; }
        public void setValueDistribution(Map<String, Integer> valueDistribution) { this.valueDistribution = valueDistribution; }
    }
    
    public static class NumericStatistics {
        private int count;
        private int min;
        private int max;
        private double mean;
        private double median;
        private double standardDeviation;
        private int p25;
        private int p75;
        
        // Getters and setters
        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }
        public int getMin() { return min; }
        public void setMin(int min) { this.min = min; }
        public int getMax() { return max; }
        public void setMax(int max) { this.max = max; }
        public double getMean() { return mean; }
        public void setMean(double mean) { this.mean = mean; }
        public double getMedian() { return median; }
        public void setMedian(double median) { this.median = median; }
        public double getStandardDeviation() { return standardDeviation; }
        public void setStandardDeviation(double standardDeviation) { this.standardDeviation = standardDeviation; }
        public int getP25() { return p25; }
        public void setP25(int p25) { this.p25 = p25; }
        public int getP75() { return p75; }
        public void setP75(int p75) { this.p75 = p75; }
    }
    
    public static class MetricAggregateStats {
        private String metricName;
        private int totalEntities;
        private int ordersWithMetric;
        private Map<Integer, MetricStatistics> orderStats;
        private NumericStatistics aggregateNumericStats;
        
        public MetricAggregateStats(String metricName) {
            this.metricName = metricName;
            this.orderStats = new HashMap<>();
        }
        
        public void addOrderStats(Integer orderId, MetricStatistics stats) {
            orderStats.put(orderId, stats);
            totalEntities += stats.getEntityCount();
            ordersWithMetric++;
        }
        
        public void finalizeCalculations(int totalOrders) {
            // Could calculate cross-order statistics here
        }
        
        // Getters and setters
        public String getMetricName() { return metricName; }
        public int getTotalEntities() { return totalEntities; }
        public int getOrdersWithMetric() { return ordersWithMetric; }
        public Map<Integer, MetricStatistics> getOrderStats() { return orderStats; }
        public NumericStatistics getAggregateNumericStats() { return aggregateNumericStats; }
    }
    
    public static class SummaryInsights {
        private int totalEntitiesAnalyzed;
        private int totalOrdersAnalyzed;
        private List<String> entityTypesAnalyzed;
        private List<String> metricsFound;
        private int totalMetricsFound;
        private List<String> dataQualityInsights;
        private List<String> metricDistributionInsights;
        
        // Getters and setters
        public int getTotalEntitiesAnalyzed() { return totalEntitiesAnalyzed; }
        public void setTotalEntitiesAnalyzed(int totalEntitiesAnalyzed) { this.totalEntitiesAnalyzed = totalEntitiesAnalyzed; }
        public int getTotalOrdersAnalyzed() { return totalOrdersAnalyzed; }
        public void setTotalOrdersAnalyzed(int totalOrdersAnalyzed) { this.totalOrdersAnalyzed = totalOrdersAnalyzed; }
        public List<String> getEntityTypesAnalyzed() { return entityTypesAnalyzed; }
        public void setEntityTypesAnalyzed(List<String> entityTypesAnalyzed) { this.entityTypesAnalyzed = entityTypesAnalyzed; }
        public List<String> getMetricsFound() { return metricsFound; }
        public void setMetricsFound(List<String> metricsFound) { this.metricsFound = metricsFound; }
        public int getTotalMetricsFound() { return totalMetricsFound; }
        public void setTotalMetricsFound(int totalMetricsFound) { this.totalMetricsFound = totalMetricsFound; }
        public List<String> getDataQualityInsights() { return dataQualityInsights; }
        public void setDataQualityInsights(List<String> dataQualityInsights) { this.dataQualityInsights = dataQualityInsights; }
        public List<String> getMetricDistributionInsights() { return metricDistributionInsights; }
        public void setMetricDistributionInsights(List<String> metricDistributionInsights) { this.metricDistributionInsights = metricDistributionInsights; }
    }
}
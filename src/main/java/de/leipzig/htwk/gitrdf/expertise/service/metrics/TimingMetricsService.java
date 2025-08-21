package de.leipzig.htwk.gitrdf.expertise.service.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service for tracking comprehensive timing metrics across similarity calculations
 */
@Service
public class TimingMetricsService {

    private static final Logger log = LoggerFactory.getLogger(TimingMetricsService.class);

    /**
     * Thread-local storage for timing data per request
     */
    private final ThreadLocal<Map<String, Instant>> timingData = ThreadLocal.withInitial(ConcurrentHashMap::new);
    private final ThreadLocal<Map<String, Object>> contextData = ThreadLocal.withInitial(HashMap::new);

    /**
     * Start timing for a specific operation
     */
    public void startTiming(String operation) {
        timingData.get().put(operation, Instant.now());
        log.debug("⏱️ Started timing for operation: {}", operation);
    }

    /**
     * End timing for a specific operation and return duration in milliseconds
     */
    public long endTiming(String operation) {
        Instant start = timingData.get().get(operation);
        if (start == null) {
            log.warn("No start time found for operation: {}", operation);
            return 0;
        }
        
        long durationMs = Duration.between(start, Instant.now()).toMillis();
        log.debug("⏱️ Completed operation: {} in {}ms", operation, durationMs);
        return durationMs;
    }

    /**
     * Get timing for operation without ending it
     */
    public long getCurrentTiming(String operation) {
        Instant start = timingData.get().get(operation);
        if (start == null) {
            return 0;
        }
        return Duration.between(start, Instant.now()).toMillis();
    }

    /**
     * Add context data for the current request
     */
    public void addContext(String key, Object value) {
        contextData.get().put(key, value);
    }

    /**
     * Get all timing data for current request
     */
    public Map<String, Long> getAllTimings() {
        Map<String, Long> results = new HashMap<>();
        Map<String, Instant> currentTimings = timingData.get();
        Instant now = Instant.now();
        
        for (Map.Entry<String, Instant> entry : currentTimings.entrySet()) {
            long duration = Duration.between(entry.getValue(), now).toMillis();
            results.put(entry.getKey(), duration);
        }
        
        return results;
    }

    /**
     * Get comprehensive performance report
     */
    public PerformanceReport generateReport() {
        Map<String, Long> timings = getAllTimings();
        Map<String, Object> context = new HashMap<>(contextData.get());
        
        // Calculate total request time
        long totalTime = timings.getOrDefault("request_total", 0L);
        
        // Categorize timings
        Map<String, Long> cacheTimings = new HashMap<>();
        Map<String, Long> dbTimings = new HashMap<>();
        Map<String, Long> embeddingTimings = new HashMap<>();
        Map<String, Long> processingTimings = new HashMap<>();
        
        for (Map.Entry<String, Long> entry : timings.entrySet()) {
            String operation = entry.getKey();
            Long duration = entry.getValue();
            
            if (operation.contains("cache")) {
                cacheTimings.put(operation, duration);
            } else if (operation.contains("db") || operation.contains("database") || operation.contains("query")) {
                dbTimings.put(operation, duration);
            } else if (operation.contains("embedding") || operation.contains("vector")) {
                embeddingTimings.put(operation, duration);
            } else {
                processingTimings.put(operation, duration);
            }
        }
        
        return new PerformanceReport(
            totalTime,
            timings,
            cacheTimings,
            dbTimings,
            embeddingTimings,
            processingTimings,
            context
        );
    }

    /**
     * Clear all timing data for current thread (call at end of request)
     */
    public void clearTimingData() {
        timingData.get().clear();
        contextData.get().clear();
    }

    /**
     * Start request timing with context
     */
    public void startRequest(String requestId, String operation, Map<String, Object> requestContext) {
        startTiming("request_total");
        startTiming("request_" + operation);
        
        addContext("requestId", requestId);
        addContext("operation", operation);
        addContext("timestamp", Instant.now().toString());
        
        if (requestContext != null) {
            requestContext.forEach(this::addContext);
        }
        
        log.info("🚀 Started request: {} for operation: {}", requestId, operation);
    }

    /**
     * End request and generate final report
     */
    public PerformanceReport endRequest() {
        endTiming("request_total");
        PerformanceReport report = generateReport();
        
        log.info(" Request completed in {}ms (breakdown: cache={}ms, db={}ms, embedding={}ms)",
            report.totalTimeMs(),
            report.cacheTimings().values().stream().mapToLong(Long::longValue).sum(),
            report.dbTimings().values().stream().mapToLong(Long::longValue).sum(),
            report.embeddingTimings().values().stream().mapToLong(Long::longValue).sum()
        );
        
        clearTimingData();
        return report;
    }

    /**
     * Performance report record
     */
    public record PerformanceReport(
        long totalTimeMs,
        Map<String, Long> allTimings,
        Map<String, Long> cacheTimings,
        Map<String, Long> dbTimings,
        Map<String, Long> embeddingTimings,
        Map<String, Long> processingTimings,
        Map<String, Object> context
    ) {
        
        /**
         * Get cache performance summary
         */
        public Map<String, Object> getCachePerformance() {
            long totalCacheTime = cacheTimings.values().stream().mapToLong(Long::longValue).sum();
            long cacheHits = (Long) context.getOrDefault("cache_hits", 0L);
            long cacheMisses = (Long) context.getOrDefault("cache_misses", 0L);
            double hitRate = cacheHits + cacheMisses > 0 ? 
                (double) cacheHits / (cacheHits + cacheMisses) * 100 : 0;
            
            return Map.of(
                "totalCacheTimeMs", totalCacheTime,
                "cacheHits", cacheHits,
                "cacheMisses", cacheMisses,
                "hitRatePercent", Math.round(hitRate * 100.0) / 100.0,
                "averageCacheLookupMs", cacheTimings.isEmpty() ? 0 : 
                    totalCacheTime / cacheTimings.size()
            );
        }
        
        /**
         * Get performance breakdown as percentages
         */
        public Map<String, Double> getTimeBreakdown() {
            if (totalTimeMs == 0) return Map.of();
            
            long cacheTime = cacheTimings.values().stream().mapToLong(Long::longValue).sum();
            long dbTime = dbTimings.values().stream().mapToLong(Long::longValue).sum();
            long embeddingTime = embeddingTimings.values().stream().mapToLong(Long::longValue).sum();
            long processingTime = processingTimings.values().stream().mapToLong(Long::longValue).sum();
            
            return Map.of(
                "cachePercent", Math.round((double) cacheTime / totalTimeMs * 10000.0) / 100.0,
                "databasePercent", Math.round((double) dbTime / totalTimeMs * 10000.0) / 100.0,
                "embeddingPercent", Math.round((double) embeddingTime / totalTimeMs * 10000.0) / 100.0,
                "processingPercent", Math.round((double) processingTime / totalTimeMs * 10000.0) / 100.0
            );
        }
    }
}
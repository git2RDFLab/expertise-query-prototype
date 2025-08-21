package de.leipzig.htwk.gitrdf.expertise.service.embedding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import de.leipzig.htwk.gitrdf.expertise.config.EmbeddingServiceConfig;
import de.leipzig.htwk.gitrdf.expertise.model.EmbeddingModel;
import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class BatchEmbeddingService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final VectorSimilarityService vectorSimilarityService;
    private final EmbeddingServiceConfig embeddingConfig;
    
    // Progress tracking
    private volatile long lastProgressLog = 0;
    
    // Single model configuration
    @Value("${expert.embeddings.service-url:#{null}}")
    private String embeddingServiceUrl;
    
    @Value("${expert.embeddings.batch-size:32}")
    private int batchSize;
    
    @Value("${expert.embeddings.max-retries:3}")
    private int maxRetries;
    
    @Value("${expert.embeddings.timeout-seconds:15}")
    private int timeoutSeconds;
    
    @Value("${expert.embeddings.max-parallel-batches:4}")
    private int maxParallelBatches;

    @Value("${expert.embeddings.model-id:qwen3-embedding-8b}")
    private String modelId;

    @Value("${expert.embeddings.input-type:passage}")
    private String inputType;

    // Removed old record classes - using Map for infinity API compatibility

    public Map<String, double[]> generateEmbeddingsInBatches(Map<String, String> entityContextMap, Integer orderId) {
        return generateEmbeddingsInBatches(entityContextMap, orderId, true);
    }
    
    /**
     * Generate embeddings with metric-aware storage
     */
    public Map<String, double[]> generateEmbeddingsInBatchesWithMetrics(
            Map<String, SparqlQueryService.EntityData> entitiesData,
            Map<String, String> entityContextMap, 
            Integer orderId, 
            String metricNameFilter,
            boolean storeEmbeddings) {
        
        log.info("Generating embeddings with metric awareness for {} entities for order {}", 
                entityContextMap.size(), orderId);
        
        // First generate embeddings normally
        Map<String, double[]> allEmbeddings = generateEmbeddingsInBatches(entityContextMap, orderId, false);
        
        if (storeEmbeddings) {
            // Group embeddings by metric type and store separately
            storeEmbeddingsByMetricType(allEmbeddings, entitiesData, orderId, metricNameFilter);
        }
        
        return allEmbeddings;
    }
    
    /**
     * Store embeddings grouped by metric type
     */
    private void storeEmbeddingsByMetricType(
            Map<String, double[]> allEmbeddings, 
            Map<String, SparqlQueryService.EntityData> entitiesData,
            Integer orderId, 
            String metricNameFilter) {
        
        log.info("Storing embeddings by metric type for order {} (filter: {})", 
                orderId, metricNameFilter != null ? metricNameFilter : "none");
        
        // Group entities by their metric types
        Map<String, Map<String, double[]>> embeddingsByMetric = new HashMap<>();
        Map<String, Map<String, Double>> ratingsByMetric = new HashMap<>();
        
        for (Map.Entry<String, double[]> embeddingEntry : allEmbeddings.entrySet()) {
            String entityUri = embeddingEntry.getKey();
            double[] embedding = embeddingEntry.getValue();
            
            SparqlQueryService.EntityData entityData = entitiesData.get(entityUri);
            if (entityData == null) {
                log.warn("No entity data found for URI: {}", entityUri);
                continue;
            }
            
            // Get all metric IDs for this entity
            Set<String> metricIds = entityData.getMetricIds();
            
            if (metricIds.isEmpty()) {
                log.warn("Entity {} has no metric IDs", entityUri);
                // Store in general category
                embeddingsByMetric.computeIfAbsent("general", k -> new HashMap<>()).put(entityUri, embedding);
                continue;
            }
            
            // Store embedding for each metric this entity is associated with
            for (String metricId : metricIds) {
                // Apply metric filter if specified
                if (metricNameFilter != null && !metricNameFilter.equals(metricId)) {
                    continue;
                }
                
                embeddingsByMetric.computeIfAbsent(metricId, k -> new HashMap<>()).put(entityUri, embedding);
                
                // Also collect rating values for this metric
                Double rating = entityData.getRatingForMetric(metricId);
                if (rating != null) {
                    ratingsByMetric.computeIfAbsent(metricId, k -> new HashMap<>()).put(entityUri, rating);
                }
            }
        }
        
        // Store embeddings for each metric type
        for (Map.Entry<String, Map<String, double[]>> metricEntry : embeddingsByMetric.entrySet()) {
            String metricId = metricEntry.getKey();
            Map<String, double[]> metricEmbeddings = metricEntry.getValue();
            Map<String, Double> metricRatings = ratingsByMetric.get(metricId);
            
            log.info("Storing {} embeddings for metric '{}' in order {}", 
                    metricEmbeddings.size(), metricId, orderId);
            
            if (metricRatings != null && !metricRatings.isEmpty()) {
                // Store with rating values
                vectorSimilarityService.storeEmbeddingsWithRatings(metricEmbeddings, orderId, metricId, metricRatings);
                log.info("Stored {} embeddings with ratings for metric '{}'", metricEmbeddings.size(), metricId);
            } else {
                // Store without ratings
                vectorSimilarityService.storeEmbeddings(metricEmbeddings, orderId, metricId);
                log.info("Stored {} embeddings for metric '{}'", metricEmbeddings.size(), metricId);
            }
        }
        
        log.info("Completed storing embeddings for {} different metrics", embeddingsByMetric.size());
    }
    
    public Map<String, double[]> generateEmbeddingsInBatches(Map<String, String> entityContextMap, Integer orderId, boolean storeEmbeddings) {
        long startTime = System.currentTimeMillis();
        lastProgressLog = startTime; // Reset progress timer
        log.info("Starting embedding generation for order {} - {} entities in {} batches of {} (max {} parallel batches)", 
                orderId, entityContextMap.size(), 
                (int) Math.ceil((double) entityContextMap.size() / batchSize), batchSize, maxParallelBatches);
        
        List<Map.Entry<String, String>> entities = new ArrayList<>(entityContextMap.entrySet());
        Map<String, double[]> allEmbeddings = new ConcurrentHashMap<>();
        
        // Process in batches
        List<List<Map.Entry<String, String>>> batches = createBatches(entities, batchSize);
        
        // Process batches in parallel with limited concurrency
        try (ExecutorService executor = Executors.newFixedThreadPool(maxParallelBatches)) {
            List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> CompletableFuture.runAsync(() -> 
                    processBatch(batch, allEmbeddings, orderId), executor))
                .collect(Collectors.toList());
            
            // Wait for all batches to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(timeoutSeconds * batches.size(), TimeUnit.SECONDS)
                .join();
                
        } catch (Exception e) {
            log.error("Error in batch processing for order {}: {}", orderId, e.getMessage());
            throw new RuntimeException("Failed to generate embeddings in batches", e);
        }
        
        // Calculate summary statistics
        int totalEntities = entityContextMap.size();
        int successfulEmbeddings = allEmbeddings.size();
        int failedEmbeddings = totalEntities - successfulEmbeddings;
        long duration = System.currentTimeMillis() - startTime;
        
        // Log final summary
        if (failedEmbeddings == 0) {
            log.info("Embedding generation completed for order {} - {} embeddings created in {}ms", 
                    orderId, successfulEmbeddings, duration);
        } else {
            log.warn("Embedding generation completed for order {} - {} successful, {} failed in {}ms", 
                    orderId, successfulEmbeddings, failedEmbeddings, duration);
        }
        
        // Store embeddings in vector database if requested
        if (storeEmbeddings) {
            log.warn("Storing all embeddings as 'general' type - metric-specific storage not yet implemented");
            vectorSimilarityService.storeEmbeddings(allEmbeddings, orderId, "general");
        }
        
        return allEmbeddings;
    }
    
    private void processBatch(List<Map.Entry<String, String>> batch, Map<String, double[]> allEmbeddings, Integer orderId) {
        // Only log errors during processing, not verbose details
        List<String> contexts = batch.stream()
            .map(entry -> {
                if (entry == null || entry.getValue() == null) {
                    log.error("Null entry or context value in batch for entity: {}", 
                            entry != null && entry.getKey() != null ? entry.getKey() : "unknown-entity");
                    return "";
                }
                return entry.getValue();
            })
            .collect(Collectors.toList());
            
        // Check for processing errors - only log errors with entity URIs
        for (int i = 0; i < batch.size(); i++) {
            Map.Entry<String, String> entry = batch.get(i);
            
            if (entry == null) {
                log.error("Null entry at batch index {}", i);
                continue;
            }
            
            String entityKey = entry.getKey();
            String context = entry.getValue();
            
            if (entityKey == null) {
                log.error("Null entity URI at batch index {}, context: '{}'", i, 
                        context != null ? (context.length() > 50 ? context.substring(0, 50) + "..." : context) : "null");
                continue;
            }
            
            if (context == null || context.trim().isEmpty()) {
                log.error("Empty context for entity: {}", entityKey);
            }
        }
        
        Map<String, double[]> batchEmbeddings = generateEmbeddingsWithRetry(contexts, batch);
        allEmbeddings.putAll(batchEmbeddings);
    }
    
    private Map<String, double[]> generateEmbeddingsWithRetry(List<String> contexts, List<Map.Entry<String, String>> entityBatch) {
        // Check if external service is configured
        if (embeddingServiceUrl == null || embeddingServiceUrl.trim().isEmpty()) {
            log.error("Embedding service error: No embedding service URL configured");
            log.error("Expected property: expert.embeddings.service-url");
            log.error("Current value: {}", embeddingServiceUrl);
            log.error("Please configure a valid embedding service endpoint");
            throw new RuntimeException("No embedding service configured. Set 'expert.embeddings.service-url' property.");
        }
        
        // Only log connection details when debugging
        log.debug("Attempting to connect to embedding service: {} with {} texts", embeddingServiceUrl, contexts.size());
        
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("Attempt {}/{} to generate embeddings for batch of {} texts", attempt, maxRetries, contexts.size());
                
                Map<String, double[]> result = callEmbeddingService(contexts, entityBatch);
                // Success - no logging needed, progress is shown with dots
                return result;
                
            } catch (Exception e) {
                lastException = e;
                log.error("Attempt {}/{} failed: {} - {}", attempt, maxRetries, e.getClass().getSimpleName(), e.getMessage());
                if (e.getCause() != null) {
                    log.debug("Root cause: {}", e.getCause().getMessage());
                }
                
                if (attempt < maxRetries) {
                    long sleepTime = 1000L * attempt;
                    log.warn("⏳ Waiting {}ms before retry {}/{}", sleepTime, attempt + 1, maxRetries);
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry backoff", ie);
                    }
                } else {
                    log.error("All retries exhausted - cannot generate embeddings without external service at {}", embeddingServiceUrl);
                    log.error("Troubleshooting checklist:");
                    log.error("  1. Is the embedding service container running? (docker ps | grep sentence)");
                    log.error("  2. Is the service accessible? (curl {})", embeddingServiceUrl);
                    log.error("  3. Are you on the correct Docker network?");
                    log.error("  4. Check service logs: docker logs <container_name>");
                    log.error("  5. Verify service URL configuration: {}", embeddingServiceUrl);
                }
            }
        }
        
        throw new RuntimeException(
            String.format("Failed to generate embeddings after %d attempts. Last error: %s", 
                maxRetries, lastException.getMessage()), lastException);
    }
    
    private Map<String, double[]> callEmbeddingService(List<String> contexts, List<Map.Entry<String, String>> entityBatch) {
        // External embedding service API endpoint (URL should include full path)
        String fullUrl = embeddingServiceUrl;
        // New API format: {"input": ["text1", "text2"], "model": "model-name", "dimensions": 4096}
        Map<String, Object> request = new HashMap<>();
        request.put("input", contexts);
        request.put("model", modelId);
        request.put("dimensions", embeddingConfig.getDimensions());
        
        log.debug("Making HTTP POST to: {} with {} texts, dimensions: {}", fullUrl, contexts.size(), embeddingConfig.getDimensions());
        
        try {
            // External embedding API - track request timing
            long requestStartTime = System.currentTimeMillis();
            Map<String, Object> response = restTemplate.postForObject(fullUrl, request, Map.class);
            long requestEndTime = System.currentTimeMillis();
            
            // Log transformer request timing
            
            if (response == null) {
                log.error("Null response from embedding service at {}", fullUrl);
                throw new RuntimeException("Null response from embedding service at " + fullUrl);
            }
            
            return parseEmbeddingResponse(response, entityBatch, contexts);
            
        } catch (Exception e) {
            log.error("HTTP request failed to {}: {} - {}", fullUrl, e.getClass().getSimpleName(), e.getMessage());
            if (e.getCause() != null) {
                log.debug("Root cause: {}", e.getCause().getMessage());
            }
            throw e;
        }
    }
    
    /**
     * Parse standard OpenAI-style embedding response format
     */
    private Map<String, double[]> parseEmbeddingResponse(Map<String, Object> response, List<Map.Entry<String, String>> entityBatch, List<String> contexts) {
        Map<String, double[]> embeddings = new HashMap<>();
        
        log.debug("Parsing embedding response for {} entities", entityBatch.size());
        
        // OpenAI-style API response format: {"data": [{"object": "embedding", "embedding": [...], "index": 0}], "model": "...", "usage": {...}}
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) response.get("data");
        
        if (dataList == null) {
            log.error("Invalid response from embedding service: data field is null, response keys: {}", response.keySet());
            throw new RuntimeException("Embedding service returned null data field");
        }
        
        if (dataList.size() != contexts.size()) {
            log.error("Embedding count mismatch: expected {}, got {} from model {}", contexts.size(), dataList.size(), response.get("model"));
            throw new RuntimeException(
                String.format("Embedding count mismatch: expected %d, got %d", 
                    contexts.size(), dataList.size()));
        }
        
        // Log progress every 10 seconds
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastProgressLog > 10000) {
            log.info("Batch processing in progress...");
            lastProgressLog = currentTime;
        }
        
        for (int i = 0; i < entityBatch.size(); i++) {
            Map.Entry<String, String> entry = entityBatch.get(i);
            
            // Handle null entries
            if (entry == null) {
                log.error("Null entry at index {} in entity batch", i);
                continue;
            }
            
            String entityUri = entry.getKey();
            if (entityUri == null) {
                log.error("Null entity URI at index {} in entity batch", i);
                continue;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> embeddingData = dataList.get(i);
            @SuppressWarnings("unchecked")
            List<Double> embeddingList = (List<Double>) embeddingData.get("embedding");
            
            if (embeddingList == null || embeddingList.isEmpty()) {
                log.error("Empty embedding at index {} for entity: {}, data keys: {}", i, entityUri, embeddingData.keySet());
                throw new RuntimeException("Empty embedding received for entity: " + entityUri);
            }
            
            double[] embedding = embeddingList.stream().mapToDouble(Double::doubleValue).toArray();
            embeddings.put(entityUri, embedding);
            
            if (i == 0) {
                log.debug("First embedding dimensions: {}", embedding.length);
            }
        }
        
        log.debug("Successfully parsed {} embeddings from response (model: {})", 
            embeddings.size(), response.get("model"));
        
        return embeddings;
    }
    
    // FALLBACK METHODS REMOVED - System will fail fast with clear error messages
    // This forces proper embedding service configuration instead of producing meaningless results
    
    private <T> List<List<T>> createBatches(List<T> items, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, items.size());
            batches.add(items.subList(i, endIndex));
        }
        return batches;
    }
    
    /**
     * Store embeddings directly to database (used for cached embeddings)
     */
    public void storeEmbeddingsInDatabase(Map<String, double[]> embeddings, Integer orderId) {
        storeEmbeddingsInDatabase(embeddings, orderId, null);
    }
    
    /**
     * Store embeddings directly to database with metric type
     */
    public void storeEmbeddingsInDatabase(Map<String, double[]> embeddings, Integer orderId, String metricType) {
        log.info("Storing {} cached embeddings for order {} directly to database (metric: {})", 
                embeddings.size(), orderId, metricType != null ? metricType : "general");
        vectorSimilarityService.storeEmbeddings(embeddings, orderId, metricType);
        log.info("Successfully stored {} cached embeddings for order {} in database", embeddings.size(), orderId);
    }
    
    /**
     * Store embeddings directly to database with metric type and rating values
     */
    public void storeEmbeddingsWithRatingsInDatabase(Map<String, double[]> embeddings, Integer orderId, String metricType, Map<String, Double> entityRatings) {
        log.info("Storing {} cached embeddings with ratings for order {} directly to database (metric: {})", 
                embeddings.size(), orderId, metricType != null ? metricType : "general");
        vectorSimilarityService.storeEmbeddingsWithRatings(embeddings, orderId, metricType, entityRatings);
        log.info("Successfully stored {} cached embeddings with ratings for order {} in database", embeddings.size(), orderId);
    }
    
    /**
     * Wipe all embeddings from database
     */
    public void wipeAllEmbeddings() {
        log.info("Wiping all embeddings from database...");
        vectorSimilarityService.clearAllEmbeddings();
        log.info("All embeddings wiped from database");
    }
    
    public Map<String, Object> getBatchConfiguration() {
        return Map.of(
            "embeddingServiceUrl", embeddingServiceUrl,
            "batchSize", batchSize,
            "maxRetries", maxRetries,
            "timeoutSeconds", timeoutSeconds,
            "maxParallelBatches", maxParallelBatches
        );
    }
    
    // ========== NEW DUAL MODEL METHODS ==========
    
    /**
     * Result class for embedding generation with character length tracking
     */
    public static class EmbeddingResult {
        private final Map<String, double[]> embeddings;
        private final Map<String, Integer> characterLengths;
        
        public EmbeddingResult(Map<String, double[]> embeddings, Map<String, Integer> characterLengths) {
            this.embeddings = embeddings;
            this.characterLengths = characterLengths;
        }
        
        public Map<String, double[]> getEmbeddings() { return embeddings; }
        public Map<String, Integer> getCharacterLengths() { return characterLengths; }
    }

    /**
     * Generate embeddings using specified model
     */
    public Map<String, double[]> generateEmbeddingsWithModel(
            Map<String, String> entityContextMap, 
            EmbeddingModel model, 
            Integer orderId) {
        return generateEmbeddingsWithModelAndLengths(entityContextMap, model, orderId).getEmbeddings();
    }
    
    /**
     * Generate embeddings using specified model with character length tracking
     */
    public EmbeddingResult generateEmbeddingsWithModelAndLengths(
            Map<String, String> entityContextMap, 
            EmbeddingModel model, 
            Integer orderId) {
        
        embeddingConfig.validateConfiguration();
        
        log.info("Generating {} embeddings for {} entities (order: {})", 
                model.getKey(), entityContextMap.size(), orderId);
        
        if (entityContextMap.isEmpty()) {
            log.warn("Empty entity context map provided");
            return new EmbeddingResult(new HashMap<>(), new HashMap<>());
        }
        
        Map<String, double[]> allEmbeddings = new HashMap<>();
        Map<String, Integer> allCharacterLengths = new HashMap<>();
        List<String> entityUris = new ArrayList<>(entityContextMap.keySet());
        
        // Process in batches
        for (int i = 0; i < entityUris.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, entityUris.size());
            List<String> batchEntityUris = entityUris.subList(i, endIndex);
            
            // Create batch context map and track character lengths
            Map<String, String> batchContextMap = new HashMap<>();
            for (String entityUri : batchEntityUris) {
                String context = entityContextMap.get(entityUri);
                batchContextMap.put(entityUri, context);
                allCharacterLengths.put(entityUri, context != null ? context.length() : 0);
            }
            
            // Generate embeddings for this batch
            Map<String, double[]> batchEmbeddings = generateEmbeddingsBatchWithModel(
                    batchContextMap, model, orderId);
            
            allEmbeddings.putAll(batchEmbeddings);
            
            // Progress logging
            logProgress(i + batchContextMap.size(), entityUris.size(), orderId);
        }
        
        log.info("Completed {} embedding generation - {} embeddings created", 
                model.getKey(), allEmbeddings.size());
        
        return new EmbeddingResult(allEmbeddings, allCharacterLengths);
    }
    
    /**
     * Generate embeddings for a single batch with specified model
     */
    private Map<String, double[]> generateEmbeddingsBatchWithModel(
            Map<String, String> batchContextMap,
            EmbeddingModel model,
            Integer orderId) {
        
        List<String> texts = new ArrayList<>(batchContextMap.values());
        
        // Prepare request for embedding API (new NDJSON format)
        Map<String, Object> request = new HashMap<>();
        request.put("input", texts);
        request.put("model", model.getModelId());
        request.put("dimensions", model.getDimensions());
        
        try {
            // Call embedding service - track request timing
            long requestStartTime = System.currentTimeMillis();
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.postForObject(
                embeddingServiceUrl, 
                request, 
                Map.class);
            long requestEndTime = System.currentTimeMillis();
            
            
            if (response == null || !response.containsKey("data")) {
                throw new RuntimeException("Invalid response from embedding service");
            }
            
            // Convert context map to entity batch format for parsing
            List<Map.Entry<String, String>> entityBatch = new ArrayList<>(batchContextMap.entrySet());
            
            return parseEmbeddingResponse(response, entityBatch, texts);
            
        } catch (Exception e) {
            log.error("Failed to generate {} embeddings for batch: {}", model.getKey(), e.getMessage());
            throw new RuntimeException("Embedding generation failed for model: " + model.getKey(), e);
        }
    }
    
    /**
     * Log progress for dual model processing
     */
    private void logProgress(int processed, int total, Integer orderId) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastProgressLog > 5000) { // Log every 5 seconds
            int percentage = (int) ((processed * 100.0) / total);
            log.info("Progress: {}/{} ({}%) embeddings processed for order {}", 
                    processed, total, percentage, orderId);
            lastProgressLog = currentTime;
        }
    }
    
    /**
     * Check if embedding service is available for a specific model
     */
    public void checkEmbeddingServiceAvailability(EmbeddingModel model) {
        embeddingConfig.validateConfiguration();
        
        try {
            log.info("Checking {} embedding service availability at: {}", 
                    model.getKey(), embeddingServiceUrl);
            
            String healthUrl = embeddingServiceUrl.replace("/v1/embeddings", "/docs");
            restTemplate.getForEntity(healthUrl, String.class);
            
            log.info("{} embedding service is available and responding", model.getKey());
            
        } catch (Exception e) {
            log.error("{} embedding service is not available at: {}", 
                    model.getKey(), embeddingServiceUrl);
            throw new RuntimeException(
                String.format("%s embedding service is not available. Error: %s. " +
                    "Please ensure the external embedding service is running and accessible.",
                    model.getKey(), e.getMessage()));
        }
    }
}
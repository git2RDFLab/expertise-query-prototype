package de.leipzig.htwk.gitrdf.expertise.service.embedding;

import de.leipzig.htwk.gitrdf.expertise.entity.EntityEmbedding;
import de.leipzig.htwk.gitrdf.expertise.repository.EntityEmbeddingRepository;
import de.leipzig.htwk.gitrdf.expertise.model.EntityType;
import de.leipzig.htwk.gitrdf.expertise.model.EmbeddingModel;
import de.leipzig.htwk.gitrdf.expertise.service.EntityTypeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

@Service
@Slf4j
public class VectorSimilarityService {

    private final EntityEmbeddingRepository embeddingRepository;
    private final JdbcTemplate vectorJdbcTemplate;
    private final EntityTypeService entityTypeService;
    
    @Value("${expert.vector.similarity-threshold:0.6}")
    private double similarityThreshold;
    
    @Value("${expert.vector.max-candidates:100}")
    private int maxCandidates;
    
    @Value("${expert.vector.batch-size:1000}")
    private int batchSize;
    
    @Value("${expert.vector.enable-pgvector:false}")
    private boolean enablePgvector;
    
    @Value("${expert.vector.max-parallel-threads:4}")
    private int maxParallelThreads;
    
    @Value("${expert.vector.filter-by-entity-type:false}")
    private boolean filterByEntityType;

    public VectorSimilarityService(EntityEmbeddingRepository embeddingRepository, 
                                 DataSource dataSource,
                                 EntityTypeService entityTypeService) {
        this.embeddingRepository = embeddingRepository;
        this.vectorJdbcTemplate = new JdbcTemplate(dataSource);
        this.entityTypeService = entityTypeService;
    }

    public record SimilarityResult(String entityUri, String entityType, String metricType, Integer orderId, Double ratingValue, double similarity, Integer characterLength) {}
    
    public enum SortBy {
        SIMILARITY, // Default: sort by vector similarity
        BEST_RATED, // Sort by highest rating first, then similarity
        WORST_RATED // Sort by lowest rating first, then similarity
    }

    public void storeEmbeddings(Map<String, double[]> embeddings, Integer orderId) {
        log.error("Legacy storeEmbeddings method called without metric type - this should not happen");
        log.error("Stack trace to identify caller:");
        Thread.dumpStack();
        throw new RuntimeException("Attempted to store embeddings without metric type - this should not happen!");
    }
    
    public void storeEmbeddings(Map<String, double[]> embeddings, Integer orderId, String metricType) {
        storeEmbeddings(embeddings, orderId, metricType, "text-based"); // Default strategy
    }
    
    public void storeEmbeddings(Map<String, double[]> embeddings, Integer orderId, String metricType, String strategy) {
        int totalEmbeddings = embeddings.size();
        log.info("Storing {} embeddings for order {} (metric: {}, strategy: {})", totalEmbeddings, orderId, metricType != null ? metricType : "all", strategy);
        
        storeEmbeddingsWithNativeSQL(embeddings, orderId, metricType, null, strategy);
        
        log.info("Successfully stored {} embeddings for order {} (metric: {}, strategy: {})", totalEmbeddings, orderId, metricType != null ? metricType : "all", strategy);
    }
    
    public void storeEmbeddingsWithRatings(Map<String, double[]> embeddings, Integer orderId, String metricType, Map<String, Double> entityRatings) {
        storeEmbeddingsWithRatings(embeddings, orderId, metricType, entityRatings, "text-based"); // Default strategy
    }
    
    public void storeEmbeddingsWithRatings(Map<String, double[]> embeddings, Integer orderId, String metricType, Map<String, Double> entityRatings, String strategy) {
        storeEmbeddingsWithRatings(embeddings, orderId, metricType, entityRatings, strategy, null);
    }
    
    public void storeEmbeddingsWithRatings(Map<String, double[]> embeddings, Integer orderId, String metricType, Map<String, Double> entityRatings, String strategy, Map<String, String> entityTypes) {
        int totalEmbeddings = embeddings.size();
        int embeddingsWithRatings = entityRatings != null ? entityRatings.size() : 0;
        log.info("Storing {} embeddings ({} with ratings) for order {} (metric: {}, strategy: {})", 
                totalEmbeddings, embeddingsWithRatings, orderId, metricType != null ? metricType : "all", strategy);
        
        storeEmbeddingsWithNativeSQL(embeddings, orderId, metricType, entityRatings, strategy, entityTypes);
        
        log.info("Successfully stored {} embeddings with ratings for order {} (metric: {}, strategy: {})", 
                totalEmbeddings, orderId, metricType != null ? metricType : "all", strategy);
    }
    
    private void storeEmbeddingsWithNativeSQL(Map<String, double[]> embeddings, Integer orderId) {
        storeEmbeddingsWithNativeSQL(embeddings, orderId, null, null, "text-based");
    }
    
    private void storeEmbeddingsWithNativeSQL(Map<String, double[]> embeddings, Integer orderId, String metricType) {
        storeEmbeddingsWithNativeSQL(embeddings, orderId, metricType, null, "text-based");
    }
    
    private void storeEmbeddingsWithNativeSQL(Map<String, double[]> embeddings, Integer orderId, String metricType, Map<String, Double> entityRatings, String strategy) {
        storeEmbeddingsWithNativeSQL(embeddings, orderId, metricType, entityRatings, strategy, null);
    }
    
    private void storeEmbeddingsWithNativeSQL(Map<String, double[]> embeddings, Integer orderId, String metricType, Map<String, Double> entityRatings, String strategy, Map<String, String> entityTypes) {
        // Clear existing embeddings for this order and metric combination only
        String deleteSQL = metricType != null ? 
            "DELETE FROM entity_embeddings WHERE order_id = ? AND metric_type = ?" :
            "DELETE FROM entity_embeddings WHERE order_id = ?";
            
        try {
            if (metricType != null) {
                int deletedRows = vectorJdbcTemplate.update(deleteSQL, orderId, metricType);
                log.info("Cleared {} existing embeddings for order {} and metric {}", deletedRows, orderId, metricType);
            } else {
                int deletedRows = vectorJdbcTemplate.update(deleteSQL, orderId);
                log.info("Cleared {} existing embeddings for order {}", deletedRows, orderId);
            }
        } catch (Exception e) {
            log.warn("Could not clear existing embeddings: {}", e.getMessage());
        }
        
        // Updated to use single embedding + dimensions approach
        String insertSQL = """
            INSERT INTO entity_embeddings (entity_uri, order_id, entity_type, metric_type, rating_value, strategy, 
                                         embedding, dimensions, model_name, character_length, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?::vector, ?, ?, ?, NOW())
            """;
        
        int successCount = 0;
        for (Map.Entry<String, double[]> entry : embeddings.entrySet()) {
            Double ratingValue = entityRatings != null ? entityRatings.get(entry.getKey()) : null;
            String entityType = entityTypes != null ? entityTypes.get(entry.getKey()) : null;
            successCount += storeEmbeddingWithIsolation(entry, orderId, metricType, ratingValue, strategy, insertSQL, successCount, entityType);
        }
        
        if (successCount == embeddings.size()) {
            log.info("Successfully stored all {} embeddings", successCount);
        } else {
            log.warn("Stored {}/{} embeddings - {} failed", successCount, embeddings.size(), embeddings.size() - successCount);
        }
        
        if (successCount == 0) {
            throw new RuntimeException("Failed to store any embeddings for order " + orderId);
        }
    }
    
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    private int storeEmbeddingWithIsolation(Map.Entry<String, double[]> entry, Integer orderId, String metricType, Double ratingValue, String strategy, String insertSQL, int currentSuccessCount, String entityType) {
        String vectorString = doubleArrayToVectorString(entry.getValue());
        
        // Use provided entity type if available, otherwise extract from URI using EntityTypeService
        String rawEntityType = entityType;
        if (rawEntityType == null) {
            try {
                EntityType extractedType = EntityType.fromUri(entry.getKey());
                rawEntityType = extractedType.getNormalizedName();
            } catch (IllegalArgumentException e) {
                log.warn("Cannot determine entity type from URI: {} - {}", entry.getKey(), e.getMessage());
                rawEntityType = "unknown";
            }
        }
        
        // Normalize the entity type using the new system
        String normalizedEntityType;
        try {
            EntityType normalized = entityTypeService.normalizeEntityType(rawEntityType);
            normalizedEntityType = normalized.getNormalizedName();
        } catch (IllegalArgumentException e) {
            log.error("Failed to normalize entity type '{}' for {}: {}", rawEntityType, entry.getKey(), e.getMessage());
            throw new RuntimeException("Invalid entity type: " + rawEntityType, e);
        }
        
        try {
            int dimensions = entry.getValue().length;
            String modelName = dimensions == 384 ? "all-MiniLM-L6-v2" : "all-mpnet-base-v2";
            
            int updated = vectorJdbcTemplate.update(insertSQL,
                entry.getKey(),                     // entity_uri
                orderId,                           // order_id
                normalizedEntityType,              // entity_type (normalized!)
                metricType,                        // metric_type
                ratingValue,                       // rating_value
                strategy,                          // strategy
                vectorString,                      // embedding
                dimensions,                        // dimensions
                modelName,                         // model_name
                null                               // character_length (will be populated from BatchEmbeddingService)
            );
            
            if (entityType != null) {
                log.debug("Stored embedding for {} as normalized type '{}' (from provided '{}')", 
                    entry.getKey(), normalizedEntityType, entityType);
            } else {
                log.debug("Stored embedding for {} as normalized type '{}' (from URI-derived '{}')", 
                    entry.getKey(), normalizedEntityType, rawEntityType);
            }
            
            return updated > 0 ? 1 : 0;
        } catch (Exception e) {
            log.error("Embedding failed for {}: {}", entry.getKey(), e.getMessage());
            log.error("Vector sample: {}", vectorString.substring(0, Math.min(50, vectorString.length())));
            throw new RuntimeException("Embedding insertion failed", e);
        }
    }
    
    private void saveInBatches(List<EntityEmbedding> entities) {
        for (int i = 0; i < entities.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, entities.size());
            List<EntityEmbedding> batch = entities.subList(i, endIndex);
            embeddingRepository.saveAll(batch);
            log.debug("Saved batch {}-{} of {}", i + 1, endIndex, entities.size());
        }
    }

    public List<SimilarityResult> findSimilarEntities(String queryEntityUri, Integer queryOrderId, int topK) {
        return findSimilarEntities(queryEntityUri, queryOrderId, null, topK);
    }
    
    /**
     * Find similar entities with optional metric type filtering
     * If query entity is not in embeddings database, fetch content via SPARQL and generate embedding on-the-fly
     */
    public List<SimilarityResult> findSimilarEntities(String queryEntityUri, Integer queryOrderId, String metricType, int topK) {
        return findSimilarEntities(queryEntityUri, queryOrderId, metricType, topK, SortBy.SIMILARITY);
    }
    
    public List<SimilarityResult> findSimilarEntities(String queryEntityUri, Integer queryOrderId, String metricType, int topK, SortBy sortBy) {
        log.info("Finding {} similar entities to '{}' from order {} with metric: {} (sort: {})", 
                topK, queryEntityUri, queryOrderId, metricType != null ? metricType : "all", sortBy);
        
        // NOTE: We should NOT look for the query entity in stored embeddings!
        // The whole point is to find SIMILAR entities, not the same entity.
        // The SimilarityService should generate the query embedding on-the-fly.
        
        log.warn("VectorSimilarityService.findSimilarEntities() should not be used for on-the-fly queries");
        log.info("Use findSimilarWithPgVectorFromQueryVector() instead with generated embedding");
        return List.of();
    }
    
    /**
     * Generate embedding for query entity on-the-fly and search for similar entities
     * This method requires QueryEmbeddingService injection
     */
    public List<SimilarityResult> findSimilarEntitiesWithOnTheFlyEmbedding(String queryEntityUri, Integer queryOrderId, String metricType, int topK) {
        // This will be handled by QueryEmbeddingService to avoid circular dependencies
        log.warn("On-the-fly embedding generation requested but not available in this service");
        return List.of();
    }
    
    /**
     * Public accessor for doubleArrayToVectorString method (used by QueryEmbeddingService)
     */
    public String convertDoubleArrayToVectorString(double[] vector) {
        return doubleArrayToVectorString(vector);
    }
    
    /**
     * Search with a query vector string (used by QueryEmbeddingService)
     */
    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVector(String queryVector, String excludeUri, Integer orderId, String metricType, int topK) {
        return findSimilarWithPgVector(queryVector, excludeUri, orderId, metricType, topK, SortBy.SIMILARITY, "fast"); // Default to fast model
    }
    
    /**
     * Search with a query vector string with rating-based sorting (used by QueryEmbeddingService)
     */
    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVector(String queryVector, String excludeUri, Integer orderId, String metricType, int topK, SortBy sortBy) {
        return findSimilarWithPgVector(queryVector, excludeUri, orderId, metricType, topK, sortBy, "fast"); // Default to fast model
    }
    
    /**
     * Search with a query vector string, entity type filtering, and rating-based sorting
     */
    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVector(String queryVector, String excludeUri, Integer orderId, String metricType, String entityType, int topK, SortBy sortBy) {
        return findSimilarWithPgVectorAndEntityType(queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy, "fast"); // Default to fast model
    }
    
    /**
     * ENHANCED: Search with custom similarity metric support
     * Routes to the appropriate pgvector operator based on similarity metric
     */
    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVector(
            String queryVector, String excludeUri, Integer orderId, String metricType, 
            String entityType, int topK, SortBy sortBy, 
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric) {
        
        log.info("Enhanced similarity search using {} operator: {}", 
                similarityMetric.getPgVectorOperator(), similarityMetric);
        
        return findSimilarWithPgVectorAndEntityTypeAndMetric(
            queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy, similarityMetric, "fast"); // Default to fast model
    }
    
    /**
     * NEW: Search with dual model support - specify which embedding model to use
     */
    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVector(
            String queryVector, String excludeUri, Integer orderId, String metricType, 
            String entityType, int topK, SortBy sortBy, 
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric,
            String modelType) {
        
        log.info("Dual model similarity search using {} operator with {} model: {}", 
                similarityMetric.getPgVectorOperator(), modelType, similarityMetric);
        
        return findSimilarWithPgVectorAndEntityTypeAndMetric(
            queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy, similarityMetric, modelType);
    }
    
    /**
     * Find similar entities across ALL orders (all expert opinions combined)
     * This gives the most comprehensive similarity search across all rated entities
     */
    public List<SimilarityResult> findSimilarEntitiesAcrossAllOrders(String queryEntityUri, int topK) {
        log.info("Finding {} similar entities to '{}' across ALL orders (all expert opinions)", topK, queryEntityUri);
        
        // First, find the query entity in any order
        List<EntityEmbedding> queryCandidates = embeddingRepository.findByEntityUriIn(List.of(queryEntityUri));
            
        if (queryCandidates.isEmpty()) {
            log.warn("Query entity '{}' not found in any embeddings", queryEntityUri);
            return List.of();
        }
        
        // Use the first occurrence of the entity (they should have similar embeddings anyway)
        EntityEmbedding queryEmbedding = queryCandidates.get(0);
        String queryVector = queryEmbedding.getEmbedding(); // Get embedding regardless of model
        
        log.info("Found query entity '{}' in order {} (searching across all orders)", queryEntityUri, queryEmbedding.getOrderId());
        
        // Search across ALL orders (no orderId restriction)
        return findSimilarWithPgVector(queryVector, queryEntityUri, null, null, topK, SortBy.SIMILARITY);
    }
    
    private List<SimilarityResult> findSimilarWithPgVector(String queryVector, String excludeUri, Integer orderId, String metricType, int topK) {
        return findSimilarWithPgVector(queryVector, excludeUri, orderId, metricType, topK, SortBy.SIMILARITY);
    }
    
    private List<SimilarityResult> findSimilarWithPgVectorAndEntityType(String queryVector, String excludeUri, Integer orderId, String metricType, String entityType, int topK, SortBy sortBy) {
        return findSimilarWithPgVectorAndEntityType(queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy, "fast"); // Default to fast model
    }
    
    private List<SimilarityResult> findSimilarWithPgVectorAndEntityType(String queryVector, String excludeUri, Integer orderId, String metricType, String entityType, int topK, SortBy sortBy, String modelType) {
        // Normalize entity type for database query
        String normalizedEntityType = null;
        if (entityType != null) {
            try {
                EntityType normalized = entityTypeService.normalizeEntityType(entityType);
                normalizedEntityType = normalized.getNormalizedName();
                if (!entityType.equals(normalizedEntityType)) {
                    log.debug("Normalized entity type: '{}' -> '{}'", entityType, normalizedEntityType);
                }
            } catch (IllegalArgumentException e) {
                log.error("Invalid entity type for similarity search: '{}' - {}", entityType, e.getMessage());
                throw new RuntimeException("Invalid entity type: " + entityType, e);
            }
        }
        
        log.info("Similarity search: {} similar to {} (order: {}, metric: {}, entityType: {})", 
            topK, normalizedEntityType != null ? normalizedEntityType : "any", orderId, metricType, normalizedEntityType);
        
        try {
            List<Object[]> results;
            if (metricType != null) {
                if (filterByEntityType && normalizedEntityType != null) {
                    // Use entity type filtering with normalized entityType
                    switch (sortBy) {
                        case BEST_RATED:
                            results = embeddingRepository.findSimilarByVectorMetricAndEntityTypeOrderByRatingDesc(
                                    queryVector, excludeUri, metricType, normalizedEntityType, topK);
                            break;
                        case WORST_RATED:
                            results = embeddingRepository.findSimilarByVectorMetricAndEntityTypeOrderByRatingAsc(
                                    queryVector, excludeUri, metricType, normalizedEntityType, topK);
                            break;
                        case SIMILARITY:
                        default:
                            results = embeddingRepository.findSimilarByVectorMetricAndEntityType(
                                    queryVector, excludeUri, metricType, normalizedEntityType, topK);
                            break;
                    }
                } else {
                    // No entity type filtering (cross-type similarity)
                    switch (sortBy) {
                        case BEST_RATED:
                            results = embeddingRepository.findSimilarByVectorAndMetricOrderByRatingDesc(queryVector, excludeUri, metricType, topK);
                            break;
                        case WORST_RATED:
                            results = embeddingRepository.findSimilarByVectorAndMetricOrderByRatingAsc(queryVector, excludeUri, metricType, topK);
                            break;
                        case SIMILARITY:
                        default:
                            results = embeddingRepository.findSimilarByVectorAndMetricCosineWithModel(queryVector, excludeUri, metricType, 384, topK); // Default to fast model (384D)
                            break;
                    }
                }
            } else {
                // Always search across ALL orders - orderId is only for fetching raw data
                // Use dual model approach with default "general" metric for no-metric queries
                log.warn("Generic similarity search without metric filter - using cosine similarity with fast model");
                results = embeddingRepository.findSimilarByVectorAndMetricCosineWithModel(queryVector, excludeUri, "general", 384, topK);
            }
            
            List<SimilarityResult> similarityResults = results.stream()
                .map(this::mapResultToSimilarityResult)
                .collect(Collectors.toList());
                
            if (!similarityResults.isEmpty()) {
                double minSim = similarityResults.stream().mapToDouble(SimilarityResult::similarity).min().orElse(0.0);
                double maxSim = similarityResults.stream().mapToDouble(SimilarityResult::similarity).max().orElse(0.0);
                log.info("Found {} results, similarity range: {:.3f} - {:.3f}", 
                        similarityResults.size(), minSim, maxSim);
            }
            
            return similarityResults;
                
        } catch (Exception e) {
            log.error("pgvector query failed: {}", e.getMessage());
            throw new RuntimeException("Vector similarity search failed - pgvector extension required", e);
        }
    }
    
    /**
     * Enhanced search method that routes to appropriate repository method based on similarity metric
     */
    private List<SimilarityResult> findSimilarWithPgVectorAndEntityTypeAndMetric(
            String queryVector, String excludeUri, Integer orderId, String metricType, 
            String entityType, int topK, SortBy sortBy, 
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric) {
        return findSimilarWithPgVectorAndEntityTypeAndMetric(queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy, similarityMetric, "fast"); // Default to fast model
    }
    
    private List<SimilarityResult> findSimilarWithPgVectorAndEntityTypeAndMetric(
            String queryVector, String excludeUri, Integer orderId, String metricType, 
            String entityType, int topK, SortBy sortBy, 
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric,
            String modelType) {
        
        // Normalize entity type for database query
        String normalizedEntityType = null;
        if (entityType != null) {
            try {
                EntityType parsedType = EntityType.fromString(entityType);
                normalizedEntityType = parsedType.getNormalizedName();
            } catch (IllegalArgumentException e) {
                log.warn("Invalid entity type: {}, proceeding without entity type filtering", entityType);
            }
        }
        
        try {
            List<Object[]> results;
            
            if (metricType != null) {
                if (normalizedEntityType != null && filterByEntityType) {
                    // Entity type filtering + custom similarity metric
                    results = executeQueryWithSimilarityMetric(
                        queryVector, excludeUri, metricType, normalizedEntityType, topK, sortBy, similarityMetric, true, modelType);
                } else {
                    // No entity type filtering, just metric + similarity metric
                    results = executeQueryWithSimilarityMetric(
                        queryVector, excludeUri, metricType, null, topK, sortBy, similarityMetric, false, modelType);
                }
            } else {
                // Fallback to default behavior for no metric filtering
                return findSimilarWithPgVectorAndEntityType(queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy);
            }
            
            List<SimilarityResult> similarityResults = results.stream()
                .map(this::mapResultToSimilarityResult)
                .collect(Collectors.toList());
            
            if (!similarityResults.isEmpty()) {
                double minSim = similarityResults.stream().mapToDouble(SimilarityResult::similarity).min().orElse(0.0);
                double maxSim = similarityResults.stream().mapToDouble(SimilarityResult::similarity).max().orElse(0.0);
                log.info("Found {} similar entities using {} (range: {:.3f} - {:.3f})", 
                        similarityResults.size(), similarityMetric.getPgVectorOperator(), minSim, maxSim);
            } else {
                log.info("No similar entities found using {} for metric: {}", 
                        similarityMetric.getPgVectorOperator(), metricType);
            }
            
            return similarityResults;
            
        } catch (Exception e) {
            log.error("Error in similarity search with {}: {}", similarityMetric, e.getMessage());
            throw new RuntimeException("Similarity search failed", e);
        }
    }
    
    /**
     * Execute query with the appropriate similarity metric
     */
    private List<Object[]> executeQueryWithSimilarityMetric(
            String queryVector, String excludeUri, String metricType, String entityType,
            int topK, SortBy sortBy, 
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric,
            boolean withEntityType, String modelType) {
        
        // For now, we only support SIMILARITY sorting with custom metrics
        // TODO: Add rating-based sorting for custom metrics
        if (sortBy != SortBy.SIMILARITY) {
            log.warn("Rating-based sorting not yet implemented for custom similarity metrics, using SIMILARITY sort");
        }
        
        // Convert model type to dimensions
        int dimensions = "fast".equals(modelType) ? 384 : 384;
        
        if (withEntityType) {
            return switch (similarityMetric) {
                case COSINE -> embeddingRepository.findSimilarByVectorMetricAndEntityTypeCosineWithModel(
                    queryVector, excludeUri, metricType, entityType, dimensions, topK);
                case EUCLIDEAN -> embeddingRepository.findSimilarByVectorAndMetricEuclideanWithModel(
                    queryVector, excludeUri, metricType, dimensions, topK); // Uses <-> operator
                case DOT_PRODUCT -> embeddingRepository.findSimilarByVectorAndMetricDotProductWithModel(
                    queryVector, excludeUri, metricType, dimensions, topK);
            };
        } else {
            return switch (similarityMetric) {
                case COSINE -> embeddingRepository.findSimilarByVectorAndMetricCosineWithModel(
                    queryVector, excludeUri, metricType, dimensions, topK);
                case EUCLIDEAN -> embeddingRepository.findSimilarByVectorAndMetricEuclideanWithModel(
                    queryVector, excludeUri, metricType, dimensions, topK); // Uses <-> operator
                case DOT_PRODUCT -> embeddingRepository.findSimilarByVectorAndMetricDotProductWithModel(
                    queryVector, excludeUri, metricType, dimensions, topK);
            };
        }
    }

    private List<SimilarityResult> findSimilarWithPgVector(String queryVector, String excludeUri, Integer orderId, String metricType, int topK, SortBy sortBy) {
        return findSimilarWithPgVector(queryVector, excludeUri, orderId, metricType, topK, sortBy, "fast"); // Default to fast model
    }
    
    private List<SimilarityResult> findSimilarWithPgVector(String queryVector, String excludeUri, Integer orderId, String metricType, int topK, SortBy sortBy, String modelType) {
        String queryEntityType;
        try {
            EntityType extractedType = EntityType.fromUri(excludeUri);
            queryEntityType = extractedType.getNormalizedName();
        } catch (IllegalArgumentException e) {
            log.warn("Cannot determine entity type from URI: {} - {}", excludeUri, e.getMessage());
            queryEntityType = "unknown";
        }
        log.info("Similarity search: {} similar to {} (order: {}, metric: {})", topK, queryEntityType, orderId, metricType);
        
        try {
            List<Object[]> results;
            if (metricType != null) {
                if (filterByEntityType) {
                    // Use entity type filtering - simplified to use dual model methods for all cases
                    // TODO: Add rating-based sorting support for dual models
                    int dimensions = "fast".equals(modelType) ? 384 : 384;
                    results = embeddingRepository.findSimilarByVectorMetricAndEntityTypeCosineWithModel(
                            queryVector, excludeUri, metricType, queryEntityType, dimensions, topK);
                    if (sortBy == SortBy.BEST_RATED || sortBy == SortBy.WORST_RATED) {
                        log.warn("Rating-based sorting not yet supported with dual models, using SIMILARITY sort");
                    }
                    switch (sortBy) {
                        case BEST_RATED:
                        case WORST_RATED:
                        case SIMILARITY:
                        default:
                            results = embeddingRepository.findSimilarByVectorMetricAndEntityTypeCosineWithModel(
                                    queryVector, excludeUri, metricType, queryEntityType, dimensions, topK);
                            break;
                    }
                } else {
                    // No entity type filtering (cross-type similarity) - use dual model methods
                    int dimensions = "fast".equals(modelType) ? 384 : 384;
                    results = embeddingRepository.findSimilarByVectorAndMetricCosineWithModel(queryVector, excludeUri, metricType, dimensions, topK);
                }
            } else {
                // Always search across ALL orders - orderId is only for fetching raw data
                // Use dual model approach with default "general" metric for no-metric queries
                log.warn("Generic similarity search without metric filter - using cosine similarity with {} model", modelType);
                int dimensions = "fast".equals(modelType) ? 384 : 384;
                results = embeddingRepository.findSimilarByVectorAndMetricCosineWithModel(queryVector, excludeUri, "general", dimensions, topK);
            }
            
            List<SimilarityResult> similarityResults = results.stream()
                .map(this::mapResultToSimilarityResult)
                .collect(Collectors.toList());
                
            if (!similarityResults.isEmpty()) {
                double minSim = similarityResults.stream().mapToDouble(SimilarityResult::similarity).min().orElse(0.0);
                double maxSim = similarityResults.stream().mapToDouble(SimilarityResult::similarity).max().orElse(0.0);
                log.info("Found {} results, similarity range: {:.3f} - {:.3f}", 
                        similarityResults.size(), minSim, maxSim);
            }
            
            return similarityResults;
                
        } catch (Exception e) {
            log.error("pgvector query failed: {}", e.getMessage());
            throw new RuntimeException("Vector similarity search failed - pgvector extension required", e);
        }
    }
    
    
    private List<SimilarityResult> findSimilarWithInMemoryCalculation(double[] queryVector, String excludeUri, Integer excludeOrderId, int topK) {
        List<EntityEmbedding> allEmbeddings = embeddingRepository.findAll();
        log.debug("Calculating similarities against {} stored embeddings", allEmbeddings.size());
        
        // Use parallel processing for similarity calculations
        ForkJoinPool customThreadPool = new ForkJoinPool(maxParallelThreads);
        try {
            List<CompletableFuture<SimilarityResult>> futures = allEmbeddings.stream()
                .filter(e -> !e.getEntityUri().equals(excludeUri))
                .filter(e -> excludeOrderId == null || !e.getOrderId().equals(excludeOrderId))
                .map(embedding -> CompletableFuture.supplyAsync(() -> {
                    double[] candidateVector = vectorStringToDoubleArray(embedding.getEmbedding()); // Get embedding regardless of model
                    double similarity = calculateCosineSimilarity(queryVector, candidateVector);
                    return new SimilarityResult(
                        embedding.getEntityUri(), 
                        embedding.getEntityType(), 
                        embedding.getMetricType(),
                        embedding.getOrderId(),
                        embedding.getRatingValue(),
                        similarity,
                        embedding.getCharacterLength()
                    );
                }, customThreadPool))
                .collect(Collectors.toList());
            
            // Collect results and apply threshold filtering
            List<SimilarityResult> results = futures.stream()
                .map(CompletableFuture::join)
                .filter(result -> result.similarity >= similarityThreshold)
                .sorted((a, b) -> Double.compare(b.similarity, a.similarity))
                .limit(Math.min(topK, maxCandidates))
                .collect(Collectors.toList());
                
            log.info("Found {} similar entities above threshold {}", results.size(), similarityThreshold);
            return results;
            
        } finally {
            customThreadPool.shutdown();
            try {
                if (!customThreadPool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    customThreadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                customThreadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    public List<SimilarityResult> findSimilarEntitiesForMetric(
            String queryEntityUri, 
            Integer queryOrderId, 
            List<String> expertRatedEntityUris, 
            int topK) {
        
        log.info("Finding {} similar entities to '{}' from {} expert-rated candidates", 
                topK, queryEntityUri, expertRatedEntityUris.size());
        
        // Get query embedding
        Optional<EntityEmbedding> queryEmbeddingOpt = embeddingRepository.findByEntityUriAndOrderId(queryEntityUri, queryOrderId);
        if (queryEmbeddingOpt.isEmpty()) {
            log.warn("Query entity '{}' not found in embeddings for order {}", queryEntityUri, queryOrderId);
            return List.of();
        }
        
        double[] queryVector = vectorStringToDoubleArray(queryEmbeddingOpt.get().getEmbedding()); // Get embedding regardless of model
        
        // Get embeddings for expert-rated entities only
        List<EntityEmbedding> candidateEmbeddings = embeddingRepository.findByEntityUriIn(expertRatedEntityUris);
        log.debug("Found embeddings for {} out of {} expert-rated entities", 
                candidateEmbeddings.size(), expertRatedEntityUris.size());
        
        // Calculate similarities in parallel
        ForkJoinPool customThreadPool = new ForkJoinPool(maxParallelThreads);
        try {
            List<SimilarityResult> results = candidateEmbeddings.parallelStream()
                .filter(e -> !e.getEntityUri().equals(queryEntityUri))
                .map(embedding -> {
                    double[] candidateVector = vectorStringToDoubleArray(embedding.getEmbedding()); // Get embedding regardless of model
                    double similarity = calculateCosineSimilarity(queryVector, candidateVector);
                    return new SimilarityResult(
                        embedding.getEntityUri(), 
                        embedding.getEntityType(), 
                        embedding.getMetricType(),
                        embedding.getOrderId(),
                        embedding.getRatingValue(),
                        similarity,
                        embedding.getCharacterLength()
                    );
                })
                .filter(result -> result.similarity >= similarityThreshold)
                .sorted((a, b) -> Double.compare(b.similarity, a.similarity))
                .limit(topK)
                .collect(Collectors.toList());
                
            log.info("Found {} similar expert-rated entities above threshold {}", results.size(), similarityThreshold);
            return results;
            
        } finally {
            customThreadPool.shutdown();
        }
    }
    
    private double calculateCosineSimilarity(double[] vectorA, double[] vectorB) {
        if (vectorA.length != vectorB.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensions");
        }
        
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        
        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            normA += vectorA[i] * vectorA[i];
            normB += vectorB[i] * vectorB[i];
        }
        
        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }
        
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }
    
    
    private byte[] floatsToBytes(double[] floats) {
        ByteBuffer buffer = ByteBuffer.allocate(floats.length * 4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (double f : floats) {
            buffer.putFloat((float) f);
        }
        return buffer.array();
    }
    
    private double[] bytesToFloats(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        double[] floats = new double[bytes.length / 4];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = buffer.getFloat();
        }
        return floats;
    }
    
    public Map<String, Object> getVectorStatistics() {
        long totalEmbeddings = embeddingRepository.count();
        List<String> entityTypes = embeddingRepository.findDistinctEntityTypes();
        List<String> metricTypes = embeddingRepository.findDistinctMetricTypes();
        
        Map<String, Long> typeDistribution = entityTypes.stream()
            .collect(Collectors.toMap(
                type -> type,
                type -> (long) embeddingRepository.findByEntityType(type).size()
            ));
            
        Map<String, Long> metricDistribution = metricTypes.stream()
            .collect(Collectors.toMap(
                type -> type != null ? type : "null",
                type -> (long) embeddingRepository.findByMetricType(type).size()
            ));
        
        return Map.of(
            "totalEmbeddings", totalEmbeddings,
            "entityTypes", entityTypes,
            "metricTypes", metricTypes,
            "typeDistribution", typeDistribution,
            "metricDistribution", metricDistribution,
            "similarityThreshold", similarityThreshold,
            "maxCandidates", maxCandidates,
            "batchSize", batchSize,
            "enablePgvector", enablePgvector
        );
    }
    
    @Transactional
    public void clearEmbeddings(Integer orderId) {
        log.info("Clearing embeddings for order {}", orderId);
        embeddingRepository.deleteByOrderId(orderId);
        log.info("Cleared embeddings for order {}", orderId);
    }
    
    @Transactional
    public void clearAllEmbeddings() {
        log.info("Clearing ALL embeddings from database");
        try {
            embeddingRepository.deleteAll();
            log.info("Cleared all embeddings from database");
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("relation \"entity_embeddings\" does not exist")) {
                log.warn("Entity embeddings table does not exist yet - no embeddings to clear");
                log.warn("The table will be created automatically when embeddings are first stored");
            } else if (e.getMessage() != null && e.getMessage().contains("relation \"public.entity_embeddings\" does not exist")) {
                log.warn("Entity embeddings table does not exist in public schema - no embeddings to clear");
                log.warn("The table will be created automatically when embeddings are first stored");
            } else {
                log.error("Error clearing embeddings: {}", e.getMessage());
                throw e;
            }
        }
    }
    
    
    /**
     * Store a single entity embedding with a specific model
     */
    public void storeEntityEmbeddingWithSpecificModel(
            String entityUri, Integer orderId, String entityType, String metricType, 
            Double ratingValue, String strategy, double[] embedding, EmbeddingModel model) {
        storeEntityEmbeddingWithSpecificModel(entityUri, orderId, entityType, metricType, 
                ratingValue, strategy, embedding, model, null);
    }
    
    /**
     * Store a single entity embedding with a specific model and character length
     */
    public void storeEntityEmbeddingWithSpecificModel(
            String entityUri, Integer orderId, String entityType, String metricType, 
            Double ratingValue, String strategy, double[] embedding, EmbeddingModel model, Integer characterLength) {
        
        String vectorString = doubleArrayToVectorString(embedding);
        
        // Normalize the entity type
        String normalizedEntityType;
        try {
            EntityType normalized = entityTypeService.normalizeEntityType(entityType);
            normalizedEntityType = normalized.getNormalizedName();
        } catch (IllegalArgumentException e) {
            log.error("Failed to normalize entity type '{}' for {}: {}", entityType, entityUri, e.getMessage());
            throw new RuntimeException("Invalid entity type: " + entityType, e);
        }
        
        String insertSQL = """
            INSERT INTO entity_embeddings (entity_uri, order_id, entity_type, metric_type, rating_value, strategy, 
                                         embedding, dimensions, model_name, character_length, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?::vector, ?, ?, ?, NOW())
            """;
            
        try {
            int updated = vectorJdbcTemplate.update(insertSQL,
                entityUri,                     // entity_uri
                orderId,                      // order_id
                normalizedEntityType,         // entity_type (normalized!)
                metricType,                   // metric_type
                ratingValue,                  // rating_value
                strategy,                     // strategy
                vectorString,                 // embedding
                model.getDimensions(),        // dimensions
                model.getModelId(),           // model_name
                characterLength               // character_length
            );
            
            log.debug("Stored {} model embedding for {} as type '{}' with {} dims", 
                model.getKey(), entityUri, normalizedEntityType, embedding.length);
                
        } catch (Exception e) {
            log.error("{} model embedding failed for {}: {}", model.getKey(), entityUri, e.getMessage());
            throw new RuntimeException("Specific model embedding insertion failed", e);
        }
    }
    
    /**
     * Convert double array to pgvector string format: [1.0,2.0,3.0]
     */
    private String doubleArrayToVectorString(double[] vector) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(vector[i]);
        }
        sb.append("]");
        return sb.toString();
    }
    
    /**
     * Convert pgvector string format to double array: [1.0,2.0,3.0] -> double[]
     */
    private double[] vectorStringToDoubleArray(String vectorString) {
        // Remove brackets and split by comma
        String content = vectorString.substring(1, vectorString.length() - 1);
        String[] parts = content.split(",");
        double[] result = new double[parts.length];
        
        for (int i = 0; i < parts.length; i++) {
            result[i] = Double.parseDouble(parts[i].trim());
        }
        
        return result;
    }
    
    /**
     * Map database query result to SimilarityResult object
     */
    private SimilarityResult mapResultToSimilarityResult(Object[] result) {
        // Handle both regular results (6 fields) and length-aware results (7 fields)
        if (result.length == 6) {
            // Regular query: entity_uri, entity_type, metric_type, order_id, rating_value, distance
            String entityUri = (String) result[0];
            String entityType = (String) result[1];
            String metricType = (String) result[2];
            Integer orderId = (Integer) result[3];
            Double ratingValue = result[4] != null ? ((Number) result[4]).doubleValue() : null;
            Double distance = (Double) result[5];
            
            // Convert distance to similarity (cosine distance to cosine similarity)
            double similarity = Math.max(0.0, 1.0 - distance);
            
            return new SimilarityResult(entityUri, entityType, metricType, orderId, ratingValue, similarity, null);
        } else if (result.length == 7) {
            // Length-aware query: entity_uri, entity_type, metric_type, order_id, rating_value, distance, character_length
            String entityUri = (String) result[0];
            String entityType = (String) result[1];
            String metricType = (String) result[2];
            Integer orderId = (Integer) result[3];
            Double ratingValue = result[4] != null ? ((Number) result[4]).doubleValue() : null;
            Double distance = (Double) result[5];
            Integer characterLength = result[6] != null ? ((Number) result[6]).intValue() : null;
            
            // Convert distance to similarity (cosine distance to cosine similarity)
            double similarity = Math.max(0.0, 1.0 - distance);
            
            return new SimilarityResult(entityUri, entityType, metricType, orderId, ratingValue, similarity, characterLength);
        } else {
            throw new IllegalArgumentException("Unexpected result array length: " + result.length);
        }
    }
    
    /**
     * Extract repository name from GitHub URI
     */
    private String extractRepoName(String entityUri) {
        if (entityUri != null && entityUri.contains("github.com")) {
            String[] parts = entityUri.split("/");
            if (parts.length >= 5) {
                return parts[3] + "/" + parts[4];
            }
        }
        return "unknown";
    }
    
    /**
     * Calculate similarity between two text strings using external embedding service
     * This method requires the embedding service to be properly configured
     */
    public double calculateSimilarity(String text1, String text2) {
        if (text1 == null || text2 == null) {
            throw new IllegalArgumentException("Text inputs cannot be null for similarity calculation");
        }
        
        if (text1.equals(text2)) {
            return 1.0;
        }
        
        log.info("Calculating embedding similarity between texts of length {} and {}", 
                text1.length(), text2.length());
        
        // For now, use a simple fallback similarity calculation to avoid circular dependency
        // This method should be called by services that already have embeddings
        log.warn("Direct text similarity calculation not supported to avoid circular dependency");
        log.warn("This method should be used with pre-generated embeddings from BatchEmbeddingService");
        
        // Simple fallback based on text length and common words for basic similarity
        if (text1.equals(text2)) {
            return 1.0;
        }
        
        // Very basic similarity based on shared words (fallback only)
        String[] words1 = text1.toLowerCase().split("\\s+");
        String[] words2 = text2.toLowerCase().split("\\s+");
        
        Set<String> set1 = Set.of(words1);
        Set<String> set2 = Set.of(words2);
        
        Set<String> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);
        
        Set<String> union = new HashSet<>(set1);
        union.addAll(set2);
        
        double jaccardSimilarity = union.isEmpty() ? 0.0 : (double) intersection.size() / union.size();
        
        log.debug("Calculated fallback Jaccard similarity: {:.4f} for texts: '{}...' and '{}...'", 
                 jaccardSimilarity,
                 text1.substring(0, Math.min(50, text1.length())),
                 text2.substring(0, Math.min(50, text2.length())));
        
        return jaccardSimilarity;
    }
    
    /**
     * NEW: Search with dimension filtering to prevent duplicates from different models
     */
    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVectorAndDimensions(
            String queryVector, String excludeUri, Integer orderId, String metricType, 
            String entityType, int topK, SortBy sortBy, Integer modelDimensions) {
        return findSimilarWithPgVectorFromQueryVectorAndDimensions(
            queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy, modelDimensions, null);
    }
    
    /**
     * Find similar entities with dimension filtering and custom threshold
     */
    /**
     * Length-aware similarity search with character length filtering
     */
    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVectorAndDimensions(
            String queryVector, String excludeUri, Integer orderId, String metricType, 
            String entityType, int topK, SortBy sortBy, Integer modelDimensions, Double customThreshold,
            Integer queryLength, Integer lengthTolerance) {
        
        log.info("Length-aware similarity search: queryLength={}, tolerance={}", queryLength, lengthTolerance);
        
        // If no length filtering requested, use existing method
        if (queryLength == null || lengthTolerance == null) {
            return findSimilarWithPgVectorFromQueryVectorAndDimensions(queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy, modelDimensions, customThreshold);
        }
        
        // Calculate length range
        int minLength = Math.max(0, queryLength - lengthTolerance);
        int maxLength = queryLength + lengthTolerance;
        
        log.info("Filtering by character length range: {} - {}", minLength, maxLength);
        
        try {
            String normalizedEntityType = entityTypeService.normalizeEntityType(entityType).getNormalizedName();
            List<Object[]> results = new ArrayList<>();
            
            // Get larger pool to allow for length filtering
            int expandedLimit = Math.max(topK * 3, 100);
            
            // Use length-aware repository methods
            if (entityType != null) {
                log.debug("Length-aware repository call: excludeUri={}, metricType={}, entityType={}, dimensions={}, lengthRange={}-{}, limit={}", 
                         excludeUri, metricType, normalizedEntityType, modelDimensions, minLength, maxLength, topK);
                switch (sortBy) {
                    case BEST_RATED -> {
                        log.info("Using BEST_RATED length-aware query with {} dimensions, length range: {}-{}", 
                                modelDimensions, minLength, maxLength);
                        results = embeddingRepository.findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingDescWithLength(
                                queryVector, excludeUri, metricType, normalizedEntityType, modelDimensions, minLength, maxLength, topK);
                    }
                    case WORST_RATED -> {
                        log.info("Using WORST_RATED length-aware query with {} dimensions, length range: {}-{}", 
                                modelDimensions, minLength, maxLength);
                        results = embeddingRepository.findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingAscWithLength(
                                queryVector, excludeUri, metricType, normalizedEntityType, modelDimensions, minLength, maxLength, topK);
                    }
                    case SIMILARITY -> {
                        log.info("Using SIMILARITY length-aware query with {} dimensions, length range: {}-{}", 
                                modelDimensions, minLength, maxLength);
                        results = embeddingRepository.findSimilarByVectorMetricAndEntityTypeCosineWithModelAndLength(
                                queryVector, excludeUri, metricType, normalizedEntityType, modelDimensions, minLength, maxLength, topK);
                    }
                }
                log.info("Length-aware repository returned {} results", results.size());
            }
            
            // Apply threshold filtering (length already filtered at DB level)
            double effectiveThreshold = customThreshold != null ? customThreshold : similarityThreshold;
            
            List<SimilarityResult> filteredResults = results.stream()
                .map(this::mapResultToSimilarityResult)
                .filter(result -> result.similarity >= effectiveThreshold)
                .sorted((a, b) -> Double.compare(b.similarity, a.similarity))
                .collect(Collectors.toList());
                
            log.info("Length-aware search returned {} results (range: {}-{}, threshold: {})", 
                    filteredResults.size(), minLength, maxLength, effectiveThreshold);
            return filteredResults;
            
        } catch (Exception e) {
            log.error("Error in length-aware similarity search: {}", e.getMessage());
            throw new RuntimeException("Length-aware similarity search failed", e);
        }
    }

    /**
     * Calculate dynamic length tolerance based on query length
     */
    public static int calculateLengthTolerance(int queryLength) {
        if (queryLength <= 50) {
            // Short queries (titles, one-liners): Fixed tolerance
            return 100;
        } else if (queryLength <= 200) {
            // Medium queries: Allow 2x variation
            return (int) (queryLength * 1.5);
        } else if (queryLength <= 1000) {
            // Long queries: Allow 50% variation  
            return (int) (queryLength * 0.5);
        } else {
            // Very long queries: Cap at reasonable maximum
            return Math.min(500, (int) (queryLength * 0.3));
        }
    }

    public List<SimilarityResult> findSimilarWithPgVectorFromQueryVectorAndDimensions(
            String queryVector, String excludeUri, Integer orderId, String metricType, 
            String entityType, int topK, SortBy sortBy, Integer modelDimensions, Double customThreshold) {
        
        if (modelDimensions == null) {
            // No dimension filtering - use existing method
            return findSimilarWithPgVectorFromQueryVector(queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy);
        }
        
        log.info("Searching similar entities with dimension filtering: {} dimensions, metric: {}, entityType: {}", 
                modelDimensions, metricType, entityType);
        
        try {
            String normalizedEntityType = entityTypeService.normalizeEntityType(entityType).getNormalizedName();
            List<Object[]> results = new ArrayList<>();
            
            // Use dimension-aware repository methods to prevent duplicates
            if (entityType != null) {
                log.debug("Repository call parameters: excludeUri={}, metricType={}, entityType={}, dimensions={}, topK={}", 
                         excludeUri, metricType, normalizedEntityType, modelDimensions, topK);
                switch (sortBy) {
                    case BEST_RATED -> {
                        log.info("Using BEST_RATED dimension-aware query with {} dimensions", modelDimensions);
                        results = embeddingRepository.findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingDesc(
                                queryVector, excludeUri, metricType, normalizedEntityType, modelDimensions, topK);
                    }
                    case WORST_RATED -> {
                        log.info("Using WORST_RATED dimension-aware query with {} dimensions", modelDimensions);
                        results = embeddingRepository.findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingAsc(
                                queryVector, excludeUri, metricType, normalizedEntityType, modelDimensions, topK);
                    }
                    case SIMILARITY -> {
                        log.info("Using SIMILARITY dimension-aware query with {} dimensions", modelDimensions);
                        results = embeddingRepository.findSimilarByVectorMetricAndEntityTypeCosineWithModel(
                                queryVector, excludeUri, metricType, normalizedEntityType, modelDimensions, topK);
                    }
                }
                log.info("Repository returned {} results with dimension filtering", results.size());
            } else {
                // If no entity type specified, we'd need to add more repository methods
                // For now, fall back to existing behavior
                log.warn("Entity type filtering required for dimension-aware search - falling back to default");
                return findSimilarWithPgVectorFromQueryVector(queryVector, excludeUri, orderId, metricType, entityType, topK, sortBy);
            }
            
            // Apply threshold filtering and sorting
            double effectiveThreshold = customThreshold != null ? customThreshold : similarityThreshold;
            log.info("Applying similarity threshold: {} (custom: {})", effectiveThreshold, customThreshold != null);
            
            List<SimilarityResult> filteredResults = results.stream()
                .map(this::mapResultToSimilarityResult)
                .filter(result -> result.similarity >= effectiveThreshold)
                .sorted((a, b) -> Double.compare(b.similarity, a.similarity))
                .collect(Collectors.toList());
                
            log.info("After threshold filtering: {} results (threshold: {})", filteredResults.size(), effectiveThreshold);
            return filteredResults;
            
        } catch (Exception e) {
            log.error("Error in dimension-aware similarity search: {}", e.getMessage());
            throw new RuntimeException("Dimension-aware similarity search failed", e);
        }
    }

}
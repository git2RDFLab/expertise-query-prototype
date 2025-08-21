package de.leipzig.htwk.gitrdf.expertise.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.model.EmbeddingModel;
import de.leipzig.htwk.gitrdf.expertise.model.EntityType;
import de.leipzig.htwk.gitrdf.expertise.service.data.MetricDefinitionService;
import de.leipzig.htwk.gitrdf.expertise.service.embedding.UnifiedEmbeddingService;
import de.leipzig.htwk.gitrdf.expertise.service.embedding.VectorSimilarityService;
import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for calculating similarity scores between entities based on expert ratings
 * and semantic analysis using order-based querying.
 * 
 * Uses lexical embeddings based on metric-relevant text content.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SimilarityService {

    private final SparqlQueryService sparqlQueryService;
    private final MetricDefinitionService metricDefinitionService;
    private final VectorSimilarityService vectorSimilarityService;
    private final UnifiedEmbeddingService unifiedEmbeddingService;
    private final EmbeddingModel embeddingModel;
    

    /**
     * Calculate similarity for entities in a specific order using metric-aware vector embeddings
     * 
     * @param orderId The order ID to query entities for
     * @param entityUri Optional specific entity URI to find similar entities for
     * @param metricName Optional specific metric to filter by
     * @param limit Maximum number of results to return
     * @return Map containing similarity results and metadata
     */
    public Map<String, Object> calculateSimilarity(Integer orderId, String entityUri, 
                                                  String metricName, int limit) {
        log.info("Calculating metric-aware similarity for order: {}, entity: {}, metric: {}, limit: {}", 
                orderId, entityUri, metricName, limit);

        if (entityUri == null) {
            return Map.of(
                "success", false,
                "message", "entityUri parameter is required for similarity search",
                "entities", List.of()
            );
        }

        try {
            // Try vector similarity service first (for stored embeddings)
            List<VectorSimilarityService.SimilarityResult> similarityResults = 
                vectorSimilarityService.findSimilarEntities(entityUri, orderId, metricName, limit);
                
            // If no results from stored embeddings, try on-the-fly generation
            if (similarityResults.isEmpty()) {
                log.info("No results from stored embeddings, trying on-the-fly embedding generation");
                
                // First, need to determine entity type - try to get entity data
                SparqlQueryService.EntityData queryEntity = sparqlQueryService.getEntityContent(orderId, entityUri);
                if (queryEntity != null) {
                    // Normalize entity type
                    EntityType normalizedType = EntityType.fromString(queryEntity.entityType);
                    String normalizedEntityType = normalizedType.getNormalizedName();
                    
                    // Use the new UnifiedEmbeddingService approach 
                    Map<String, Object> similarityData = calculateSimilarityWithLexicalEmbedding(orderId, entityUri, normalizedEntityType, metricName, limit);
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> similarities = (List<Map<String, Object>>) similarityData.get("similarities");
                    
                    if (similarities != null && !similarities.isEmpty()) {
                        // Convert to SimilarityResult format
                        similarityResults = similarities.stream()
                            .map(sim -> new VectorSimilarityService.SimilarityResult(
                                (String) sim.get("entityUri"),
                                (String) sim.get("entityType"),
                                (String) sim.get("metricType"),
                                (Integer) sim.get("orderId"),
                                ((Number) sim.get("ratingValue")).doubleValue(),
                                1.0 - ((Number) sim.get("distance")).doubleValue(), // Convert distance to similarity
                                (Integer) sim.get("characterLength")
                            ))
                            .collect(Collectors.toList());
                    }
                } else {
                    log.warn("Could not determine entity type for URI: {}", entityUri);
                }
            }
            
            if (similarityResults.isEmpty()) {
                return Map.of(
                    "success", false,
                    "message", String.format("No similar entities found for entity '%s' with metric '%s'", 
                            entityUri, metricName != null ? metricName : "any"),
                    "entities", List.of(),
                    "orderId", orderId,
                    "queryEntity", entityUri,
                    "metricFilter", metricName != null ? metricName : "all"
                );
            }

            // Convert to API response format
            List<Map<String, Object>> formattedResults = similarityResults.stream()
                .map(result -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("entityUri", result.entityUri());
                    map.put("entityType", result.entityType());
                    map.put("metricType", result.metricType() != null ? result.metricType() : "general");
                    map.put("orderId", result.orderId());
                    map.put("similarity", Math.round(result.similarity() * 1000.0) / 1000.0);
                    map.put("repositoryName", extractRepositoryName(result.entityUri()));
                    map.put("entityId", extractEntityId(result.entityUri()));
                    return map;
                })
                .collect(Collectors.toList());

            return Map.of(
                "success", true,
                "entities", formattedResults,
                "totalResults", similarityResults.size(),
                "queryEntity", entityUri,
                "orderId", orderId,
                "metricFilter", metricName != null ? metricName : "all",
                "message", String.format("Found %d similar entities", similarityResults.size())
            );
            
        } catch (Exception e) {
            log.error("Error calculating similarity for entity '{}' with metric '{}': {}", 
                    entityUri, metricName, e.getMessage());
            return Map.of(
                "success", false,
                "message", "Error calculating similarity: " + e.getMessage(),
                "entities", List.of(),
                "queryEntity", entityUri,
                "orderId", orderId
            );
        }
    }
    
    /**
     * Generate lexical embedding for a single entity
     */
    private double[] generateLexicalEmbeddingForEntity(SparqlQueryService.EntityData entity, Integer orderId) {
        try {
            log.info("Generating lexical embedding with {} model for {}", 
                    embeddingModel.getKey(), entity.entityUri);
            return unifiedEmbeddingService.generateEmbeddingWithModel(entity, 
                    embeddingModel, orderId);
        } catch (Exception e) {
            log.error("Error generating lexical embedding for entity '{}': {}", 
                     entity.entityUri, e.getMessage());
            return null;
        }
    }
    
    /**
     * Generate lexical embedding with model specification
     */
    private double[] generateLexicalEmbeddingForEntity(SparqlQueryService.EntityData entity, Integer orderId, Integer modelDimensions) {
        try {
            // Model dimensions are required - no defaults
            if (modelDimensions == null) {
                throw new IllegalArgumentException("Model dimensions must be specified (384 for fast, 768 for quality)");
            }
            
            log.info("Generating lexical embedding with {} model ({} dimensions) for {}", 
                    embeddingModel.getKey(), modelDimensions, entity.entityUri);
            return unifiedEmbeddingService.generateEmbeddingWithModel(entity, 
                    embeddingModel, orderId);
            
        } catch (Exception e) {
            log.error("Error generating lexical embedding for entity '{}': {}", 
                     entity.entityUri, e.getMessage());
            return null;
        }
    }
    
    
    
    /**
     * Extract repository name from GitHub URI
     */
    private String extractRepositoryName(String entityUri) {
        if (entityUri != null && entityUri.contains("github.com")) {
            String[] parts = entityUri.split("/");
            if (parts.length >= 5) {
                return parts[3] + "/" + parts[4];
            }
        }
        return "unknown";
    }
    
    /**
     * Extract entity ID from GitHub URI
     */
    private String extractEntityId(String entityUri) {
        if (entityUri.contains("/issues/")) {
            return "#" + entityUri.substring(entityUri.lastIndexOf("/") + 1);
        } else if (entityUri.contains("/pull/")) {
            return "PR#" + entityUri.substring(entityUri.lastIndexOf("/") + 1);
        } else if (entityUri.contains("/commit/")) {
            String hash = entityUri.substring(entityUri.lastIndexOf("/") + 1);
            return hash.length() > 7 ? hash.substring(0, 7) : hash;
        }
        return entityUri.substring(entityUri.lastIndexOf("/") + 1);
    }


    /**
     * Calculate semantic similarity between two entities based on their content
     */
    private double calculateSemanticSimilarity(SparqlQueryService.EntityData entity1, 
                                              SparqlQueryService.EntityData entity2) {
        String content1 = entity1.buildSemanticContext();
        String content2 = entity2.buildSemanticContext();
        
        // Use vector similarity service for semantic comparison - NO FALLBACK
        log.info("Calculating semantic similarity using external embedding service");
        return vectorSimilarityService.calculateSimilarity(content1, content2);
    }

    /**
     * Calculate structural similarity based on entity types and characteristics
     */
    private double calculateStructuralSimilarity(SparqlQueryService.EntityData entity1, 
                                                SparqlQueryService.EntityData entity2) {
        // Same entity type gets higher similarity
        if (entity1.entityType.equals(entity2.entityType)) {
            return 0.8; // High structural similarity for same type
        }
        
        // Related types get medium similarity
        Set<String> relatedTypes = Set.of("issue", "pull_request"); // Issues and PRs are related
        if (relatedTypes.contains(entity1.entityType) && relatedTypes.contains(entity2.entityType)) {
            return 0.5; // Medium similarity for related types
        }
        
        return 0.1; // Low similarity for different types
    }

    /**
     * Calculate metric-based similarity using metric definitions
     */
    private double calculateMetricBasedSimilarity(SparqlQueryService.EntityData entity1,
                                                 SparqlQueryService.EntityData entity2,
                                                 String metricName, Integer orderId) {
        MetricDefinitionService.MetricDefinition definition = 
            metricDefinitionService.getMetricDefinition(metricName);
        
        if (definition == null) {
            log.warn("No definition found for metric: {}", metricName);
            return 0.0;
        }
        
        // Calculate similarity based on the fields used by this metric
        List<String> fields = definition.getFields();
        double totalSimilarity = 0.0;
        int validComparisons = 0;
        
        for (String field : fields) {
            String value1 = getFieldValue(entity1, field);
            String value2 = getFieldValue(entity2, field);
            
            if (value1 != null && value2 != null) {
                log.info("Calculating field similarity for metric {} field {}", metricName, field);
                double fieldSimilarity = vectorSimilarityService.calculateSimilarity(value1, value2);
                totalSimilarity += fieldSimilarity;
                validComparisons++;
            }
        }
        
        return validComparisons > 0 ? totalSimilarity / validComparisons : 0.0;
    }

    /**
     * Extract field value from entity data
     */
    private String getFieldValue(SparqlQueryService.EntityData entity, String field) {
        return switch (field.toLowerCase()) {
            case "message", "commitmessage" -> entity.message;
            case "title", "issuetitle" -> entity.title;
            case "body", "issuebody" -> entity.body;
            case "commentbody" -> entity.commentBody;
            default -> null;
        };
    }

    /**
     * Calculate overall similarity as weighted combination
     */
    private double calculateOverallSimilarity(double semanticSimilarity, 
                                            double structuralSimilarity,
                                            Map<String, Double> metricSimilarities) {
        // Weights for different similarity components
        double semanticWeight = 0.4;
        double structuralWeight = 0.2;
        double metricWeight = 0.4;
        
        double avgMetricSimilarity = metricSimilarities.values().stream()
            .mapToDouble(Double::doubleValue)
            .average()
            .orElse(0.0);
        
        return (semanticSimilarity * semanticWeight) + 
               (structuralSimilarity * structuralWeight) + 
               (avgMetricSimilarity * metricWeight);
    }


    /**
     * Get all available metrics for a specific order from stored embeddings
     */
    public List<String> getAvailableMetrics(Integer orderId) {
        log.info("Getting available metrics from stored embeddings for order {}", orderId);
        
        try {
            // Get distinct metric types from stored embeddings for this order
            List<String> distinctMetrics = vectorSimilarityService.getVectorStatistics()
                .entrySet().stream()
                .filter(entry -> "metricTypes".equals(entry.getKey()))
                .map(entry -> (List<String>) entry.getValue())
                .findFirst()
                .orElse(List.of());
            
            // Filter out null values and sort
            List<String> availableMetrics = distinctMetrics.stream()
                .filter(Objects::nonNull)
                .sorted()
                .collect(Collectors.toList());
                
            log.info("Found {} available metrics: {}", availableMetrics.size(), availableMetrics);
            return availableMetrics;
            
        } catch (Exception e) {
            log.error("Error getting available metrics for order {}: {}", orderId, e.getMessage());
            // Fallback to metric definition service
            return metricDefinitionService.getExportableMetrics();
        }
    }

    /**
     * Get all entities with expert ratings for a specific order with their metric information
     */
    public List<Map<String, Object>> getExpertRatedEntities(Integer orderId) {
        log.info("Getting expert-rated entities with metric information for order {}", orderId);
        
        Map<String, SparqlQueryService.EntityData> entities = 
            sparqlQueryService.getExpertRatedEntitiesWithContent(orderId);
        
        List<Map<String, Object>> result = new ArrayList<>();
        for (SparqlQueryService.EntityData entity : entities.values()) {
            Map<String, Object> entityMap = new HashMap<>();
            entityMap.put("uri", entity.entityUri);
            entityMap.put("type", entity.entityType);
            entityMap.put("title", entity.title != null ? entity.title : "");
            entityMap.put("hasMessage", entity.message != null);
            entityMap.put("hasBody", entity.body != null);
            entityMap.put("hasCommentBody", entity.commentBody != null);
            entityMap.put("metricTypes", new ArrayList<>(entity.getMetricIds())); // Include metric types
            entityMap.put("repositoryName", extractRepositoryName(entity.entityUri));
            entityMap.put("entityId", extractEntityId(entity.entityUri));
            result.add(entityMap);
        }
        
        log.info("Retrieved {} entities with metric information", result.size());
        return result;
    }

    /**
     * Calculate similarity using lexical embeddings
     */
    public Map<String, Object> calculateSimilarityWithLexicalEmbedding(
            Integer orderId, 
            String queryEntityUri, 
            String entityType,
            String metricName,
            int limit) {
        return calculateSimilarityWithLexicalEmbedding(orderId, queryEntityUri, entityType, metricName, limit, "best");
    }
    
    /**
     * Calculate similarity with scale-based sorting using lexical embeddings
     */
    public Map<String, Object> calculateSimilarityWithLexicalEmbedding(
            Integer orderId, 
            String queryEntityUri, 
            String entityType,
            String metricName,
            int limit,
            String scaleType) {
        
        log.info("Calculating vector similarity for order {} using lexical embeddings with {} scale", orderId, scaleType != null ? scaleType : "best");
        
        try {
            // 1. Get query entity data using buildSingleEntityQuery
            if (queryEntityUri == null || entityType == null) {
                return Map.of(
                    "error", "Both queryEntityUri and entityType are required",
                    "similarities", List.of(),
                    "totalStoredVectors", 0
                );
            }
            
            SparqlQueryService.EntityData queryEntity = sparqlQueryService.getSingleEntityContent(orderId, queryEntityUri, entityType);
            if (queryEntity == null) {
                return Map.of(
                    "error", "Query entity not found: " + queryEntityUri + " (type: " + entityType + ")",
                    "similarities", List.of(),
                    "totalStoredVectors", 0
                );
            }
            
            // 2. Generate embedding for query entity on-the-fly using lexical strategy
            double[] queryEmbedding = generateLexicalEmbeddingForEntity(queryEntity, orderId);
            if (queryEmbedding == null) {
                return Map.of(
                    "error", "Failed to generate embedding for query entity",
                    "similarities", List.of(),
                    "totalStoredVectors", 0
                );
            }
            
            // 3. Convert embedding to vector string for pgvector comparison
            String queryVectorString = vectorSimilarityService.convertDoubleArrayToVectorString(queryEmbedding);
            log.info("Query vector string: {}", queryVectorString.length() > 50 ? queryVectorString.substring(0, 50) + "..." : queryVectorString);
            
            // 4. Handle different scale types with appropriate search strategy
            List<VectorSimilarityService.SimilarityResult> vectorResults;
            
            if ("center".equals(scaleType)) {
                // For center, make two separate searches
                vectorResults = performCenterSearch(queryEntityUri, orderId, metricName, entityType, queryVectorString, limit);
            } else if ("randomcenter".equalsIgnoreCase(scaleType)) {
                // For randomCenter, perform random selection from rating-based parts
                vectorResults = performRandomCenterSearch(queryEntityUri, orderId, metricName, entityType, queryVectorString, limit);
            } else {
                // For best/worst, single search with appropriate sorting
                VectorSimilarityService.SortBy sortBy = determineSortBy(scaleType);
                
                // Use generated embedding to find similar entities (not the query entity itself!)
                log.info("Searching for entities similar to generated embedding (sort: {})", sortBy);
                
                // Search across ALL orders - no fallbacks
                vectorResults = vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                    queryVectorString, queryEntityUri, null, metricName, entityType, limit, sortBy);
            }
            
            // 5. Convert results to response format
            List<Map<String, Object>> similarities = vectorResults.stream()
                .map(result -> {
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("entityUri", result.entityUri());
                    resultMap.put("entityType", result.entityType());
                    resultMap.put("metricType", result.metricType());
                    resultMap.put("orderId", result.orderId());
                    resultMap.put("ratingValue", result.ratingValue());
                    resultMap.put("similarity", Math.round(result.similarity() * 1000.0) / 1000.0);
                    resultMap.put("repositoryName", extractRepositoryName(result.entityUri()));
                    resultMap.put("entityId", extractEntityId(result.entityUri()));
                    return resultMap;
                })
                .collect(Collectors.toList());
            
            return Map.of(
                "queryEntity", queryEntityUri,
                "queryEntityType", entityType,
                "metricName", metricName != null ? metricName : "all_metrics",
                "scaleType", scaleType != null ? scaleType : "best",
                "totalStoredVectors", vectorResults.size(), // This is from actual vector DB query
                "similarities", similarities,
                "resultCount", similarities.size()
            );
            
        } catch (Exception e) {
            log.error("Failed to calculate vector similarity: {}", e.getMessage());
            return Map.of(
                "error", e.getMessage(),
                "similarities", List.of(),
                "totalStoredVectors", 0
            );
        }
    }
    
    /**
     * Calculate similarity with custom similarity threshold using lexical embeddings
     */
    public Map<String, Object> calculateSimilarityWithLexicalEmbedding(
            Integer orderId, 
            String queryEntityUri, 
            String entityType,
            String metricName,
            int limit,
            String scaleType,
            Double similarityThreshold) {
        
        // Pass the threshold through to the enhanced options method
        if (similarityThreshold != null) {
            log.info("Using custom similarity threshold: {}", similarityThreshold);
        }
        
        // Use enhanced options method that supports threshold passing
        return calculateSimilarityWithEnhancedOptions(
            orderId, queryEntityUri, entityType, 
            metricName != null ? List.of(metricName) : List.of(),
            limit, scaleType, similarityThreshold, null, null, null);
    }
    
    /**
     * ENHANCED: Calculate similarity with multiple metrics and custom similarity metrics
     * Supports multi-metric filtering and alternative similarity metrics
     */
    public Map<String, Object> calculateSimilarityWithEnhancedOptions(
            Integer orderId, 
            String queryEntityUri, 
            String entityType,
            java.util.List<String> metricNames,  // Multiple metrics supported
            int limit,
            String scaleType,
            Double similarityThreshold,
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric,
            String fallbackStrategy,
            Integer modelDimensions) {  // Filter by embedding dimensions (384=fast, 768=quality)
        
        log.info("🔍 Enhanced similarity calculation - scaleType: '{}', metrics: {}, similarity metric: {}, fallback: {}", 
                scaleType, metricNames, similarityMetric, fallbackStrategy);
        
        // Handle case where no entityUri is provided (rating-based search only)
        if (queryEntityUri == null || queryEntityUri.trim().isEmpty()) {
            log.info("No entityUri provided - performing rating-based search for entityType: {}", entityType);
            return performRatingBasedSearch(orderId, entityType, metricNames, limit, scaleType != null ? scaleType : "best", similarityMetric);
        }
        
        // Handle randomCenter as pure rating-based search (ignore similarity entirely)
        // IMPORTANT: This must come BEFORE any optimization paths to ensure randomCenter always works
        if ("randomcenter".equalsIgnoreCase(scaleType)) {
            log.info("RandomCenter scaleType detected - performing pure rating-based search (ignoring entityUri for similarity)");
            return performRatingBasedSearch(orderId, entityType, metricNames, limit, "randomcenter", similarityMetric);
        }
        
        // For backward compatibility, if metricNames is null/empty but we need to search,
        // perform similarity search without metric filtering (search all metrics)
        if (metricNames == null || metricNames.isEmpty()) {
            log.info("No metric filtering - searching across all stored embeddings");
            Map<String, Object> results = calculateSimilarityWithLexicalEmbedding(orderId, queryEntityUri, entityType, null, limit, scaleType, similarityThreshold);
            
            // Apply fallback strategy if no results found
            if (fallbackStrategy != null && isEmptyResults(results)) {
                log.info("No similarity results found, applying fallback strategy: {}", fallbackStrategy);
                return performRatingBasedSearch(orderId, entityType, metricNames, limit, fallbackStrategy, similarityMetric);
            }
            return results;
        }
        
        // Optimize for single metric case - use direct method instead of multi-metric logic
        if (metricNames.size() == 1) {
            log.info("Single metric search optimization: using direct search for metric: {} with dimensions: {}", metricNames.get(0), modelDimensions);
            Map<String, Object> results = calculateSimilarityWithLexicalEmbeddingAndMetric(
                orderId, queryEntityUri, entityType, metricNames.get(0), limit, scaleType, similarityThreshold, similarityMetric, modelDimensions);
            
            // Apply fallback strategy if no results found
            if (fallbackStrategy != null && isEmptyResults(results)) {
                log.info("No similarity results found for single metric, applying fallback strategy: {}", fallbackStrategy);
                return performRatingBasedSearch(orderId, entityType, metricNames, limit, fallbackStrategy, similarityMetric);
            }
            return results;
        }
        
        // For multiple metrics, we'll combine results from each metric search with deduplication
        java.util.Map<String, Object> combinedResults = new java.util.HashMap<>();
        java.util.Map<String, java.util.Map<String, Object>> uniqueSimilarities = new java.util.LinkedHashMap<>(); // Use LinkedHashMap to preserve order
        int totalStoredVectors = 0;
        
        for (String metricName : metricNames) {
            log.info("Searching for similarities in metric: {} using {}", metricName, similarityMetric);
            
            // Calculate similarity for this specific metric with custom similarity metric
            Map<String, Object> metricResults = calculateSimilarityWithLexicalEmbeddingAndMetric(
                orderId, queryEntityUri, entityType, metricName, limit, scaleType, similarityThreshold, similarityMetric, modelDimensions);
            
            // Extract similarities and add metric context
            @SuppressWarnings("unchecked")
            java.util.List<java.util.Map<String, Object>> metricSimilarities = 
                (java.util.List<java.util.Map<String, Object>>) metricResults.get("similarities");
            
            if (metricSimilarities != null && !metricSimilarities.isEmpty()) {
                // Deduplicate by entityUri, keeping the best similarity score
                for (java.util.Map<String, Object> similarity : metricSimilarities) {
                    String entityUri = (String) similarity.get("entityUri");
                    similarity.put("sourceMetric", metricName);
                    
                    // If we already have this entity, keep the one with higher similarity
                    java.util.Map<String, Object> existing = uniqueSimilarities.get(entityUri);
                    if (existing == null || (Double) similarity.get("similarity") > (Double) existing.get("similarity")) {
                        uniqueSimilarities.put(entityUri, similarity);
                    }
                }
            }
            
            // Accumulate total stored vectors
            Object storedCount = metricResults.get("totalStoredVectors");
            if (storedCount instanceof Integer) {
                totalStoredVectors += (Integer) storedCount;
            }
        }
        
        // Convert to list for sorting and apply scale-aware sorting
        java.util.List<java.util.Map<String, Object>> allSimilarities = new java.util.ArrayList<>(uniqueSimilarities.values());
        
        // Apply scale-specific sorting and limiting
        if ("center".equals(scaleType)) {
            // For center scale, we want a balanced mix of good and bad examples
            allSimilarities.sort((a, b) -> {
                Double ratingA = (Double) a.get("ratingValue");
                Double ratingB = (Double) b.get("ratingValue");
                return Double.compare(ratingB, ratingA); // Sort by rating (descending)
            });
            
            // Take balanced selection: half from top (good) and half from bottom (bad)
            java.util.List<java.util.Map<String, Object>> centerResults = new java.util.ArrayList<>();
            int halfLimit = Math.max(1, limit / 2);
            
            // Take best rated (first half)
            int bestCount = Math.min(halfLimit, allSimilarities.size());
            centerResults.addAll(allSimilarities.subList(0, bestCount));
            
            // Take worst rated (last half)
            int worstCount = Math.min(limit - bestCount, allSimilarities.size() - bestCount);
            if (worstCount > 0 && allSimilarities.size() > bestCount) {
                int startIdx = Math.max(bestCount, allSimilarities.size() - worstCount);
                centerResults.addAll(allSimilarities.subList(startIdx, allSimilarities.size()));
            }
            
            allSimilarities = centerResults;
            log.info("Applied center scale selection: {} best + {} worst = {} total results", 
                    bestCount, worstCount, allSimilarities.size());
        } else if ("randomcenter".equalsIgnoreCase(scaleType)) {
            // For randomCenter, perform random selection from rating-based parts (best/undecided/worst)
            allSimilarities = performRandomCenterSelection(allSimilarities, limit);
        } else {
            // For best/worst scale, sort by similarity score and apply limit
            allSimilarities.sort((a, b) -> {
                Double simA = (Double) a.get("similarity");
                Double simB = (Double) b.get("similarity");
                return Double.compare(simB, simA); // Descending order for best similarity
            });
            
            // Apply limit to combined results
            if (allSimilarities.size() > limit) {
                allSimilarities = allSimilarities.subList(0, limit);
            }
        }
        
        // Build combined response
        combinedResults.put("similarities", allSimilarities);
        combinedResults.put("strategy", "lexical");
        combinedResults.put("totalStoredVectors", totalStoredVectors);
        combinedResults.put("searchedMetrics", metricNames);
        combinedResults.put("similarityMetric", similarityMetric != null ? similarityMetric.getValue() : "cosine");
        
        // Apply fallback strategy if no results found and fallbackStrategy is specified
        if (fallbackStrategy != null && allSimilarities.isEmpty()) {
            log.info("No similarity results found across {} metrics, applying fallback strategy: {}", metricNames.size(), fallbackStrategy);
            return performRatingBasedSearch(orderId, entityType, metricNames, limit, fallbackStrategy, similarityMetric);
        }
        
        log.info("Enhanced similarity search completed: {} results from {} metrics", 
                allSimilarities.size(), metricNames.size());
        
        return combinedResults;
    }
    
    /**
     * Check if similarity results are empty
     */
    private boolean isEmptyResults(Map<String, Object> results) {
        if (results == null) return true;
        
        @SuppressWarnings("unchecked")
        java.util.List<java.util.Map<String, Object>> similarities = 
            (java.util.List<java.util.Map<String, Object>>) results.get("similarities");
        
        return similarities == null || similarities.isEmpty();
    }
    
    /**
     * Perform rating-based search when no entityUri is provided or as fallback
     * Returns entities sorted by rating values according to the specified strategy
     */
    private Map<String, Object> performRatingBasedSearch(
            Integer orderId, String entityType, java.util.List<String> metricNames, 
            int limit, String ratingStrategy, 
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric) {
        
        log.info("Performing rating-based search: entityType={}, metrics={}, strategy={}", 
                entityType, metricNames, ratingStrategy);
        
        try {
            // Get all expert-rated entities for the specified order and entity type
            Map<String, SparqlQueryService.EntityData> allEntities = 
                sparqlQueryService.getExpertRatedEntitiesWithContent(orderId);
            
            log.info(" Retrieved {} expert-rated entities total across all types", allEntities.size());
            
            // Debug: Show entity type breakdown
            Map<String, Long> entityTypeCounts = allEntities.values().stream()
                .collect(Collectors.groupingBy(entity -> entity.entityType, Collectors.counting()));
            log.info("🔍 Entity type breakdown: {}", entityTypeCounts);
            
            // Debug: Show available metrics for the requested entity type
            Set<String> availableMetricsForEntityType = allEntities.values().stream()
                .filter(entity -> entityType.equals(entity.entityType))
                .flatMap(entity -> entity.getMetricIds().stream())
                .collect(Collectors.toSet());
            log.info("🔍 Available metrics for entityType '{}': {}", entityType, availableMetricsForEntityType);
            
            // Filter by entity type and metrics
            java.util.List<java.util.Map<String, Object>> filteredResults = new java.util.ArrayList<>();
            
            for (SparqlQueryService.EntityData entity : allEntities.values()) {
                // Debug: Log first few entities to see their actual entityType values
                if (filteredResults.size() < 3) {
                    log.info("🔍 Sample entity: entityType='{}', URI='{}', metrics={}", 
                            entity.entityType, entity.entityUri, entity.getMetricIds());
                }
                
                // Skip if entity type doesn't match (handle both normalized and raw formats)
                boolean entityTypeMatches = entityType.equals(entity.entityType) || 
                    isEntityTypeMatch(entityType, entity.entityType);
                if (!entityTypeMatches) {
                    continue;
                }
                
                // Filter by metrics if specified
                if (metricNames != null && !metricNames.isEmpty()) {
                    boolean hasMatchingMetric = false;
                    for (String metricName : metricNames) {
                        if (entity.getMetricIds().contains(metricName)) {
                            hasMatchingMetric = true;
                            break;
                        }
                    }
                    if (!hasMatchingMetric) {
                        continue;
                    }
                }
                
                // Get rating values for this entity
                Set<String> entityMetrics = entity.getMetricIds();
                if (metricNames != null && !metricNames.isEmpty()) {
                    entityMetrics = entityMetrics.stream()
                        .filter(metricNames::contains)
                        .collect(Collectors.toSet());
                }
                
                // Create result entry for each metric
                for (String metricId : entityMetrics) {
                    Double rating = entity.getRatingForMetric(metricId);
                    if (rating != null) {
                        Map<String, Object> resultMap = new HashMap<>();
                        resultMap.put("entityUri", entity.entityUri);
                        resultMap.put("entityType", entity.entityType);
                        resultMap.put("metricType", metricId);
                        resultMap.put("orderId", orderId);
                        resultMap.put("ratingValue", rating);
                        resultMap.put("similarity", null); // No similarity calculation for rating-based search
                        resultMap.put("repositoryName", extractRepositoryName(entity.entityUri));
                        resultMap.put("entityId", extractEntityId(entity.entityUri));
                        resultMap.put("searchType", "rating-based");
                        filteredResults.add(resultMap);
                    }
                }
            }
            
            log.info("Rating-based search found {} entities for entityType: {}, metrics: {}", 
                    filteredResults.size(), entityType, metricNames);
            
            // Debug: Show rating distribution in the filtered results
            if (!filteredResults.isEmpty()) {
                Map<Double, Long> ratingCounts = filteredResults.stream()
                    .collect(Collectors.groupingBy(r -> (Double) r.get("ratingValue"), Collectors.counting()));
                log.info("Rating distribution: {}", ratingCounts);
            }
            
            // Sort by rating strategy
            switch (ratingStrategy.toLowerCase()) {
                case "best" -> filteredResults.sort((a, b) -> 
                    Double.compare((Double) b.get("ratingValue"), (Double) a.get("ratingValue")));
                case "worst" -> filteredResults.sort((a, b) -> 
                    Double.compare((Double) a.get("ratingValue"), (Double) b.get("ratingValue")));
                case "center" -> {
                    // 3-way diverse sampling: best, median, worst ratings
                    filteredResults.sort((a, b) -> 
                        Double.compare((Double) b.get("ratingValue"), (Double) a.get("ratingValue")));
                    
                    List<Map<String, Object>> diverseResults = new ArrayList<>();
                    int totalSize = filteredResults.size();
                    
                    if (totalSize <= limit) {
                        // If we have fewer results than requested, return all
                        diverseResults = filteredResults;
                    } else {
                        // Three-way split: best, median, worst
                        int bestCount = limit / 3;
                        int medianCount = limit / 3;
                        int worstCount = limit - bestCount - medianCount; // Handle remainder
                        
                        // Add best ratings (top of the list)
                        for (int i = 0; i < Math.min(bestCount, totalSize); i++) {
                            diverseResults.add(filteredResults.get(i));
                        }
                        
                        // Add median ratings (middle range, with some randomization to avoid always same entities)
                        int medianStart = Math.max(bestCount, totalSize / 3);
                        int medianEnd = Math.min(totalSize * 2 / 3, totalSize);
                        List<Map<String, Object>> medianPool = filteredResults.subList(medianStart, medianEnd);
                        
                        // Shuffle median pool to ensure variety across requests
                        List<Map<String, Object>> shuffledMedian = new ArrayList<>(medianPool);
                        Collections.shuffle(shuffledMedian);
                        
                        for (int i = 0; i < Math.min(medianCount, shuffledMedian.size()); i++) {
                            diverseResults.add(shuffledMedian.get(i));
                        }
                        
                        // Add worst ratings (bottom of the list, also with some randomization)
                        int worstStart = Math.max(medianEnd, totalSize - worstCount * 2);
                        List<Map<String, Object>> worstPool = filteredResults.subList(worstStart, totalSize);
                        
                        // Shuffle worst pool to ensure variety
                        List<Map<String, Object>> shuffledWorst = new ArrayList<>(worstPool);
                        Collections.shuffle(shuffledWorst);
                        
                        for (int i = 0; i < Math.min(worstCount, shuffledWorst.size()); i++) {
                            diverseResults.add(shuffledWorst.get(i));
                        }
                    }
                    
                    filteredResults = diverseResults;
                    log.info("Applied 3-way diverse center sampling: {} results from {} total available", 
                            filteredResults.size(), totalSize);
                }
                case "randomcenter" -> {
                    // Perform randomCenter selection from rating-based parts (best/undecided/worst)
                    List<Map<String, Object>> randomCenterResults = performRandomCenterSelectionFromRatings(filteredResults, limit);
                    filteredResults = randomCenterResults;
                    log.info("Applied randomCenter selection: {} results from {} total available", 
                            filteredResults.size(), filteredResults.size());
                }
            }
            
            // Apply limit
            if (filteredResults.size() > limit) {
                filteredResults = new ArrayList<>(filteredResults.subList(0, limit));
            }
            
            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("similarities", filteredResults);
            response.put("strategy", "lexical");
            response.put("totalStoredVectors", filteredResults.size());
            response.put("searchedMetrics", metricNames);
            response.put("similarityMetric", similarityMetric.getValue());
            response.put("searchType", "rating-based");
            response.put("fallbackStrategy", ratingStrategy);
            
            log.info("Rating-based search completed: {} results using {} strategy", 
                    filteredResults.size(), ratingStrategy);
            
            return response;
            
        } catch (Exception e) {
            log.error("Rating-based search failed: {}", e.getMessage());
            throw new RuntimeException("Rating-based search failed", e);
        }
    }
    
    /**
     * Calculate similarity with custom similarity metric using lexical embeddings (internal method)
     */
    private Map<String, Object> calculateSimilarityWithLexicalEmbeddingAndMetric(
            Integer orderId, String queryEntityUri, String entityType, String metricName,
            int limit, String scaleType, Double similarityThreshold,
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric, Integer modelDimensions) {
        
        log.info("🔍 Single-metric method - scaleType: '{}', metric: {}, entity: {}", scaleType, metricName, queryEntityUri);
        
        // Handle randomCenter as pure rating-based search (bypass all similarity logic)
        if ("randomcenter".equalsIgnoreCase(scaleType)) {
            log.info("RandomCenter scaleType detected in single-metric method - performing pure rating-based search");
            return performRatingBasedSearch(orderId, entityType, List.of(metricName), limit, "randomcenter", similarityMetric);
        }
        
        // Generate embedding for query entity
        SparqlQueryService.EntityData queryEntity = sparqlQueryService.getSingleEntityContent(orderId, queryEntityUri, entityType);
        if (queryEntity == null) {
            log.error("Query entity not found: {}", queryEntityUri);
            throw new RuntimeException("Query entity not found: " + queryEntityUri);
        }
        
        // STREAMLINED: Generate lexical embedding with correct model from the start
        double[] queryEmbedding = generateLexicalEmbeddingForEntity(queryEntity, orderId, modelDimensions);
        if (queryEmbedding == null) {
            log.error("Failed to generate embedding for query entity");
            throw new RuntimeException("Failed to generate embedding for query entity");
        }
        
        String queryVectorString = vectorSimilarityService.convertDoubleArrayToVectorString(queryEmbedding);
        
        // Search with custom similarity metric
        VectorSimilarityService.SortBy sortBy = determineSortBy(scaleType);
        List<VectorSimilarityService.SimilarityResult> vectorResults;
        
        if ("center".equals(scaleType)) {
            // For center, use combined search but with custom similarity metric and length awareness
            vectorResults = performCenterSearchWithMetric(queryEntityUri, orderId, metricName, entityType, queryVectorString, limit, similarityMetric, modelDimensions, queryEntity);
        } else {
            // Use enhanced method with similarity metric and dimension filtering
            if (modelDimensions != null) {
                vectorResults = vectorSimilarityService.findSimilarWithPgVectorFromQueryVectorAndDimensions(
                    queryVectorString, queryEntityUri, null, metricName, entityType, limit, sortBy, modelDimensions, similarityThreshold);
            } else {
                vectorResults = vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                    queryVectorString, queryEntityUri, null, metricName, entityType, limit, sortBy, similarityMetric);
            }
        }
        
        // Convert to response format
        List<Map<String, Object>> similarities = vectorResults.stream()
            .map(result -> {
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("entityUri", result.entityUri());
                resultMap.put("entityType", result.entityType());
                resultMap.put("metricType", result.metricType());
                resultMap.put("orderId", result.orderId());
                resultMap.put("ratingValue", result.ratingValue());
                resultMap.put("similarity", Math.round(result.similarity() * 1000.0) / 1000.0);
                resultMap.put("repositoryName", extractRepositoryName(result.entityUri()));
                resultMap.put("entityId", extractEntityId(result.entityUri()));
                return resultMap;
            })
            .collect(Collectors.toList());
        
        // Build response
        Map<String, Object> response = new HashMap<>();
        response.put("similarities", similarities);
        response.put("strategy", "lexical");
        response.put("totalStoredVectors", similarities.size());
        response.put("similarityMetric", similarityMetric.getValue());
        
        log.info("Found {} similar entities using {}", similarities.size(), similarityMetric);
        return response;
    }
    
    /**
     * Perform center search with custom similarity metric using 3-way split and length awareness
     */
    private List<VectorSimilarityService.SimilarityResult> performCenterSearchWithMetric(
            String queryEntityUri, Integer orderId, String metricName, String entityType, 
            String queryVectorString, int limit, 
            de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric similarityMetric, Integer modelDimensions,
            SparqlQueryService.EntityData queryEntity) {
        
        // Three-way split: best similar, median similar, worst similar
        int bestCount = limit / 3;
        int medianCount = limit / 3; 
        int worstCount = limit - bestCount - medianCount; // Handle remainder
        
        // Length awareness: get query text length for filtering
        String querySemanticContext = queryEntity.buildSemanticContext();
        int queryLength = querySemanticContext.length();
        int lengthTolerance = VectorSimilarityService.calculateLengthTolerance(queryLength);
        
        log.info("Performing 3-way length-aware center search with {} metric: {} best + {} median + {} worst (query length: {}, tolerance: {})", 
                similarityMetric, bestCount, medianCount, worstCount, queryLength, lengthTolerance);
        
        // Use length-aware search for better scope matching
        List<VectorSimilarityService.SimilarityResult> allSimilarResults;
        
        if (modelDimensions != null) {
            // Use length-aware search with dimension filtering
            allSimilarResults = vectorSimilarityService.findSimilarWithPgVectorFromQueryVectorAndDimensions(
                queryVectorString, queryEntityUri, null, metricName, entityType, limit * 3, 
                VectorSimilarityService.SortBy.SIMILARITY, modelDimensions, null, 
                queryLength, lengthTolerance);
        } else {
            // Fallback to regular search if no model dimensions specified
            allSimilarResults = vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                queryVectorString, queryEntityUri, null, metricName, entityType, limit * 3, 
                VectorSimilarityService.SortBy.SIMILARITY, similarityMetric);
        }
        
        if (allSimilarResults.isEmpty()) {
            log.warn("No similar results found for center search with metric {}", similarityMetric);
            return new ArrayList<>();
        }
        
        // NEW APPROACH: Rating-tier based center search  
        // Step 1: Sort by rating value to identify tier boundaries
        List<VectorSimilarityService.SimilarityResult> ratingsSorted = allSimilarResults.stream()
            .filter(result -> result.ratingValue() != null)
            .sorted((a, b) -> Double.compare(b.ratingValue(), a.ratingValue()))
            .collect(Collectors.toList());
            
        if (ratingsSorted.isEmpty()) {
            log.warn("No results with rating values found for rating-tier center search");
            return new ArrayList<>();
        }
        
        // Step 2: Divide into rating tiers (33%, 33%, 34%)
        int totalRated = ratingsSorted.size();
        int tier1End = totalRated / 3;                        // Best ratings (top 33%)
        int tier2End = (totalRated * 2) / 3;                  // Mid ratings (middle 33%)
        // tier3 = rest (bottom 34%)
        
        List<VectorSimilarityService.SimilarityResult> bestRatingTier = ratingsSorted.subList(0, tier1End);
        List<VectorSimilarityService.SimilarityResult> midRatingTier = ratingsSorted.subList(tier1End, tier2End);
        List<VectorSimilarityService.SimilarityResult> worstRatingTier = ratingsSorted.subList(tier2End, totalRated);
        
        log.info("Rating tiers: {} best (ratings {:.1f}-{:.1f}), {} mid (ratings {:.1f}-{:.1f}), {} worst (ratings {:.1f}-{:.1f})",
                bestRatingTier.size(), 
                bestRatingTier.isEmpty() ? 0 : bestRatingTier.get(bestRatingTier.size()-1).ratingValue(),
                bestRatingTier.isEmpty() ? 0 : bestRatingTier.get(0).ratingValue(),
                midRatingTier.size(),
                midRatingTier.isEmpty() ? 0 : midRatingTier.get(midRatingTier.size()-1).ratingValue(),
                midRatingTier.isEmpty() ? 0 : midRatingTier.get(0).ratingValue(),
                worstRatingTier.size(),
                worstRatingTier.isEmpty() ? 0 : worstRatingTier.get(worstRatingTier.size()-1).ratingValue(),
                worstRatingTier.isEmpty() ? 0 : worstRatingTier.get(0).ratingValue());
        
        // Step 3: Within each tier, find most similar to query
        List<VectorSimilarityService.SimilarityResult> centerResults = new ArrayList<>();
        
        // Best rating tier - find most similar
        if (bestCount > 0 && !bestRatingTier.isEmpty()) {
            List<VectorSimilarityService.SimilarityResult> bestSelected = bestRatingTier.stream()
                .sorted((a, b) -> Double.compare(b.similarity(), a.similarity()))  // Sort by similarity within tier
                .limit(bestCount)
                .collect(Collectors.toList());
            centerResults.addAll(bestSelected);
            log.info("Selected {} from best rating tier (similarity: {:.3f}, rating: {:.1f})", 
                    bestSelected.size(),
                    bestSelected.isEmpty() ? 0 : bestSelected.get(0).similarity(),
                    bestSelected.isEmpty() ? 0 : bestSelected.get(0).ratingValue());
        }
        
        // Mid rating tier - find most similar  
        if (medianCount > 0 && !midRatingTier.isEmpty()) {
            List<VectorSimilarityService.SimilarityResult> midSelected = midRatingTier.stream()
                .sorted((a, b) -> Double.compare(b.similarity(), a.similarity()))  // Sort by similarity within tier
                .limit(medianCount)
                .collect(Collectors.toList());
            centerResults.addAll(midSelected);
            log.info("Selected {} from mid rating tier (similarity: {:.3f}, rating: {:.1f})", 
                    midSelected.size(),
                    midSelected.isEmpty() ? 0 : midSelected.get(0).similarity(),
                    midSelected.isEmpty() ? 0 : midSelected.get(0).ratingValue());
        }
        
        // Worst rating tier - find most similar
        if (worstCount > 0 && !worstRatingTier.isEmpty()) {
            List<VectorSimilarityService.SimilarityResult> worstSelected = worstRatingTier.stream()
                .sorted((a, b) -> Double.compare(b.similarity(), a.similarity()))  // Sort by similarity within tier
                .limit(worstCount)
                .collect(Collectors.toList());
            centerResults.addAll(worstSelected);
            log.info("Selected {} from worst rating tier (similarity: {:.3f}, rating: {:.1f})", 
                    worstSelected.size(),
                    worstSelected.isEmpty() ? 0 : worstSelected.get(0).similarity(),
                    worstSelected.isEmpty() ? 0 : worstSelected.get(0).ratingValue());
        }
        
        log.info("Rating-tier center search completed: {} results with true rating diversity + length awareness", 
                centerResults.size());
        
        return centerResults;
    }
    
    /**
     * Determine the SortBy strategy based on scaleType
     */
    private VectorSimilarityService.SortBy determineSortBy(String scaleType) {
        if (scaleType == null) return VectorSimilarityService.SortBy.BEST_RATED; // Default to best rated
        
        return switch (scaleType.toLowerCase()) {
            case "best" -> VectorSimilarityService.SortBy.BEST_RATED;
            case "worst" -> VectorSimilarityService.SortBy.WORST_RATED;
            case "randomcenter" -> VectorSimilarityService.SortBy.SIMILARITY; // Use similarity for pool generation
            default -> VectorSimilarityService.SortBy.SIMILARITY;
        };
    }
    
    /**
     * Perform center search with three-way split: best similar, median similar, worst similar
     * Uses similarity scores to create a balanced representation across the similarity spectrum
     */
    private List<VectorSimilarityService.SimilarityResult> performCenterSearch(
            String queryEntityUri, Integer orderId, String metricName, String entityType, 
            String queryVectorString, int limit) {
        
        // Three-way split: best similar, median similar, worst similar
        int bestCount = limit / 3;
        int medianCount = limit / 3; 
        int worstCount = limit - bestCount - medianCount; // Handle remainder
        
        log.info("Performing 3-way center search: {} best similar + {} median similar + {} worst similar", 
                bestCount, medianCount, worstCount);
        
        // First, get a large pool of similar results to select from
        int poolSize = Math.max(limit * 3, 50); // Get more results to choose from
        List<VectorSimilarityService.SimilarityResult> allSimilarResults = 
            vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                queryVectorString, queryEntityUri, orderId, metricName, entityType, poolSize, VectorSimilarityService.SortBy.SIMILARITY);
        
        if (allSimilarResults.isEmpty()) {
            log.info("No results from same order, searching across all orders");
            allSimilarResults = vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                queryVectorString, queryEntityUri, null, metricName, entityType, poolSize, VectorSimilarityService.SortBy.SIMILARITY);
        }
        
        if (allSimilarResults.isEmpty()) {
            log.warn("No similar results found for center search");
            return new ArrayList<>();
        }
        
        // Sort by similarity score (highest first)
        allSimilarResults.sort((a, b) -> Double.compare(b.similarity(), a.similarity()));
        
        List<VectorSimilarityService.SimilarityResult> centerResults = new ArrayList<>();
        
        // 1. Best similar (highest similarity scores)
        if (bestCount > 0 && !allSimilarResults.isEmpty()) {
            List<VectorSimilarityService.SimilarityResult> bestSimilar = allSimilarResults.stream()
                .limit(Math.min(bestCount, allSimilarResults.size()))
                .collect(Collectors.toList());
            centerResults.addAll(bestSimilar);
            log.info("Selected {} best similar results (similarity range: {:.3f} - {:.3f})", 
                    bestSimilar.size(), 
                    bestSimilar.isEmpty() ? 0 : bestSimilar.get(bestSimilar.size()-1).similarity(),
                    bestSimilar.isEmpty() ? 0 : bestSimilar.get(0).similarity());
        }
        
        // 2. Median similar (middle similarity scores)
        if (medianCount > 0 && allSimilarResults.size() > bestCount) {
            int medianStart = Math.max(bestCount, allSimilarResults.size() / 3);
            int medianEnd = Math.min(medianStart + medianCount, allSimilarResults.size());
            
            List<VectorSimilarityService.SimilarityResult> medianSimilar = allSimilarResults.stream()
                .skip(medianStart)
                .limit(medianEnd - medianStart)
                .collect(Collectors.toList());
            centerResults.addAll(medianSimilar);
            log.info("Selected {} median similar results (similarity range: {:.3f} - {:.3f})", 
                    medianSimilar.size(),
                    medianSimilar.isEmpty() ? 0 : medianSimilar.get(medianSimilar.size()-1).similarity(),
                    medianSimilar.isEmpty() ? 0 : medianSimilar.get(0).similarity());
        }
        
        // 3. Worst similar (lowest similarity scores, but still above threshold)
        if (worstCount > 0 && allSimilarResults.size() > bestCount + medianCount) {
            List<VectorSimilarityService.SimilarityResult> worstSimilar = allSimilarResults.stream()
                .skip(Math.max(bestCount + medianCount, allSimilarResults.size() - worstCount))
                .collect(Collectors.toList());
            centerResults.addAll(worstSimilar);
            log.info("Selected {} worst similar results (similarity range: {:.3f} - {:.3f})", 
                    worstSimilar.size(),
                    worstSimilar.isEmpty() ? 0 : worstSimilar.get(worstSimilar.size()-1).similarity(),
                    worstSimilar.isEmpty() ? 0 : worstSimilar.get(0).similarity());
        }
        
        log.info("3-way center search completed: {} total results across similarity spectrum", centerResults.size());
        return centerResults;
    }

    /**
     * Calculate similarities using only lexical (text-based) content
     * Focuses on metric-relevant properties only for symmetry
     */
    private List<Map<String, Object>> calculateLexicalSimilarities(
            Map<String, SparqlQueryService.EntityData> expertRatedEntities,
            SparqlQueryService.EntityData queryEntity,
            String metricName,
            int limit) {
        
        log.info("Calculating lexical similarities for {} entities", expertRatedEntities.size());
        
        List<Map<String, Object>> similarities = new ArrayList<>();
        
        // Generate embedding for query entity using unified service (if provided)
        double[] queryEmbedding = null;
        if (queryEntity != null) {
            queryEmbedding = generateLexicalEmbeddingForEntity(queryEntity, null);
        }
        
        // Calculate similarities for expert entities
        for (SparqlQueryService.EntityData entity : expertRatedEntities.values()) {
            try {
                // Filter by metric if specified
                if (metricName != null && !entity.getMetricIds().contains(metricName)) {
                    continue;
                }
                
                // Generate embedding for this entity using unified service
                double[] entityEmbedding = generateLexicalEmbeddingForEntity(entity, null);
                
                // Calculate similarity
                double similarity = 1.0; // Default if no query entity
                if (queryEntity != null && queryEmbedding != null && entityEmbedding != null) {
                    similarity = calculateCosineSimilarity(queryEmbedding, entityEmbedding);
                }
                
                similarities.add(Map.of(
                    "entityUri", entity.entityUri,
                    "entityType", entity.entityType,
                    "similarity", Math.round(similarity * 1000.0) / 1000.0,
                    "embeddingGenerated", entityEmbedding != null,
                    "metricIds", new ArrayList<>(entity.getMetricIds()),
                    "repositoryName", extractRepositoryName(entity.entityUri),
                    "entityId", extractEntityId(entity.entityUri)
                ));
                
            } catch (Exception e) {
                log.warn("Failed to calculate lexical similarity for {}: {}", entity.entityUri, e.getMessage());
            }
        }
        
        // Sort by similarity and limit results
        return similarities.stream()
            .sorted((a, b) -> Double.compare((Double) b.get("similarity"), (Double) a.get("similarity")))
            .limit(limit)
            .collect(Collectors.toList());
    }
    
    /**
     * Calculate similarities using only structural (metadata) properties  
     */
    private List<Map<String, Object>> calculateStructuralSimilarities(
            Map<String, SparqlQueryService.EntityData> expertRatedEntities,
            SparqlQueryService.EntityData queryEntity,
            String metricName,
            int limit) {
        
        log.info("Calculating structural similarities for {} entities", expertRatedEntities.size());
        
        List<Map<String, Object>> similarities = new ArrayList<>();
        
        for (SparqlQueryService.EntityData entity : expertRatedEntities.values()) {
            try {
                // Filter by metric if specified
                if (metricName != null && !entity.getMetricIds().contains(metricName)) {
                    continue;
                }
                
                // Calculate structural similarity
                double similarity = 1.0; // Default if no query entity
                if (queryEntity != null) {
                    similarity = calculateStructuralSimilarity(queryEntity, entity);
                }
                
                similarities.add(Map.of(
                    "entityUri", entity.entityUri,
                    "entityType", entity.entityType,
                    "similarity", Math.round(similarity * 1000.0) / 1000.0,
                    "structuralFeatures", buildStructuralFeatures(entity),
                    "metricIds", new ArrayList<>(entity.getMetricIds()),
                    "repositoryName", extractRepositoryName(entity.entityUri),
                    "entityId", extractEntityId(entity.entityUri)
                ));
                
            } catch (Exception e) {
                log.warn("Failed to calculate structural similarity for {}: {}", entity.entityUri, e.getMessage());
            }
        }
        
        // Sort by similarity and limit results
        return similarities.stream()
            .sorted((a, b) -> Double.compare((Double) b.get("similarity"), (Double) a.get("similarity")))
            .limit(limit)
            .collect(Collectors.toList());
    }
    
    
    /**
     * Get entity embeddings using lexical strategy
     */
    public Map<String, Object> getEntityEmbeddings(
            Integer orderId, 
            String metricName) {
        
        log.info("Getting entity embeddings for order {} using lexical strategy", orderId);
        
        try {
            Map<String, SparqlQueryService.EntityData> entities = 
                sparqlQueryService.getExpertRatedEntitiesWithContent(orderId);
            
            Map<String, Object> embeddings = new HashMap<>();
            
            for (SparqlQueryService.EntityData entity : entities.values()) {
                // Filter by metric if specified
                if (metricName != null && !entity.getMetricIds().contains(metricName)) {
                    continue;
                }
                
                Map<String, Object> entityEmbedding = new HashMap<>();
                entityEmbedding.put("entityUri", entity.entityUri);
                entityEmbedding.put("entityType", entity.entityType);
                entityEmbedding.put("metricIds", new ArrayList<>(entity.getMetricIds()));
                entityEmbedding.put("repositoryName", extractRepositoryName(entity.entityUri));
                entityEmbedding.put("entityId", extractEntityId(entity.entityUri));
                
                // Add lexical embedding content using unified embedding service
                double[] embedding = generateLexicalEmbeddingForEntity(entity, null);
                entityEmbedding.put("embedding", embedding != null ? embedding.length : 0);
                entityEmbedding.put("contentType", "lexical");
                
                embeddings.put(entity.entityUri, entityEmbedding);
            }
            
            return Map.of(
                "strategy", "lexical",
                "orderId", orderId,
                "metricName", metricName != null ? metricName : "all",
                "entityCount", embeddings.size(),
                "embeddings", embeddings
            );
            
        } catch (Exception e) {
            log.error("Failed to get entity embeddings: {}", e.getMessage());
            return Map.of(
                "strategy", "lexical",
                "error", e.getMessage(),
                "embeddings", Map.of(),
                "entityCount", 0
            );
        }
    }
    
    
    /**
     * Build structural features excluding textual content
     */
    private Map<String, Object> buildStructuralFeatures(SparqlQueryService.EntityData entity) {
        Map<String, Object> features = new HashMap<>();
        
        features.put("entityType", entity.entityType);
        features.put("repositoryName", extractRepositoryName(entity.entityUri));
        features.put("entityId", extractEntityId(entity.entityUri));
        features.put("hasTitle", entity.title != null && !entity.title.trim().isEmpty());
        features.put("hasBody", entity.body != null && !entity.body.trim().isEmpty());
        features.put("hasMessage", entity.message != null && !entity.message.trim().isEmpty());
        features.put("hasCommentBody", entity.commentBody != null && !entity.commentBody.trim().isEmpty());
        features.put("metricCount", entity.getMetricIds().size());
        features.put("metricTypes", new ArrayList<>(entity.getMetricIds()));
        
        return features;
    }
    
    /**
     * Calculate cosine similarity between two embedding vectors
     */
    private double calculateCosineSimilarity(double[] vectorA, double[] vectorB) {
        if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
            return 0.0;
        }
        
        double dotProduct = 0.0;
        double magnitudeA = 0.0;
        double magnitudeB = 0.0;
        
        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            magnitudeA += vectorA[i] * vectorA[i];
            magnitudeB += vectorB[i] * vectorB[i];
        }
        
        magnitudeA = Math.sqrt(magnitudeA);
        magnitudeB = Math.sqrt(magnitudeB);
        
        if (magnitudeA == 0.0 || magnitudeB == 0.0) {
            return 0.0;
        }
        
        return dotProduct / (magnitudeA * magnitudeB);
    }
    
    
    /**
     * Fetch raw entity properties for similarity results via query service
     * This enriches similarity results with full entity content
     */
    public List<Map<String, Object>> enrichSimilarityResultsWithRawData(
            List<VectorSimilarityService.SimilarityResult> results) {
        
        log.info("Enriching {} similarity results with raw entity data", results.size());
        
        List<Map<String, Object>> enrichedResults = new ArrayList<>();
        
        for (VectorSimilarityService.SimilarityResult result : results) {
            Map<String, Object> enrichedResult = new HashMap<>();
            
            // Basic similarity data
            enrichedResult.put("entityUri", result.entityUri());
            enrichedResult.put("entityType", result.entityType());
            enrichedResult.put("metricType", result.metricType());
            enrichedResult.put("orderId", result.orderId());
            enrichedResult.put("ratingValue", result.ratingValue());
            enrichedResult.put("similarity", Math.round(result.similarity() * 1000.0) / 1000.0);
            enrichedResult.put("repositoryName", extractRepositoryName(result.entityUri()));
            enrichedResult.put("entityId", extractEntityId(result.entityUri()));
            
            // Fetch raw entity data
            try {
                SparqlQueryService.EntityData rawData = sparqlQueryService.getSingleEntityContent(
                    result.orderId(), result.entityUri(), result.entityType());
                
                if (rawData != null) {
                    Map<String, Object> rawProperties = new HashMap<>();
                    
                    // Only add non-null entity properties (the actual raw data users want)
                    if (rawData.title != null && !rawData.title.trim().isEmpty()) {
                        rawProperties.put("title", rawData.title);
                    }
                    if (rawData.body != null && !rawData.body.trim().isEmpty()) {
                        rawProperties.put("body", rawData.body);
                    }
                    if (rawData.message != null && !rawData.message.trim().isEmpty()) {
                        rawProperties.put("message", rawData.message);
                    }
                    if (rawData.commentBody != null && !rawData.commentBody.trim().isEmpty()) {
                        rawProperties.put("commentBody", rawData.commentBody);
                    }
                    if (rawData.getMetricIds() != null && !rawData.getMetricIds().isEmpty()) {
                        rawProperties.put("metricIds", new ArrayList<>(rawData.getMetricIds()));
                    }
                    
                    // Add entity metadata
                    rawProperties.put("entityUri", rawData.entityUri);
                    rawProperties.put("entityType", rawData.entityType);
                    
                    enrichedResult.put("rawData", rawProperties);
                    log.debug("Successfully fetched raw data for entity: {}", result.entityUri());
                } else {
                    enrichedResult.put("rawData", null);
                    log.warn("Could not fetch raw data for entity: {} (orderId: {})", 
                            result.entityUri(), result.orderId());
                }
                
            } catch (Exception e) {
                enrichedResult.put("rawData", null);
                log.error("Failed to fetch raw data for entity {}: {}", result.entityUri(), e.getMessage());
            }
            
            enrichedResults.add(enrichedResult);
        }
        
        log.info("Successfully enriched {} similarity results", enrichedResults.size());
        return enrichedResults;
    }
    
    /**
     * Perform randomCenter search with random selection from rating-based parts
     * Instead of splitting by similarity, randomly selects from best/undecided/worst rating groups
     */
    private List<VectorSimilarityService.SimilarityResult> performRandomCenterSearch(
            String queryEntityUri, Integer orderId, String metricName, String entityType, 
            String queryVectorString, int limit) {
        
        log.info("Performing randomCenter search: random selection from rating-based parts (best/undecided/worst)");
        
        // Get a large pool of similar results to select from
        int poolSize = Math.max(limit * 3, 50);
        List<VectorSimilarityService.SimilarityResult> allSimilarResults = 
            vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                queryVectorString, queryEntityUri, orderId, metricName, entityType, poolSize, VectorSimilarityService.SortBy.SIMILARITY);
        
        if (allSimilarResults.isEmpty()) {
            log.info("No results from same order, searching across all orders");
            allSimilarResults = vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                queryVectorString, queryEntityUri, null, metricName, entityType, poolSize, VectorSimilarityService.SortBy.SIMILARITY);
        }
        
        if (allSimilarResults.isEmpty()) {
            log.warn("No similar results found for randomCenter search");
            return new ArrayList<>();
        }
        
        return performRandomCenterSelectionFromSimilarityResults(allSimilarResults, limit);
    }
    
    
    /**
     * Perform random selection from rating-based parts (best/undecided/worst)
     * This is the core logic for randomCenter - randomly picks from rating tiers instead of similarity
     */
    private List<VectorSimilarityService.SimilarityResult> performRandomCenterSelectionFromSimilarityResults(
            List<VectorSimilarityService.SimilarityResult> allResults, int limit) {
        
        // Filter results with rating values
        List<VectorSimilarityService.SimilarityResult> ratedResults = allResults.stream()
            .filter(result -> result.ratingValue() != null)
            .collect(Collectors.toList());
            
        if (ratedResults.isEmpty()) {
            log.warn("No results with rating values found for randomCenter selection");
            return new ArrayList<>();
        }
        
        // Sort by rating value to identify boundaries
        ratedResults.sort((a, b) -> Double.compare(b.ratingValue(), a.ratingValue()));
        
        // Three-way split based on rating tiers
        int totalSize = ratedResults.size();
        int tier1End = totalSize / 3;      // Best ratings (top 33%)
        int tier2End = (totalSize * 2) / 3; // Mid ratings (middle 33%)
        // tier3 = rest (bottom 34%)
        
        List<VectorSimilarityService.SimilarityResult> bestRatingTier = ratedResults.subList(0, tier1End);
        List<VectorSimilarityService.SimilarityResult> midRatingTier = ratedResults.subList(tier1End, tier2End);
        List<VectorSimilarityService.SimilarityResult> worstRatingTier = ratedResults.subList(tier2End, totalSize);
        
        log.info("Rating tiers: {} best, {} undecided, {} worst ratings", 
                bestRatingTier.size(), midRatingTier.size(), worstRatingTier.size());
        
        // Determine how many to select from each tier
        int bestCount = limit / 3;
        int undecidedCount = limit / 3;
        int worstCount = limit - bestCount - undecidedCount; // Handle remainder
        
        List<VectorSimilarityService.SimilarityResult> randomCenterResults = new ArrayList<>();
        
        // Randomly select from best rating tier
        if (bestCount > 0 && !bestRatingTier.isEmpty()) {
            List<VectorSimilarityService.SimilarityResult> shuffledBest = new ArrayList<>(bestRatingTier);
            Collections.shuffle(shuffledBest);
            randomCenterResults.addAll(shuffledBest.stream().limit(bestCount).collect(Collectors.toList()));
            log.info("Randomly selected {} from best rating tier", Math.min(bestCount, shuffledBest.size()));
        }
        
        // Randomly select from undecided (middle) rating tier
        if (undecidedCount > 0 && !midRatingTier.isEmpty()) {
            List<VectorSimilarityService.SimilarityResult> shuffledMid = new ArrayList<>(midRatingTier);
            Collections.shuffle(shuffledMid);
            randomCenterResults.addAll(shuffledMid.stream().limit(undecidedCount).collect(Collectors.toList()));
            log.info("Randomly selected {} from undecided rating tier", Math.min(undecidedCount, shuffledMid.size()));
        }
        
        // Randomly select from worst rating tier
        if (worstCount > 0 && !worstRatingTier.isEmpty()) {
            List<VectorSimilarityService.SimilarityResult> shuffledWorst = new ArrayList<>(worstRatingTier);
            Collections.shuffle(shuffledWorst);
            randomCenterResults.addAll(shuffledWorst.stream().limit(worstCount).collect(Collectors.toList()));
            log.info("Randomly selected {} from worst rating tier", Math.min(worstCount, shuffledWorst.size()));
        }
        
        log.info("RandomCenter selection completed: {} total results with random selection from rating tiers", 
                randomCenterResults.size());
        
        return randomCenterResults;
    }
    
    /**
     * Perform random selection from rating-based parts for multi-metric enhanced search
     */
    private List<Map<String, Object>> performRandomCenterSelection(
            List<Map<String, Object>> allSimilarities, int limit) {
        
        if (allSimilarities.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Sort by rating value to identify boundaries
        allSimilarities.sort((a, b) -> {
            Double ratingA = (Double) a.get("ratingValue");
            Double ratingB = (Double) b.get("ratingValue");
            return Double.compare(ratingB, ratingA); // Sort by rating (descending)
        });
        
        // Use the same rating value-based logic as performRandomCenterSelectionFromRatings
        return performRandomCenterSelectionFromRatings(allSimilarities, limit);
    }
    
    /**
     * Perform random selection from rating-based parts for rating-based search fallback
     */
    private List<Map<String, Object>> performRandomCenterSelectionFromRatings(
            List<Map<String, Object>> filteredResults, int limit) {
        
        log.info("🎯 performRandomCenterSelectionFromRatings called with {} entities, limit: {}", 
                filteredResults.size(), limit);
        
        if (filteredResults.isEmpty()) {
            log.warn("🎯 No entities provided to performRandomCenterSelectionFromRatings");
            return new ArrayList<>();
        }
        
        // Sort by rating value first
        filteredResults.sort((a, b) -> 
            Double.compare((Double) b.get("ratingValue"), (Double) a.get("ratingValue")));
        
        int totalSize = filteredResults.size();
        
        if (totalSize <= limit) {
            // If we have fewer results than requested, return all shuffled
            List<Map<String, Object>> shuffledResults = new ArrayList<>(filteredResults);
            Collections.shuffle(shuffledResults);
            return shuffledResults;
        }
        
        // Three-way split: best, undecided, worst
        int bestCount = limit / 3;
        int undecidedCount = limit / 3;
        int worstCount = limit - bestCount - undecidedCount; // Handle remainder
        
        // Debug: Show all rating values before processing
        List<Double> allRatings = filteredResults.stream()
            .map(r -> (Double) r.get("ratingValue"))
            .collect(Collectors.toList());
        log.info("🎯 All rating values in order: {}", allRatings);
        
        // Use rating value-based boundaries instead of position-based boundaries
        double maxRating = ((Double) filteredResults.get(0).get("ratingValue"));
        double minRating = ((Double) filteredResults.get(totalSize - 1).get("ratingValue"));
        double ratingRange = maxRating - minRating;
        
        log.info("RandomCenter rating analysis: {} entities, rating range {:.1f} to {:.1f} (span: {:.1f})", 
                totalSize, minRating, maxRating, ratingRange);
        
        List<Map<String, Object>> randomCenterResults = new ArrayList<>();
        
        // If all entities have the same rating, just return random selection
        if (ratingRange < 0.1) { // Essentially the same rating
            log.info("All entities have similar ratings ({:.1f}), performing pure random selection", maxRating);
            List<Map<String, Object>> shuffledAll = new ArrayList<>(filteredResults);
            Collections.shuffle(shuffledAll);
            return shuffledAll.subList(0, Math.min(limit, shuffledAll.size()));
        }
        
        // Define rating tier boundaries (33rd and 67th percentiles)
        double highThreshold = maxRating - (ratingRange * 0.33);  // Top 33%
        double lowThreshold = maxRating - (ratingRange * 0.67);   // Bottom 33%
        
        log.info("Rating tier thresholds: best ≥ {:.1f}, undecided {:.1f}-{:.1f}, worst ≤ {:.1f}", 
                highThreshold, lowThreshold, highThreshold, lowThreshold);
        
        // Separate entities into rating-based tiers
        List<Map<String, Object>> bestRatingTier = new ArrayList<>();
        List<Map<String, Object>> undecidedRatingTier = new ArrayList<>();
        List<Map<String, Object>> worstRatingTier = new ArrayList<>();
        
        for (Map<String, Object> entity : filteredResults) {
            double rating = (Double) entity.get("ratingValue");
            if (rating >= highThreshold) {
                bestRatingTier.add(entity);
            } else if (rating >= lowThreshold) {
                undecidedRatingTier.add(entity);
            } else {
                worstRatingTier.add(entity);
            }
        }
        
        log.info("Rating tier sizes: {} best (≥{:.1f}), {} undecided ({:.1f}-{:.1f}), {} worst (≤{:.1f})", 
                bestRatingTier.size(), highThreshold, 
                undecidedRatingTier.size(), lowThreshold, highThreshold,
                worstRatingTier.size(), lowThreshold);
        
        // Step 1: Try to select the intended amount from each tier
        List<Map<String, Object>> availableBest = new ArrayList<>(bestRatingTier);
        List<Map<String, Object>> availableUndecided = new ArrayList<>(undecidedRatingTier);
        List<Map<String, Object>> availableWorst = new ArrayList<>(worstRatingTier);
        
        // Shuffle all tiers for random selection
        Collections.shuffle(availableBest);
        Collections.shuffle(availableUndecided);
        Collections.shuffle(availableWorst);
        
        // Collect what we can from each tier
        int selectedFromBest = 0, selectedFromUndecided = 0, selectedFromWorst = 0;
        
        // Select from best tier
        if (bestCount > 0 && !availableBest.isEmpty()) {
            selectedFromBest = Math.min(bestCount, availableBest.size());
            randomCenterResults.addAll(availableBest.subList(0, selectedFromBest));
            availableBest = availableBest.subList(selectedFromBest, availableBest.size());
            log.info("Selected {} entities from best rating tier", selectedFromBest);
        }
        
        // Select from undecided tier
        if (undecidedCount > 0 && !availableUndecided.isEmpty()) {
            selectedFromUndecided = Math.min(undecidedCount, availableUndecided.size());
            randomCenterResults.addAll(availableUndecided.subList(0, selectedFromUndecided));
            availableUndecided = availableUndecided.subList(selectedFromUndecided, availableUndecided.size());
            log.info("Selected {} entities from undecided rating tier", selectedFromUndecided);
        }
        
        // Select from worst tier
        if (worstCount > 0 && !availableWorst.isEmpty()) {
            selectedFromWorst = Math.min(worstCount, availableWorst.size());
            randomCenterResults.addAll(availableWorst.subList(0, selectedFromWorst));
            availableWorst = availableWorst.subList(selectedFromWorst, availableWorst.size());
            log.info("Selected {} entities from worst rating tier", selectedFromWorst);
        }
        
        int currentTotal = randomCenterResults.size();
        int needed = limit - currentTotal;
        
        // Step 2: If we still need more entities, fill from available tiers
        if (needed > 0) {
            log.info("Need {} more entities to reach limit of {}. Filling from remaining entities.", needed, limit);
            
            // Combine all remaining entities and shuffle
            List<Map<String, Object>> remainingEntities = new ArrayList<>();
            remainingEntities.addAll(availableBest);
            remainingEntities.addAll(availableUndecided);
            remainingEntities.addAll(availableWorst);
            
            Collections.shuffle(remainingEntities);
            
            int toAdd = Math.min(needed, remainingEntities.size());
            randomCenterResults.addAll(remainingEntities.subList(0, toAdd));
            
            log.info("Added {} additional entities from remaining pool to reach total: {}", 
                    toAdd, randomCenterResults.size());
        }
        
        log.info("🎯 Final randomCenter selection: {} entities (requested: {})", 
                randomCenterResults.size(), limit);
        
        return randomCenterResults;
    }
    
    /**
     * Check if normalized entity type matches stored entity type format
     */
    private boolean isEntityTypeMatch(String normalizedType, String storedType) {
        if (normalizedType == null || storedType == null) {
            return false;
        }
        
        // Handle mapping between normalized and stored formats
        return switch (normalizedType.toLowerCase()) {
            case "commit" -> "GitCommit".equals(storedType);
            case "issue" -> "GithubIssue".equals(storedType);
            case "pull_request" -> "GithubPullRequest".equals(storedType);
            case "comment" -> "GithubComment".equals(storedType);
            default -> false;
        };
    }
}
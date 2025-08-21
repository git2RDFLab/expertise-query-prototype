package de.leipzig.htwk.gitrdf.expertise.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import de.leipzig.htwk.gitrdf.expertise.model.EmbeddingModel;
import de.leipzig.htwk.gitrdf.expertise.model.SimilarityMetric;
import de.leipzig.htwk.gitrdf.expertise.model.EntityType;
import de.leipzig.htwk.gitrdf.expertise.validation.RequestValidator;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertFile;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertRating;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertSheet;
import de.leipzig.htwk.gitrdf.expertise.model.Requests.IngestRequest;
import de.leipzig.htwk.gitrdf.expertise.service.ContextBuilderService;
import de.leipzig.htwk.gitrdf.expertise.service.EntityTypeService;
import de.leipzig.htwk.gitrdf.expertise.service.ProcessingStatusService;
import de.leipzig.htwk.gitrdf.expertise.service.SimilarityService;
import de.leipzig.htwk.gitrdf.expertise.service.data.ExpertRatingService;
import de.leipzig.htwk.gitrdf.expertise.service.embedding.BatchEmbeddingService;
import de.leipzig.htwk.gitrdf.expertise.service.embedding.VectorSimilarityService;
import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import de.leipzig.htwk.gitrdf.expertise.service.storage.StorageService;
import de.leipzig.htwk.gitrdf.expertise.service.metrics.MetricsStatisticsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Controller that publishes an API Endpoint to deliver expertise data for questions and Datasets
 */
@RestController("expertiseServiceController")
@RequestMapping("/api/v2")
@RequiredArgsConstructor
@Slf4j
public class ExpertiseController {

    private final ExpertRatingService expertRatingService;
    private final StorageService storageService;
    private final SimilarityService similarityService;
    private final VectorSimilarityService vectorSimilarityService;
    private final BatchEmbeddingService batchEmbeddingService;
    private final SparqlQueryService sparqlQueryService;
    private final ProcessingStatusService processingStatusService;
    private final EntityTypeService entityTypeService;
    private final ContextBuilderService contextBuilderService;
    private final MetricsStatisticsService metricsStatisticsService;
    private final EmbeddingModel embeddingModel;
    private final RestTemplate restTemplate = new RestTemplate();
    
    // Optional Redis cache service (only available when caching is enabled)
    
    @Value("${services.listener.url}")
    private String listenerServiceUrl;

    /**
     * Ingest all expert ratings in the directory
     */
    @PostMapping("/ingest")
    public ResponseEntity<Map<String, Object>> ingestAllExpertRatings(
            @RequestBody IngestRequest request,
            @RequestParam(required = false) String sessionId) {
        try {
            String actualSessionId = sessionId != null ? sessionId : UUID.randomUUID().toString();
            
            log.info("Ingesting all expert ratings with session: {}", actualSessionId);
            
            if (!expertRatingService.checkDirectoryExists()) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", "Experts directory not found or not accessible",
                        "processedFiles", List.of()
                    ));
            }

            // Validate request
            if (request.getStoreToDatabase() == null || request.getSaveToFile() == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", "Both storeToDatabase and saveToFile flags are required",
                        "processedFiles", List.of()
                    ));
            }

            if (!request.getStoreToDatabase() && !request.getSaveToFile()) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", "At least one of storeToDatabase or saveToFile must be true",
                        "processedFiles", List.of()
                    ));
            }

            // Process all expert files regardless of target ID
            List<String> processedFiles = expertRatingService.processAllExpertFiles(
                actualSessionId, request.getSaveToFile(), request.getStoreToDatabase());
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", String.format("Successfully processed %d expert files", processedFiles.size()),
                "processedFiles", processedFiles,
                "sessionId", actualSessionId,
                "storeToDatabase", request.getStoreToDatabase(),
                "saveToFile", request.getSaveToFile()
            ));
            
        } catch (Exception e) {
            log.error("Failed to ingest all expert ratings", e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "message", "Failed to ingest expert ratings: " + e.getMessage(),
                    "processedFiles", List.of()
                ));
        }
    }

    /**
     * Ingest expert ratings for a specific target ID
     */
    @PostMapping("/ingest/{targetId}")
    public ResponseEntity<Map<String, Object>> ingestExpertRatings(
            @PathVariable Integer targetId,
            @RequestBody IngestRequest request,
            @RequestParam(required = false) String sessionId) {
        try {
            String actualSessionId = sessionId != null ? sessionId : UUID.randomUUID().toString();
            
            log.info("Ingesting expert ratings for targetId: {} with session: {}", targetId, actualSessionId);
            
            if (!expertRatingService.checkDirectoryExists()) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", "Experts directory not found or not accessible",
                        "processedFiles", List.of(),
                        "targetId", targetId
                    ));
            }

            // Validate request
            if (request.getStoreToDatabase() == null || request.getSaveToFile() == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", "Both storeToDatabase and saveToFile flags are required",
                        "processedFiles", List.of(),
                        "targetId", targetId
                    ));
            }

            if (!request.getStoreToDatabase() && !request.getSaveToFile()) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", "At least one of storeToDatabase or saveToFile must be true",
                        "processedFiles", List.of(),
                        "targetId", targetId
                    ));
            }

            // If user wants to store to database, validate that the orderID exists
            if (request.getStoreToDatabase()) {
                if (!storageService.orderExists(targetId)) {
                    return ResponseEntity.badRequest()
                        .body(Map.of(
                            "success", false,
                            "message", String.format("Order ID %d does not exist in the database. Cannot store to database.", targetId),
                            "processedFiles", List.of(),
                            "targetId", targetId
                        ));
                }
            }

            List<String> processedFiles = expertRatingService.processExpertFiles(
                actualSessionId, targetId, request.getSaveToFile(), request.getStoreToDatabase());
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", String.format("Successfully processed %d expert files for target %d", 
                        processedFiles.size(), targetId),
                "processedFiles", processedFiles,
                "targetId", targetId,
                "sessionId", actualSessionId,
                "storeToDatabase", request.getStoreToDatabase(),
                "saveToFile", request.getSaveToFile()
            ));
            
        } catch (Exception e) {
            log.error("Failed to ingest expert ratings for targetId: {}", targetId, e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "message", "Failed to ingest expert ratings: " + e.getMessage(),
                    "processedFiles", List.of(),
                    "targetId", targetId
                ));
        }
    }

    /**
     * Get information about available expert files without processing them
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getExpertRatingsInfo() {
        try {
            log.info("Retrieving expert rating files information");
            
            if (!expertRatingService.checkDirectoryExists()) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", "Experts directory not found or not accessible",
                        "expertFiles", List.of()
                    ));
            }

            List<ExpertFile> expertFiles = expertRatingService.getExpertFilesInfoWithRatings();
            
            // Cache all URIs across all sheets and files for distinctness analysis
            Set<String> allUris = new HashSet<>();
            Map<String, Integer> uriCounts = new HashMap<>();
            Map<String, Set<String>> uriToExpertsMap = new HashMap<>();
            Map<String, Set<String>> uriToMetricsMap = new HashMap<>();
            
            for (ExpertFile file : expertFiles) {
                String expertName = file.getExpertName() != null ? file.getExpertName() : "Unknown";
                
                for (ExpertSheet sheet : file.getSheets()) {
                    String metricName = sheet.getMetricId() != null ? sheet.getMetricId() : sheet.getMetricName();
                    
                    if (sheet.getRatings() != null) {
                        for (ExpertRating rating : sheet.getRatings()) {
                            if (rating.getEntity() != null) {
                                String uri = rating.getEntity();
                                allUris.add(uri);
                                
                                // Count occurrences
                                uriCounts.put(uri, uriCounts.getOrDefault(uri, 0) + 1);
                                
                                // Track which experts rated this URI
                                uriToExpertsMap.computeIfAbsent(uri, k -> new HashSet<>()).add(expertName);
                                
                                // Track which metrics this URI was rated on
                                uriToMetricsMap.computeIfAbsent(uri, k -> new HashSet<>()).add(metricName);
                            }
                        }
                    }
                }
            }
            
            // Analyze URI distinctness and overlaps
            int totalUris = allUris.size();
            int totalRatingEntries = uriCounts.values().stream().mapToInt(Integer::intValue).sum();
            long duplicateUris = uriCounts.values().stream().filter(count -> count > 1).count();
            double distinctnessRatio = totalRatingEntries > 0 ? (double) totalUris / totalRatingEntries : 0.0;
            
            // Find most commonly rated URIs
            List<Map<String, Object>> topRatedUris = uriCounts.entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .map(entry -> {
                    String uri = entry.getKey();
                    Map<String, Object> uriInfo = new HashMap<>();
                    uriInfo.put("uri", uri);
                    uriInfo.put("ratingCount", entry.getValue());
                    uriInfo.put("expertsCount", uriToExpertsMap.get(uri).size());
                    uriInfo.put("metricsCount", uriToMetricsMap.get(uri).size());
                    uriInfo.put("experts", new ArrayList<>(uriToExpertsMap.get(uri)));
                    uriInfo.put("metrics", new ArrayList<>(uriToMetricsMap.get(uri)));
                    return uriInfo;
                })
                .collect(Collectors.toList());
            
            // Create simplified response with additional rating counts
            List<Map<String, Object>> simplifiedFiles = expertFiles.stream()
                .map(file -> {
                    Map<String, Object> fileInfo = new java.util.HashMap<>();
                    fileInfo.put("fileName", file.getFileName());
                    fileInfo.put("expertName", file.getExpertName() != null ? file.getExpertName() : "Unknown");
                    fileInfo.put("expertise", file.getExpertise() != null ? file.getExpertise() : "Unknown");
                    fileInfo.put("ratingDate", file.getRatingDate() != null ? file.getRatingDate() : "Unknown");
                    fileInfo.put("targetId", file.getTargetId() != null ? file.getTargetId() : "Unknown");
                    fileInfo.put("sheetsCount", file.getSheets().size());
                    // Per-sheet details
                    List<Map<String, Object>> sheets = file.getSheets().stream()
                        .map(sheet -> {
                            Map<String, Object> sheetInfo = new HashMap<>();
                            sheetInfo.put("metricName", sheet.getMetricId() != null ? sheet.getMetricId() : sheet.getMetricName());
                            sheetInfo.put("metricLabel", sheet.getMetricLabel() != null ? sheet.getMetricLabel() : sheet.getMetricName());
                            sheetInfo.put("ratingsCount", sheet.getRatingsCount());
                            return sheetInfo;
                        })
                        .toList();
                    fileInfo.put("sheets", sheets);
                    // Per-expert total ratings across all sheets
                    int totalRatingsForExpert = file.getSheets().stream()
                        .map(sheet -> sheet.getRatingsCount() != null ? sheet.getRatingsCount() : 0)
                        .mapToInt(Integer::intValue)
                        .sum();
                    fileInfo.put("totalRatings", totalRatingsForExpert);
                    return fileInfo;
                })
                .toList();

            // Top-level summary
            int totalExperts = expertFiles.size();
            int totalSheets = expertFiles.stream().mapToInt(f -> f.getSheets().size()).sum();
            int totalRatings = expertFiles.stream()
                .flatMap(f -> f.getSheets().stream())
                .map(sheet -> sheet.getRatingsCount() != null ? sheet.getRatingsCount() : 0)
                .mapToInt(Integer::intValue)
                .sum();

            List<Map<String, Object>> ratingsByExpert = expertFiles.stream()
                .map(file -> {
                    Map<String, Object> expertInfo = new HashMap<>();
                    expertInfo.put("expertName", file.getExpertName() != null ? file.getExpertName() : "Unknown");
                    expertInfo.put("targetId", file.getTargetId() != null ? file.getTargetId() : "Unknown");
                    expertInfo.put("totalRatings", file.getSheets().stream()
                        .map(sheet -> sheet.getRatingsCount() != null ? sheet.getRatingsCount() : 0)
                        .mapToInt(Integer::intValue)
                        .sum());
                    return expertInfo;
                })
                .toList();
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", String.format("Found %d expert files", expertFiles.size()),
                "expertFiles", simplifiedFiles,
                "summary", Map.of(
                    "totalExperts", totalExperts,
                    "totalSheets", totalSheets,
                    "totalRatings", totalRatings,
                    "ratingsByExpert", ratingsByExpert
                ),
                "uriAnalysis", Map.of(
                    "totalUniqueUris", totalUris,
                    "totalRatingEntries", totalRatingEntries,
                    "duplicateUris", duplicateUris,
                    "distinctnessRatio", Math.round(distinctnessRatio * 1000.0) / 1000.0,
                    "isFullyDistinct", duplicateUris == 0,
                    "topDuplicatedUris", topRatedUris,
                    "statistics", Map.of(
                        "averageRatingsPerUri", totalUris > 0 ? Math.round((double) totalRatingEntries / totalUris * 100.0) / 100.0 : 0.0,
                        "maxRatingsForSingleUri", uriCounts.values().stream().mapToInt(Integer::intValue).max().orElse(0),
                        "urisRatedByMultipleExperts", uriToExpertsMap.values().stream().filter(experts -> experts.size() > 1).count(),
                        "urisRatedOnMultipleMetrics", uriToMetricsMap.values().stream().filter(metrics -> metrics.size() > 1).count()
                    )
                )
            ));
            
        } catch (Exception e) {
            log.error("Failed to retrieve expert rating files information", e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "message", "Failed to retrieve expert rating files information: " + e.getMessage(),
                    "expertFiles", List.of()
                ));
        }
    }

    /**
     * Check if the experts directory exists and is accessible
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getExpertRatingsStatus() {
        boolean directoryExists = expertRatingService.checkDirectoryExists();
        
        return ResponseEntity.ok(Map.of(
            "directoryExists", directoryExists,
            "message", directoryExists ? 
                "Experts directory is accessible" : 
                "Experts directory not found or not accessible"
        ));
    }

    /**
     * Debug endpoint to check if a specific order ID exists in the database
     */
    @GetMapping("/debug/order/{orderId}")
    public ResponseEntity<Map<String, Object>> checkOrderExists(@PathVariable Integer orderId) {
        try {
            boolean exists = storageService.orderExists(orderId);
            
            return ResponseEntity.ok(Map.of(
                "orderId", orderId,
                "exists", exists,
                "message", exists ? 
                    String.format("Order ID %d exists in the database", orderId) : 
                    String.format("Order ID %d does not exist in the database", orderId)
            ));
            
        } catch (Exception e) {
            log.error("Error checking if order {} exists", orderId, e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "orderId", orderId,
                    "exists", false,
                    "error", e.getMessage(),
                    "message", String.format("Error occurred while checking order ID %d: %s", orderId, e.getMessage())
                ));
        }
    }


    /**
     * Get all available metrics for a specific order
     */
    @GetMapping("/metrics/{orderId}")
    public ResponseEntity<Map<String, Object>> getMetrics(@PathVariable Integer orderId) {
        try {
            log.info("Getting available metrics for order: {}", orderId);
            
            if (!storageService.orderExists(orderId)) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", String.format("Order ID %d does not exist in the database", orderId),
                        "orderId", orderId
                    ));
            }
            
            List<String> availableMetrics = similarityService.getAvailableMetrics(orderId);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "orderId", orderId,
                "metrics", availableMetrics
            ));
            
        } catch (Exception e) {
            log.error("Error getting metrics for order {}", orderId, e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "message", "Failed to get metrics: " + e.getMessage(),
                    "orderId", orderId
                ));
        }
    }

    /**
     * Get all entities with expert ratings for a specific order
     */
    @GetMapping("/entities/{orderId}")
    public ResponseEntity<Map<String, Object>> getEntities(@PathVariable Integer orderId) {
        try {
            log.info("Getting entities for order: {}", orderId);
            
            if (!storageService.orderExists(orderId)) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", String.format("Order ID %d does not exist in the database", orderId),
                        "orderId", orderId
                    ));
            }
            
            List<Map<String, Object>> entities = similarityService.getExpertRatedEntities(orderId);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "orderId", orderId,
                "entities", entities,
                "count", entities.size()
            ));
            
        } catch (Exception e) {
            log.error("Error getting entities for order {}", orderId, e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "message", "Failed to get entities: " + e.getMessage(),
                    "orderId", orderId
                ));
        }
    }

    /**
     * Get similarity scores using embedding analysis
     */
    @PostMapping("/similarity")
    public ResponseEntity<Map<String, Object>> getAdvancedSimilarity(
            @RequestBody AdvancedSimilarityRequest request) {
        try {
            log.info("Getting lexical similarity for order: {}", request.orderId());
            
            if (!storageService.orderExists(request.orderId())) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", String.format("Order ID %d does not exist in the database", request.orderId()),
                        "orderId", request.orderId()
                    ));
            }
            
            // Validate and normalize entity type using new enum system
            String normalizedEntityType = entityTypeService.validateAndNormalize(request.entityType()).getNormalizedName();
            
            // Default scaleType to "best" if not provided
            String scaleType = request.scaleType() != null ? request.scaleType() : "best";
            log.info("🔍 Controller received scaleType: '{}' (original: '{}')", scaleType, request.scaleType());
            
            // Default fallbackStrategy to "center" if not provided
            String fallbackStrategy = request.fallbackStrategy() != null ? request.fallbackStrategy() : "center";
            
            
            // Parse similarity metric (defaults to cosine if not provided)
            SimilarityMetric similarityMetric = SimilarityMetric.COSINE;
            if (request.similarityMetric() != null && !request.similarityMetric().trim().isEmpty()) {
                similarityMetric = SimilarityMetric.fromString(request.similarityMetric());
            }
            
            // Smart validation with detailed error messages
            RequestValidator.validateSimilarityThreshold(request.similarityThreshold(), "similarityThreshold");
            RequestValidator.validateScaleType(scaleType, "scaleType");
            RequestValidator.validateScaleType(fallbackStrategy, "fallbackStrategy");
            
            Integer modelDimensions = embeddingModel.getDimensions();
            
            // Use the enhanced method with multi-metric support and fallback strategy
            Map<String, Object> similarityResults = similarityService.calculateSimilarityWithEnhancedOptions(
                request.orderId(), request.entityUri(), normalizedEntityType, 
                request.metricNames(), request.limit() != null ? request.limit() : 10, 
                scaleType, request.similarityThreshold(), similarityMetric, fallbackStrategy, modelDimensions);
            
            // If raw data is requested, enrich with full entity properties
            if (Boolean.TRUE.equals(request.raw())) {
                Map<String, Object> enrichedResults = new HashMap<>(similarityResults);
                
                // 1. Add raw data for the query entity itself
                try {
                    SparqlQueryService.EntityData queryEntityData = sparqlQueryService.getSingleEntityContent(
                        request.orderId(), request.entityUri(), normalizedEntityType);
                    
                    if (queryEntityData != null) {
                        Map<String, Object> queryRawData = new HashMap<>();
                        queryRawData.put("entityUri", queryEntityData.entityUri);
                        queryRawData.put("entityType", queryEntityData.entityType);
                        
                        // Add all available properties based on entity type
                        if (queryEntityData.title != null && !queryEntityData.title.trim().isEmpty()) {
                            queryRawData.put("title", queryEntityData.title);
                        }
                        if (queryEntityData.body != null && !queryEntityData.body.trim().isEmpty()) {
                            queryRawData.put("body", queryEntityData.body);
                        }
                        if (queryEntityData.message != null && !queryEntityData.message.trim().isEmpty()) {
                            queryRawData.put("message", queryEntityData.message);
                        }
                        if (queryEntityData.commentBody != null && !queryEntityData.commentBody.trim().isEmpty()) {
                            queryRawData.put("commentBody", queryEntityData.commentBody);
                            // NO ALIAS! commentBody should not override body for other entity types
                        }
                        
                        enrichedResults.put("queryRawData", queryRawData);
                    }
                } catch (Exception e) {
                    log.warn("Failed to fetch raw data for query entity {} from order {}: {}", 
                            request.entityUri(), request.orderId(), e.getMessage());
                }
                
                // 2. Enrich similarities with raw data if similarities exist
                if (similarityResults.containsKey("similarities")) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> similarities = (List<Map<String, Object>>) similarityResults.get("similarities");
                    
                    // Convert to SimilarityResult objects for enrichment
                    List<VectorSimilarityService.SimilarityResult> resultObjects = similarities.stream()
                        .map(sim -> new VectorSimilarityService.SimilarityResult(
                            (String) sim.get("entityUri"),
                            (String) sim.get("entityType"),
                            (String) sim.get("metricType"),
                            (Integer) sim.get("orderId"),
                            (Double) sim.get("ratingValue"),
                            (Double) sim.get("similarity"),
                            (Integer) sim.get("characterLength")
                        ))
                        .collect(Collectors.toList());
                    
                    // Enrich with raw data
                    List<Map<String, Object>> enrichedSimilarities = similarityService.enrichSimilarityResultsWithRawData(resultObjects);
                    enrichedResults.put("similarities", enrichedSimilarities);
                }
                
                similarityResults = enrichedResults;
            }
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "orderId", request.orderId(),
                "entityUri", request.entityUri(),
                "entityType", request.entityType(),
                "metricName", request.metricName() != null ? request.metricName() : "all",
                "limit", request.limit(),
                "raw", Boolean.TRUE.equals(request.raw()),
                "results", similarityResults
            ));
            
        } catch (Exception e) {
            log.error("Error calculating advanced similarity for order {}", request.orderId(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "message", "Failed to calculate advanced similarity: " + e.getMessage(),
                    "orderId", request.orderId()
                ));
        }
    }

    /**
     * Get entity embeddings from database
     */
    @PostMapping("/embeddings")
    public ResponseEntity<Map<String, Object>> getEntityEmbeddings(
            @RequestBody EmbeddingRequest request) {
        try {
            log.info("Getting entity embeddings for order: {}", request.orderId());
            
            if (!storageService.orderExists(request.orderId())) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "success", false,
                        "message", String.format("Order ID %d does not exist in the database", request.orderId()),
                        "orderId", request.orderId()
                    ));
            }
            
            
            Map<String, Object> embeddingResults = similarityService.getEntityEmbeddings(
                request.orderId(), request.metricName());
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "orderId", request.orderId(),
                "metricName", request.metricName() != null ? request.metricName() : "all",
                "results", embeddingResults
            ));
            
        } catch (Exception e) {
            log.error("Error getting entity embeddings for order {}", request.orderId(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "message", "Failed to get entity embeddings: " + e.getMessage(),
                    "orderId", request.orderId()
                ));
        }
    }

    /**
     * Get embedding approach information
     */
    @GetMapping("/strategies")
    public ResponseEntity<Map<String, Object>> getEmbeddingStrategies() {
        return ResponseEntity.ok(Map.of(
            "success", true,
            "approach", "text-based",
            "description", "Uses metric-relevant text content (commit messages, issue titles/bodies, comment text) for similarity calculation and semantic content quality evaluation.",
            "useCases", List.of("Content quality metrics", "Clarity assessment", "Semantic similarity", "Expert matching", "Multi-faceted similarity evaluation")
        ));
    }
    
    /**
     * Get available similarity metrics and filtering options
     */
    @GetMapping("/similarity/options")
    public ResponseEntity<Map<String, Object>> getSimilarityOptions() {
        // Document similarity metrics (PostgreSQL pgvector native operators)
        Map<String, String> similarityMetrics = Map.of(
            "cosine", "Cosine distance (<=> operator) - measures angle between vectors (best for semantic similarity)",
            "euclidean", "Euclidean distance (<-> operator) - straight-line distance (sensitive to magnitude)",
            "dot_product", "Dot product (<#> operator) - measures alignment and magnitude (negative inner product)"
        );
        
        // Document filtering options
        Map<String, Object> filteringOptions = Map.of(
            "singleMetric", "Use 'metricName' field for backward compatibility",
            "multipleMetrics", "Use 'metricNames' array to search across multiple metrics",
            "allMetrics", "Omit both 'metricName' and 'metricNames' to search all stored embeddings"
        );
        
        // Document scale types
        Map<String, String> scaleTypes = Map.of(
            "best", "Return highest-rated similar entities (default)",
            "worst", "Return lowest-rated similar entities",
            "center", "Return mixed ratings around average scores"
        );
        
        // Document fallback strategies
        Map<String, String> fallbackStrategies = Map.of(
            "best", "Return highest-rated entities when no similarity results found",
            "worst", "Return lowest-rated entities when no similarity results found", 
            "center", "Return mixed ratings from center range when no similarity results found"
        );
        
        return ResponseEntity.ok(Map.of(
            "similarityMetrics", similarityMetrics,
            "filteringOptions", filteringOptions,
            "scaleTypes", scaleTypes,
            "fallbackStrategies", fallbackStrategies,
            "examples", Map.of(
                "singleMetric", Map.of(
                    "metricName", "issueBodyClarityScore",
                    "similarityMetric", "cosine",
                    "fallbackStrategy", "best"
                ),
                "multipleMetrics", Map.of(
                    "metricNames", java.util.List.of("issueBodyClarityScore", "issueTitleClarityScore"),
                    "similarityMetric", "euclidean",
                    "fallbackStrategy", "center"
                ),
                "exploratorySearch", Map.of(
                    "metricNames", java.util.List.of(), // empty array = search all
                    "similarityMetric", "dot_product",
                    "limit", 20,
                    "fallbackStrategy", "best"
                ),
                "ratingBasedSearch", Map.of(
                    "entityType", "issue", // entityUri omitted = rating-based search
                    "metricNames", java.util.List.of("issueBodyClarityScore"),
                    "scaleType", "best",
                    "limit", 15
                )
            )
        ));
    }



    // Request DTOs
    public record AdvancedSimilarityRequest(
            Integer orderId,        // Required: order ID containing expert ratings
            String entityUri,       // Optional: specific entity to find similar entities for (if null, uses rating-based search)
            String entityType,      // Required: entity type (commit, issue, pull_request, comment)
            String metricName,      // Optional - if null, uses all metrics (DEPRECATED - use metricNames instead)
            java.util.List<String> metricNames,  // Optional - list of metrics to filter by, if null/empty uses all metrics
            Integer limit,          // Optional - defaults to 10
            Boolean raw,            // Optional - if true, fetch full entity properties via query service
            String scaleType,       // Optional - "best", "worst", "center", or "randomcenter" - defaults to "best"
            Double similarityThreshold,  // Optional - minimum similarity score (0.0-1.0) - defaults to system default
            String similarityMetric, // Optional - "cosine", "euclidean", "dot_product" - defaults to "cosine"
            String fallbackStrategy, // Optional - "best", "worst", "center", or "randomcenter" - defaults to "center" when no similarity results found
            String semanticModel    // Optional - ignored (system uses configured embedding model)
    ) {
        public AdvancedSimilarityRequest {
            RequestValidator.validateRequired(orderId, "orderId", "to identify the data set");
            RequestValidator.validateRequired(entityType, "entityType", "to filter relevant entities");
            RequestValidator.validatePositiveInteger(limit, "limit", "result count");
            
            if (limit == null || limit <= 0) {
                limit = 10;
            }
            // Smart validation with detailed error messages  
            RequestValidator.validateScaleType(scaleType, "scaleType");
            RequestValidator.validateScaleType(fallbackStrategy, "fallbackStrategy");
            
            // Validate similarity metric
            if (similarityMetric != null && !similarityMetric.trim().isEmpty()) {
                try {
                    SimilarityMetric.fromString(similarityMetric);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid similarity metric: " + e.getMessage());
                }
            }
            
            // Validate metricNames list (if provided)
            if (metricNames != null && metricNames.contains(null)) {
                throw new IllegalArgumentException("metricNames list cannot contain null values");
            }
            
            // Support backward compatibility: if metricName is provided but metricNames is null/empty,
            // convert single metricName to metricNames list
            if (metricName != null && !metricName.trim().isEmpty() && 
                (metricNames == null || metricNames.isEmpty())) {
                metricNames = java.util.List.of(metricName.trim());
            }
        }
    }

    public record EmbeddingRequest(
            Integer orderId,
            String metricName  // Optional - if null, includes all metrics
    ) {
        public EmbeddingRequest {
            if (orderId == null) {
                throw new IllegalArgumentException("orderId is required");
            }
        }
    }

    /**
     * Check if embedding services are available for the specified model(s)
     */
    private void checkEmbeddingServiceAvailability(String modelKey) {
        log.info("Checking availability of embedding model: {}", embeddingModel.getModelId());
        batchEmbeddingService.checkEmbeddingServiceAvailability(embeddingModel);
        log.info("Embedding service is available");
    }

    /**
     * Get available embedding models
     */
    @GetMapping("/embeddings/models")
    public ResponseEntity<Map<String, Object>> getAvailableModels() {
        Map<String, Object> modelInfo = new HashMap<>();
        
        Map<String, Object> details = new HashMap<>();
        details.put("key", embeddingModel.getKey());
        details.put("modelId", embeddingModel.getModelId());
        details.put("dimensions", embeddingModel.getDimensions());
        details.put("description", embeddingModel.getDescription());
        modelInfo.put(embeddingModel.getKey(), details);
        
        return ResponseEntity.ok(Map.of(
            "success", true,
            "models", modelInfo,
            "availableKeys", EmbeddingModel.getAvailableKeys(),
            "defaultModel", embeddingModel.getKey()
        ));
    }

    /**
     * Process/generate embeddings for expert ratings
     * POST /api/expertise/embeddings/process
     * 
     * Request body properties:
     * - orderId (optional): if provided, processes embeddings only for that specific order; if null, processes all orders
     * - metricName (optional): if provided, filters entities by metric; if null, processes all metrics  
     * - wipeEmbeddings (optional): if true, wipes embeddings before processing (all if orderId is null, specific orderId if provided)
     * - model (optional): "fast" - only model available
     */
    @PostMapping("/embeddings/process")
    public ResponseEntity<Map<String, Object>> processEmbeddings(@RequestBody ProcessEmbeddingRequest request) {
        // Check if embedding service(s) are available before starting
        try {
            checkEmbeddingServiceAvailability(request.model());
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "success", false,
                    "error", "Embedding service not available",
                    "message", e.getMessage(),
                    "troubleshooting", "Please ensure the external embedding service is running and accessible",
                    "requestedModel", request.model() != null ? request.model() : "all",
                    "availableModels", EmbeddingModel.getAvailableKeys()
                ));
        }
        
        // Check if already processing
        if (processingStatusService.isCurrentlyProcessing()) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "success", false,
                    "message", "Processing already in progress. Check /api/expertise/embeddings/status for current status.",
                    "currentStatus", processingStatusService.getStatus().getStatus()
                ));
        }
        
        try {
            // Start async processing using CompletableFuture to ensure it runs asynchronously
            CompletableFuture.runAsync(() -> {
                try {
                    processEmbeddingsAsyncInternal(request);
                } catch (Exception e) {
                    log.error("Error in background embedding processing", e);
                    processingStatusService.failProcessing(e.getMessage());
                }
            });
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Processing started in background");
            response.put("statusEndpoint", "/api/expertise/embeddings/status");
            response.put("orderId", request.orderId() != null ? request.orderId() : "all");
            response.put("metricName", request.metricName() != null ? request.metricName() : "all");
            response.put("wipeEmbeddings", request.wipeEmbeddings() != null ? request.wipeEmbeddings() : false);
            response.put("model", request.model() != null ? request.model() : "all");
            response.put("availableModels", EmbeddingModel.getAvailableKeys());
            
            return ResponseEntity.accepted().body(response);
                
        } catch (Exception e) {
            log.error("Error starting embedding processing", e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "error", "Failed to start processing",
                    "message", e.getMessage()
                ));
        }
    }
    
    /**
     * Process embeddings for a specific order
     */
    private ResponseEntity<Map<String, Object>> processEmbeddingsForOrder(Integer orderId, String metricName) {
        if (!storageService.orderExists(orderId)) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "success", false,
                    "message", String.format("Order ID %d does not exist in the database", orderId),
                    "orderId", orderId
                ));
        }
        
        // Get expert-rated entities for the order
        Map<String, SparqlQueryService.EntityData> entities = 
            sparqlQueryService.getExpertRatedEntitiesWithContent(orderId);
        
        log.info("Retrieved {} entities from SPARQL query for order {}", entities.size(), orderId);
        
        if (entities.isEmpty()) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "success", false,
                    "message", String.format("No expert-rated entities found for order ID %d", orderId),
                    "orderId", orderId
                ));
        }
        
        // Filter and build context map
        log.info("Building entity context map from {} entities (metricName filter: {})", 
                entities.size(), metricName != null ? metricName : "none");
        
        Map<String, String> entityContextMap = buildEntityContextMap(entities, metricName);
        
        log.info("Entity context map results: {} entities have valid content after filtering", entityContextMap.size());
        
        // Log sample of what was filtered out if significant difference
        if (entities.size() > entityContextMap.size()) {
            int filteredOut = entities.size() - entityContextMap.size();
            log.info("Filtered out {} entities that had no valid content", filteredOut);
            
            // Sample debugging: show first few entities that were filtered out
            int debugCount = 0;
            for (SparqlQueryService.EntityData entity : entities.values()) {
                if (debugCount >= 3) break;
                
                // Check if this entity was filtered out
                if (entity.entityUri == null || !entityContextMap.containsKey(entity.entityUri)) {
                    String context = contextBuilderService.buildLexicalContext(entity);
                    String entityId;
                    
                    if (entity.entityUri == null) {
                        entityId = "null-uri-" + debugCount;
                    } else {
                        entityId = entity.entityUri.substring(entity.entityUri.lastIndexOf('/') + 1);
                        if (entityId.length() > 10) entityId = entityId.substring(0, 10);
                    }
                    
                    log.info("Filtered out entity {}: type='{}', context='{}', metricIds={}", 
                            entityId, entity.entityType, context.length() > 50 ? context.substring(0, 50) + "..." : context, 
                            entity.getMetricIds());
                    debugCount++;
                }
            }
        }
        
        if (entityContextMap.isEmpty()) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "success", false,
                    "message", metricName != null 
                        ? String.format("No entities found for metric '%s' in order ID %d", metricName, orderId)
                        : String.format("No entities with valid content found for order ID %d", orderId),
                    "orderId", orderId
                ));
        }
        
        log.info("Processing embeddings for {} entities (filtered from {} total) for order {}", 
                entityContextMap.size(), entities.size(), orderId);
        
        // Generate embeddings using BatchEmbeddingService with specified model
        Map<String, double[]> embeddings = batchEmbeddingService.generateEmbeddingsWithModel(
            entityContextMap, embeddingModel, orderId);
            
        // Store embeddings with metrics data
        storeEmbeddingsWithSpecificModel(embeddings, entities, orderId, metricName, embeddingModel);
        
        return ResponseEntity.ok(Map.of(
            "success", true,
            "orderId", orderId,
            "metricName", metricName != null ? metricName : "all",
            "message", String.format("Successfully processed %d embeddings for order %d", 
                    embeddings.size(), orderId),
            "processedEntities", embeddings.size(),
            "totalEntitiesFound", entities.size(),
            "filteredEntities", entityContextMap.size()
        ));
    }
    
    /**
     * Process embeddings for all orders that have expert ratings
     */
    private ResponseEntity<Map<String, Object>> processEmbeddingsForAllOrders(String metricName) {
        int totalProcessedEntities = 0;
        int totalEntitiesFound = 0;
        int processedOrders = 0;
        Map<Integer, Integer> orderResults = new HashMap<>();
        
        // Get actual order IDs with expert data from listener service
        List<Integer> orderIds = getOrdersWithExpertData();
        
        if (orderIds.isEmpty()) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "success", false,
                    "message", "No orders with expert data found from listener service"
                ));
        }
        
        log.info("Processing embeddings for {} orders with expert data: {}", orderIds.size(), orderIds);
        
        for (Integer orderId : orderIds) {
            try {
                if (!storageService.orderExists(orderId)) {
                    continue; // Skip if order doesn't exist
                }
                
                // Get expert-rated entities for this order
                Map<String, SparqlQueryService.EntityData> entities = 
                    sparqlQueryService.getExpertRatedEntitiesWithContent(orderId);
                
                if (entities.isEmpty()) {
                    continue; // Skip if no expert-rated entities
                }
                
                totalEntitiesFound += entities.size();
                
                // Filter and build context map for this order
                Map<String, String> entityContextMap = buildEntityContextMap(entities, metricName);
                
                if (!entityContextMap.isEmpty()) {
                    log.info("Processing {} entities for order {}", entityContextMap.size(), orderId);
                    
                    // Generate embeddings for this order with specified model
                    Map<String, double[]> embeddings = batchEmbeddingService.generateEmbeddingsWithModel(
                        entityContextMap, embeddingModel, orderId);
                        
                    // Store embeddings with metrics data
                    storeEmbeddingsWithSpecificModel(embeddings, entities, orderId, metricName, embeddingModel);
                    
                    totalProcessedEntities += embeddings.size();
                    orderResults.put(orderId, embeddings.size());
                    processedOrders++;
                } else {
                    log.debug("No valid entities found for order {} after filtering", orderId);
                }
                
            } catch (Exception e) {
                log.debug("Error processing order {}: {}", orderId, e.getMessage());
                // Continue with next order
            }
        }
        
        if (processedOrders == 0) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "success", false,
                    "message", "No expert-rated entities found in any order"
                ));
        }
        
        return ResponseEntity.ok(Map.of(
            "success", true,
            "metricName", metricName != null ? metricName : "all",
            "message", String.format("Successfully processed %d embeddings across %d orders", 
                    totalProcessedEntities, processedOrders),
            "processedEntities", totalProcessedEntities,
            "totalEntitiesFound", totalEntitiesFound,
            "processedOrders", processedOrders,
            "orderResults", orderResults
        ));
    }
    
    /**
     * Build entity context map with optional metric filtering
     */
    private Map<String, String> buildEntityContextMap(Map<String, SparqlQueryService.EntityData> entities, String metricName) {
        Map<String, String> entityContextMap = new HashMap<>();
        int processedCount = 0;
        int metricFilteredOut = 0;
        int emptyContextFiltered = 0;
        
        for (SparqlQueryService.EntityData entity : entities.values()) {
            processedCount++;
            
            // Filter by metric if specified
            if (metricName != null && !entity.getMetricIds().contains(metricName)) {
                metricFilteredOut++;
                continue;
            }
            
            // Build lexical context for the entity
            String context = contextBuilderService.buildLexicalContext(entity);
            
            // Check for null entity URI
            if (entity.entityUri == null) {
                log.error("Entity has null URI: type='{}', context='{}', metricIds={}", 
                        entity.entityType, context, entity.getMetricIds());
                emptyContextFiltered++;
                continue;
            }
            
            if (!context.trim().isEmpty()) {
                entityContextMap.put(entity.entityUri, context);
            } else {
                emptyContextFiltered++;
                // Empty context already logged as warning in buildEntityContext method
            }
        }
        
        log.info("Context map building stats: processed={}, metricFiltered={}, emptyContext={}, final={}", 
                processedCount, metricFilteredOut, emptyContextFiltered, entityContextMap.size());
        
        return entityContextMap;
    }
    
    /**
     * Get order IDs that have expert data from the listener service
     */
    private List<Integer> getOrdersWithExpertData() {
        try {
            String url = listenerServiceUrl + "/listener-service/api/v1/github/orders/with-experts";
            log.info("Fetching orders with expert data from: {}", url);
            
            ResponseEntity<List<Integer>> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<Integer>>() {}
            );
            
            List<Integer> orderIds = response.getBody();
            log.info("Found {} orders with expert data: {}", 
                    orderIds != null ? orderIds.size() : 0, orderIds);
            
            return orderIds != null ? orderIds : List.of();
            
        } catch (Exception e) {
            log.error("Failed to fetch orders with expert data from listener service: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch orders with expert data: " + e.getMessage(), e);
        }
    }
    
    /**
     * Truncate text to specified length with word boundary awareness
     */
    private String truncate(String text, int maxLength) {
        if (text == null || text.length() <= maxLength) return text;
        int lastSpace = text.substring(0, maxLength).lastIndexOf(' ');
        if (lastSpace > maxLength * 0.8) {
            return text.substring(0, lastSpace) + "...";
        }
        return text.substring(0, maxLength) + "...";
    }
    

    /**
     * Clear all embeddings from the database
     * DELETE /api/expertise/embeddings
     */
    @DeleteMapping("/embeddings")
    public ResponseEntity<Map<String, Object>> clearAllEmbeddings() {
        try {
            log.info("Clearing all embeddings from database");
            vectorSimilarityService.clearAllEmbeddings();
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Successfully cleared all embeddings from database"
            ));
            
        } catch (Exception e) {
            log.error("Error clearing all embeddings", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", "Failed to clear embeddings",
                "message", e.getMessage()
            ));
        }
    }

    /**
     * Clear embeddings for a specific orderId
     * DELETE /api/expertise/embeddings/{orderId}
     */
    @DeleteMapping("/embeddings/{orderId}")
    public ResponseEntity<Map<String, Object>> clearEmbeddingsByOrderId(@PathVariable Integer orderId) {
        try {
            log.info("Clearing embeddings for orderId: {}", orderId);
            vectorSimilarityService.clearEmbeddings(orderId);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", String.format("Successfully cleared embeddings for orderId: %d", orderId),
                "orderId", orderId
            ));
            
        } catch (Exception e) {
            log.error("Error clearing embeddings for orderId: {}", orderId, e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", "Failed to clear embeddings",
                "message", e.getMessage(),
                "orderId", orderId
            ));
        }
    }
    
    /**
     * Internal async processing method
     */
    private void processEmbeddingsAsyncInternal(ProcessEmbeddingRequest request) {
        try {
            String operation = request.orderId() != null ? 
                String.format("Processing order %d", request.orderId()) : 
                "Processing all orders";
            
            // Determine total orders to process
            List<Integer> orderIds;
            if (request.orderId() != null) {
                orderIds = List.of(request.orderId());
            } else {
                orderIds = getOrdersWithExpertData();
            }
            
            processingStatusService.startProcessing(operation, orderIds.size());
            
            // Handle wiping embeddings FIRST, before any processing
            if (Boolean.TRUE.equals(request.wipeEmbeddings())) {
                if (request.orderId() != null) {
                    log.info("Wiping embeddings for specific orderId: {} before processing", request.orderId());
                    vectorSimilarityService.clearEmbeddings(request.orderId());
                    log.info("Cleared embeddings for orderId: {}", request.orderId());
                } else {
                    log.info("Wiping ALL embeddings from database before processing");
                    vectorSimilarityService.clearAllEmbeddings();
                    log.info("Cleared all embeddings from database");
                }
            }
            
            // Process each order
            for (Integer orderId : orderIds) {
                processingStatusService.updateCurrentOrder(orderId);
                processEmbeddingsForOrderInternal(orderId, request.metricName(), request.model());
                processingStatusService.completeOrder(orderId);
            }
            
            processingStatusService.completeProcessing();
            
        } catch (Exception e) {
            log.error("Error in async embedding processing", e);
            processingStatusService.failProcessing(e.getMessage());
        }
    }
    
    /**
     * Internal processing method for a specific order (used by async processing)
     */
    private void processEmbeddingsForOrderInternal(Integer orderId, String metricName, String modelType) {
        if (!storageService.orderExists(orderId)) {
            throw new RuntimeException(String.format("Order ID %d does not exist in the database", orderId));
        }
        
        // Get expert-rated entities for the order
        Map<String, SparqlQueryService.EntityData> entities = 
            sparqlQueryService.getExpertRatedEntitiesWithContent(orderId);
        
        log.info("Retrieved {} entities from SPARQL query for order {}", entities.size(), orderId);
        
        if (entities.isEmpty()) {
            throw new RuntimeException(String.format("No expert-rated entities found for order ID %d", orderId));
        }
        
        // Filter and build context map
        log.info("Building entity context map from {} entities (metricName filter: {})", 
                entities.size(), metricName != null ? metricName : "none");
        
        Map<String, String> entityContextMap = buildEntityContextMap(entities, metricName);
        
        log.info("Entity context map results: {} entities have valid content after filtering", entityContextMap.size());
        
        if (entityContextMap.isEmpty()) {
            throw new RuntimeException(metricName != null 
                ? String.format("No entities found for metric '%s' in order ID %d", metricName, orderId)
                : String.format("No entities with valid content found for order ID %d", orderId));
        }
        
        log.info("Processing embeddings for {} entities (filtered from {} total) for order {}", 
                entityContextMap.size(), entities.size(), orderId);
        
        // Generate embeddings using single model
        {
            // Generate embeddings with specific model
            int embeddingCount = generateEmbeddingsWithSpecificModel(entities, entityContextMap, orderId, metricName, modelType);
            log.info("Completed processing order {} with {} model - {} embeddings created from {} total entities", 
                    orderId, modelType, embeddingCount, entities.size());
        }
    }
    
    /**
     * Generate embeddings with a specific model and store
     */
    private int generateEmbeddingsWithSpecificModel(
            Map<String, SparqlQueryService.EntityData> entities, 
            Map<String, String> entityContextMap, 
            Integer orderId, 
            String metricName, 
            String modelType) {
        
        log.info("Generating embeddings with {} model for {} entities", embeddingModel.getModelId(), entityContextMap.size());
        
        BatchEmbeddingService.EmbeddingResult result = batchEmbeddingService.generateEmbeddingsWithModelAndLengths(
                entityContextMap, embeddingModel, orderId);
        
        // Store embeddings with metrics data and character lengths
        storeEmbeddingsWithSpecificModel(result.getEmbeddings(), entities, orderId, metricName, embeddingModel, result.getCharacterLengths());
        
        log.info("Successfully generated and stored {} embeddings with {} model", 
                result.getEmbeddings().size(), embeddingModel.getModelId());
        
        return result.getEmbeddings().size();
    }
    
    
    /**
     * Store embeddings generated with a specific model
     */
    private void storeEmbeddingsWithSpecificModel(
            Map<String, double[]> embeddings, 
            Map<String, SparqlQueryService.EntityData> entitiesData, 
            Integer orderId, 
            String metricNameFilter, 
            EmbeddingModel model) {
        storeEmbeddingsWithSpecificModel(embeddings, entitiesData, orderId, metricNameFilter, model, null);
    }
    
    /**
     * Store embeddings generated with a specific model and character lengths
     */
    private void storeEmbeddingsWithSpecificModel(
            Map<String, double[]> embeddings, 
            Map<String, SparqlQueryService.EntityData> entitiesData, 
            Integer orderId, 
            String metricNameFilter, 
            EmbeddingModel model,
            Map<String, Integer> characterLengths) {
        
        log.info("Storing {} model embeddings: {} embeddings", model.getKey(), embeddings.size());
        
        for (Map.Entry<String, double[]> embeddingEntry : embeddings.entrySet()) {
            String entityUri = embeddingEntry.getKey();
            double[] embedding = embeddingEntry.getValue();
            
            SparqlQueryService.EntityData entityData = entitiesData.get(entityUri);
            if (entityData == null) {
                log.warn("No entity data found for URI: {}", entityUri);
                continue;
            }
            
            Set<String> metricIds = entityData.getMetricIds();
            if (metricIds.isEmpty()) {
                log.warn("Entity {} has no metric IDs", entityUri);
                metricIds = Set.of("general"); // Default metric
            }
            
            // Store embedding for each metric this entity is associated with
            for (String metricId : metricIds) {
                // Apply metric filter if specified
                if (metricNameFilter != null && !metricNameFilter.equals(metricId)) {
                    continue;
                }
                
                Double rating = entityData.getRatingForMetric(metricId);
                String entityType = entityData.entityType;
                
                // Get character length for this entity
                Integer characterLength = characterLengths != null ? characterLengths.get(entityUri) : null;
                
                // Store embedding for this specific model
                storeEntityEmbeddingWithSpecificModel(entityUri, orderId, metricId, rating, entityType, embedding, model, characterLength);
            }
        }
        
        log.info("Successfully stored {} model embeddings for {} entities", model.getKey(), embeddings.size());
    }
    
    
    /**
     * Store a single entity's embedding with a specific model
     */
    private void storeEntityEmbeddingWithSpecificModel(
            String entityUri, Integer orderId, String metricId, Double rating, String entityType,
            double[] embedding, EmbeddingModel model) {
        storeEntityEmbeddingWithSpecificModel(entityUri, orderId, metricId, rating, entityType, embedding, model, null);
    }
    
    /**
     * Store a single entity's embedding with a specific model and character length
     */
    private void storeEntityEmbeddingWithSpecificModel(
            String entityUri, Integer orderId, String metricId, Double rating, String entityType,
            double[] embedding, EmbeddingModel model, Integer characterLength) {
        
        // Store embedding in the specific model column
        vectorSimilarityService.storeEntityEmbeddingWithSpecificModel(
                entityUri, orderId, entityType, metricId, rating, "text-based",
                embedding, model, characterLength);
    }
    
    /**
     * Get processing status
     */
    @GetMapping("/embeddings/status")
    public ResponseEntity<Map<String, Object>> getProcessingStatus() {
        try {
            ProcessingStatusService.ProcessingStatus status = processingStatusService.getStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            
            Map<String, Object> processing = new HashMap<>();
            processing.put("isProcessing", status.isProcessing());
            processing.put("operation", status.operation() != null ? status.operation() : "None");
            processing.put("status", status.getStatus() != null ? status.getStatus() : "Unknown");
            
            Map<String, Object> progress = new HashMap<>();
            progress.put("totalOrders", status.totalOrders());
            progress.put("processedOrders", status.processedOrders());
            progress.put("currentOrderId", status.currentOrderId());
            progress.put("percentageComplete", Math.round(status.getProgressPercentage() * 100) / 100.0);
            processing.put("progress", progress);
            
            Map<String, Object> timing = new HashMap<>();
            timing.put("startTime", status.startTime());
            timing.put("endTime", status.endTime());
            processing.put("timing", timing);
            
            processing.put("lastError", status.lastError());
            response.put("processing", processing);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error getting processing status", e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "success", false,
                    "error", "Failed to get processing status",
                    "message", e.getMessage()
                ));
        }
    }

    /**
     * Generate comprehensive word count and metrics statistics
     * GET /api/v2/metrics/statistics
     */
    @GetMapping("/metrics/statistics")
    public ResponseEntity<Map<String, Object>> getMetricsStatistics(
            @RequestParam(required = false) List<Integer> orderIds,
            @RequestParam(required = false) List<String> entityTypes) {
        
        try {
            log.info("Generating metrics statistics for orders: {} and entity types: {}", orderIds, entityTypes);
            
            // If no orderIds provided, get all available orders
            if (orderIds == null || orderIds.isEmpty()) {
                orderIds = storageService.getAllOrderIds();
                if (orderIds.isEmpty()) {
                    return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "message", "No orders found in the database",
                        "availableOrders", List.of()
                    ));
                }
            }
            
            // Convert entity type strings to EntityType enums
            List<EntityType> entityTypeEnums = new ArrayList<>();
            if (entityTypes != null && !entityTypes.isEmpty()) {
                for (String entityTypeStr : entityTypes) {
                    try {
                        entityTypeEnums.add(EntityType.valueOf(entityTypeStr.toUpperCase()));
                    } catch (IllegalArgumentException e) {
                        return ResponseEntity.badRequest().body(Map.of(
                            "success", false,
                            "message", "Invalid entity type: " + entityTypeStr,
                            "validEntityTypes", Arrays.stream(EntityType.values())
                                .map(EntityType::name)
                                .collect(Collectors.toList())
                        ));
                    }
                }
            }
            
            // Generate comprehensive metrics report
            MetricsStatisticsService.MetricsStatisticsReport report = 
                metricsStatisticsService.generateComprehensiveReport(orderIds, entityTypeEnums);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "report", report,
                "summary", Map.of(
                    "totalOrders", report.getOrderIds().size(),
                    "totalEntities", report.getSummaryInsights().getTotalEntitiesAnalyzed(),
                    "totalMetrics", report.getSummaryInsights().getTotalMetricsFound(),
                    "executionTimeMs", report.getExecutionTimeMs(),
                    "generatedAt", report.getGeneratedAt()
                )
            ));
            
        } catch (Exception e) {
            log.error("Error generating metrics statistics", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", "Failed to generate metrics statistics",
                "message", e.getMessage()
            ));
        }
    }

    /**
     * Get summary of available data for metrics statistics
     * GET /api/v2/metrics/statistics/summary
     */
    @GetMapping("/metrics/statistics/summary")
    public ResponseEntity<Map<String, Object>> getMetricsStatisticsSummary() {
        try {
            // Get all available orders
            List<Integer> allOrderIds = storageService.getAllOrderIds();
            
            // Get available entity types (from enum)
            List<String> availableEntityTypes = Arrays.stream(EntityType.values())
                .map(EntityType::name)
                .collect(Collectors.toList());
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "availableOrders", allOrderIds,
                "totalOrders", allOrderIds.size(),
                "availableEntityTypes", availableEntityTypes,
                "usage", Map.of(
                    "endpoint", "/api/v2/metrics/statistics",
                    "parameters", Map.of(
                        "orderIds", "Optional list of order IDs (if empty, analyzes all orders)",
                        "entityTypes", "Optional list of entity types: " + String.join(", ", availableEntityTypes)
                    ),
                    "example", "/api/v2/metrics/statistics?orderIds=1,2,3&entityTypes=ISSUE,COMMIT"
                )
            ));
            
        } catch (Exception e) {
            log.error("Error getting metrics statistics summary", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", "Failed to get metrics statistics summary",
                "message", e.getMessage()
            ));
        }
    }


    public record ProcessEmbeddingRequest(
            Integer orderId,      // Optional - if null, processes all orders  
            String metricName,    // Optional - if null, processes all metrics
            Boolean wipeEmbeddings, // Optional - if true, wipes embeddings before processing
            String model          // Optional - "fast" - only model available
    ) {
        public ProcessEmbeddingRequest {
            // Model parameter is now informational only since we only support one configurable model
            // Validation removed as the model is configured via environment variables
        }
    }

}

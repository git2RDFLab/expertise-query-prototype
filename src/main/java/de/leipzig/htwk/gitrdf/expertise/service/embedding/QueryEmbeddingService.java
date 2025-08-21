package de.leipzig.htwk.gitrdf.expertise.service.embedding;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for generating embeddings for query entities on-the-fly via SPARQL
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class QueryEmbeddingService {

    private final SparqlQueryService sparqlQueryService;
    private final BatchEmbeddingService batchEmbeddingService;
    private final VectorSimilarityService vectorSimilarityService;

    /**
     * Find similar entities by generating embedding for query entity on-the-fly
     */
    public List<VectorSimilarityService.SimilarityResult> findSimilarEntitiesWithOnTheFlyEmbedding(
            String queryEntityUri, Integer queryOrderId, String metricType, int topK) {
        return findSimilarEntitiesWithOnTheFlyEmbedding(queryEntityUri, queryOrderId, metricType, topK, VectorSimilarityService.SortBy.SIMILARITY);
    }
    
    /**
     * Find similar entities by generating embedding for query entity on-the-fly with rating-based sorting
     */
    public List<VectorSimilarityService.SimilarityResult> findSimilarEntitiesWithOnTheFlyEmbedding(
            String queryEntityUri, Integer queryOrderId, String metricType, int topK, VectorSimilarityService.SortBy sortBy) {
        
        log.info("🔄 Generating on-the-fly embedding for query entity: {}", queryEntityUri);
        
        try {
            // 1. Fetch entity content via SPARQL
            SparqlQueryService.EntityData entityData = sparqlQueryService.getEntityContent(queryOrderId, queryEntityUri);
            
            if (entityData == null) {
                log.error(" Could not fetch content for entity: {}", queryEntityUri);
                return List.of();
            }
            
            // 2. Build semantic context from the entity data
            String semanticContext = entityData.buildSemanticContext();
            log.info("📝 Built semantic context for query entity ({} chars): '{}'", 
                    semanticContext.length(), 
                    semanticContext.length() > 150 ? semanticContext.substring(0, 150) + "..." : semanticContext);
            
            // 3. Generate embedding using external embedding service
            Map<String, String> contextMap = Map.of(queryEntityUri, semanticContext);
            Map<String, double[]> embeddings = batchEmbeddingService.generateEmbeddingsInBatches(contextMap, queryOrderId, false);
            
            if (embeddings.isEmpty() || !embeddings.containsKey(queryEntityUri)) {
                log.error(" Failed to generate embedding for query entity: {}", queryEntityUri);
                return List.of();
            }
            
            double[] queryEmbedding = embeddings.get(queryEntityUri);
            String queryVector = vectorSimilarityService.convertDoubleArrayToVectorString(queryEmbedding);
            
            log.info("Generated embedding for query entity (dimensions: {})", queryEmbedding.length);
            
            // 4. Search for similar entities using the generated embedding
            return vectorSimilarityService.findSimilarWithPgVectorFromQueryVector(
                    queryVector, queryEntityUri, queryOrderId, metricType, topK, sortBy);
                    
        } catch (Exception e) {
            log.error(" Error generating on-the-fly embedding for entity '{}': {}", queryEntityUri, e.getMessage());
            return List.of();
        }
    }
}
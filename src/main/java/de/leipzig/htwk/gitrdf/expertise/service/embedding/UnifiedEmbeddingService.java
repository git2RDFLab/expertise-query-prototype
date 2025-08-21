package de.leipzig.htwk.gitrdf.expertise.service.embedding;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.model.EmbeddingModel;
import de.leipzig.htwk.gitrdf.expertise.model.EntityType;
import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * CENTRALIZED EMBEDDING SERVICE
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class UnifiedEmbeddingService {
    
    private final BatchEmbeddingService batchEmbeddingService;
    private final SmartContextBuilder smartContextBuilder;
    

    /**
     * SINGLE ENTRY POINT: Generate embedding for any entity
     * 
     * @param entity - Entity data from SPARQL queries
     * @param orderId - Order context for debugging
     * @return Generated embedding vector
     */
    public double[] generateEmbedding(SparqlQueryService.EntityData entity, Integer orderId) {
        if (entity == null) {
            log.error("Cannot generate embedding: entity is null");
            throw new IllegalArgumentException("Entity cannot be null");
        }
        
        log.info("Generating embedding for {} (order: {})", entity.entityUri, orderId);
        
        try {
            // 1. Normalize entity type
            EntityType normalizedType = EntityType.fromString(entity.entityType);
            
            // 2. Build context using centralized strategy switching
            String context = buildContextForStrategy(entity, normalizedType);
            
            if (context == null || context.trim().isEmpty()) {
                log.error("Generated empty context for entity {} (type: {})", 
                         entity.entityUri, normalizedType);
                throw new RuntimeException("Empty context generated for entity: " + entity.entityUri);
            }
            
            log.info("Generated context for {}: '{}'", 
                    entity.entityUri, 
                    context.length() > 200 ? context.substring(0, 200) + "..." : context);
            
            // 3. Generate embedding via batch service
            Map<String, String> contextMap = Map.of(entity.entityUri, context);
            Map<String, double[]> embeddings = batchEmbeddingService.generateEmbeddingsInBatches(
                contextMap, orderId, false);
            
            double[] embedding = embeddings.get(entity.entityUri);
            if (embedding == null) {
                log.error("Batch service returned null embedding for entity: {}", entity.entityUri);
                throw new RuntimeException("Failed to generate embedding for entity: " + entity.entityUri);
            }
            
            log.info("Generated embedding for {} ({} dimensions)", 
                    entity.entityUri, embedding.length);
            
            return embedding;
            
        } catch (Exception e) {
            log.error("Failed to generate embedding for entity {}: {}", 
                     entity.entityUri, e.getMessage());
            throw new RuntimeException("Embedding generation failed for " + entity.entityUri, e);
        }
    }
    
    /**
     * Generate embeddings for multiple entities in batch
     */
    public Map<String, double[]> generateEmbeddings(
            Map<String, SparqlQueryService.EntityData> entities, 
            Integer orderId) {
        
        log.info("Generating embeddings for {} entities (order: {})", 
                entities.size(), orderId);
        
        // Build contexts for all entities
        Map<String, String> contextMap = new HashMap<>();
        for (Map.Entry<String, SparqlQueryService.EntityData> entry : entities.entrySet()) {
            String entityUri = entry.getKey();
            SparqlQueryService.EntityData entity = entry.getValue();
            
            try {
                EntityType normalizedType = EntityType.fromString(entity.entityType);
                String context = buildContextForStrategy(entity, normalizedType);
                
                if (context != null && !context.trim().isEmpty()) {
                    contextMap.put(entityUri, context);
                } else {
                    log.warn("Skipping entity {} due to empty context", entityUri);
                }
            } catch (Exception e) {
                log.error("Failed to build context for entity {}: {}", entityUri, e.getMessage());
                // Continue with other entities instead of failing entirely
            }
        }
        
        // Generate embeddings via batch service
        return batchEmbeddingService.generateEmbeddingsInBatches(contextMap, orderId, false);
    }
    
    
    /**
     * Generate embedding for a single property text (used by evaluation for vector averaging)
     */
    public double[] generateSinglePropertyEmbedding(String propertyText, EmbeddingModel model, Integer orderId) {
        if (propertyText == null || propertyText.trim().isEmpty()) {
            log.error("Cannot generate embedding: property text is null or empty");
            throw new IllegalArgumentException("Property text cannot be null or empty");
        }
        
        if (model == null) {
            log.error("Cannot generate embedding: model is null");
            throw new IllegalArgumentException("Model cannot be null");
        }
        
        try {
            String trimmedText = propertyText.trim();
            log.debug("Generating {} embedding for property text: '{}'", 
                    model.getKey(), 
                    trimmedText.length() > 100 ? trimmedText.substring(0, 100) + "..." : trimmedText);
            
            // Generate embedding via batch service for single text
            String uniqueKey = "property_" + System.currentTimeMillis() + "_" + Math.random();
            Map<String, String> contextMap = Map.of(uniqueKey, trimmedText);
            Map<String, double[]> embeddings = batchEmbeddingService.generateEmbeddingsWithModel(
                contextMap, model, orderId);
            
            double[] embedding = embeddings.get(uniqueKey);
            if (embedding == null) {
                log.error("Batch service returned null embedding for property text");
                throw new RuntimeException("Failed to generate embedding for property text");
            }
            
            log.debug("Generated {} embedding for property ({} dimensions)", 
                    model.getKey(), embedding.length);
            
            return embedding;
            
        } catch (Exception e) {
            log.error("Failed to generate {} embedding for property text: {}", 
                     model.getKey(), e.getMessage());
            throw new RuntimeException("Property embedding generation failed", e);
        }
    }
    
    /**
     * Generate embedding for entity with specific model (no custom weights)
     */
    public double[] generateEmbeddingWithModel(SparqlQueryService.EntityData entity, 
                                              EmbeddingModel model, 
                                              Integer orderId) {
        if (entity == null) {
            log.error("Cannot generate embedding: entity is null");
            throw new IllegalArgumentException("Entity cannot be null");
        }
        
        log.info("Generating embedding with {} model for {} (order: {})", 
                model.getKey(), entity.entityUri, orderId);
        
        try {
            // 1. Normalize entity type
            EntityType normalizedType = EntityType.fromString(entity.entityType);
            
            // 2. Build context using centralized strategy switching
            String context = buildContextForStrategy(entity, normalizedType);
            
            if (context == null || context.trim().isEmpty()) {
                log.error("Generated empty context for entity {} (type: {})", 
                         entity.entityUri, normalizedType);
                throw new RuntimeException("Empty context generated for entity: " + entity.entityUri);
            }
            
            log.info("Generated context for {}: '{}' - using {} model", 
                    entity.entityUri, 
                    context.length() > 200 ? context.substring(0, 200) + "..." : context,
                    model.getKey());
            
            // 3. Generate embedding with specific model via batch service
            Map<String, String> contextMap = Map.of(entity.entityUri, context);
            Map<String, double[]> embeddings = batchEmbeddingService.generateEmbeddingsWithModel(
                contextMap, model, orderId);
            
            double[] embedding = embeddings.get(entity.entityUri);
            if (embedding == null) {
                log.error("Batch service returned null embedding for entity: {}", entity.entityUri);
                throw new RuntimeException("Failed to generate embedding for entity: " + entity.entityUri);
            }
            
            log.info("Generated embedding with {} model for {} ({} dimensions)", 
                    model.getKey(), entity.entityUri, embedding.length);
            
            return embedding;
            
        } catch (Exception e) {
            log.error("Failed to generate embedding with {} model for entity {}: {}", 
                     model.getKey(), entity.entityUri, e.getMessage());
            throw new RuntimeException("Model-specific embedding generation failed for " + entity.entityUri, e);
        }
    }
    
    /**
     * Build lexical context for embedding generation
     */
    private String buildContextForStrategy(SparqlQueryService.EntityData entity, EntityType entityType) {
        return buildLexicalContext(entity, entityType);
    }
    
    /**
     * LEXICAL STRATEGY: Focus on textual content only with smart context building
     */
    private String buildLexicalContext(SparqlQueryService.EntityData entity, EntityType entityType) {
        // Use the new SmartContextBuilder for improved, bias-free context generation
        return smartContextBuilder.buildSmartContext(entity, entityType);
    }
    

}
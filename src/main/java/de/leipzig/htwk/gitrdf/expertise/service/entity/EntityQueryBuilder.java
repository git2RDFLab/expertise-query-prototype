package de.leipzig.htwk.gitrdf.expertise.service.entity;

import java.util.Map;

/**
 * Base interface for entity-specific SPARQL query builders
 * Simplified to only provide the 2 essential queries needed
 */
public interface EntityQueryBuilder {
    
    /**
     * Get the RDF type URI for this entity
     */
    String getRdfType();
    
    /**
     * 1. Build SPARQL query for /query-expert endpoint
     * Fetches entities that have expert ratings with their rating values and core properties
     * Used when querying the SPARQL server for expert-rated entities
     */
    String buildExpertRatedEntitiesQuery();
    
    /**
     * 2. Build SPARQL query to get a single entity's properties (without requiring expert ratings)
     * Used for similarity calculations where the entity may not have ratings
     * Queries against the regular /query endpoint, not /query-expert
     */
    String buildSingleEntityQuery(String entityUri);
    
    /**
     * Get property weights for semantic context building
     * Higher weights mean more important properties
     */
    Map<String, Double> getPropertyWeights();
    
    /**
     * Get metric-specific property weights for a given expert rating metric
     * Returns optimal property weights based on what the metric is measuring
     * 
     * @param metricName The expert rating metric (e.g., "issueTitleClarityScore")
     * @return Map of property weights optimized for this metric, or null if metric not supported
     * 
     * Examples:
     * - "issueTitleClarityScore" -> {title: 0.8, body: 0.2} (emphasize title)
     * - "issueBodyClarityScore" -> {title: 0.2, body: 0.8} (emphasize body)
     * - "commitMessageClarityScore" -> {message: 1.0} (pure message focus)
     */
    Map<String, Double> getMetricSpecificWeights(String metricName);
    
    /**
     * Extract semantic context from entity properties for embedding generation
     */
    String buildSemanticContext(Map<String, Object> properties);
}
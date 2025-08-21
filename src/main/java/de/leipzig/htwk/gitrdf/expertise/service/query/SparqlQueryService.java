package de.leipzig.htwk.gitrdf.expertise.service.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import de.leipzig.htwk.gitrdf.expertise.service.entity.EntityQueryBuilder;
import de.leipzig.htwk.gitrdf.expertise.service.entity.GitCommitQueryBuilder;
import de.leipzig.htwk.gitrdf.expertise.service.entity.GithubCommentQueryBuilder;
import de.leipzig.htwk.gitrdf.expertise.service.entity.GithubIssueQueryBuilder;
import de.leipzig.htwk.gitrdf.expertise.service.entity.GithubPullRequestQueryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for executing SPARQL queries against the query-service
 * to retrieve expert-rated entities with their semantic properties
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SparqlQueryService {

    @Value("${services.query.url:http://localhost:7080}")
    private String queryServiceUrl;

    private final RestTemplate restTemplate;
    private final GitCommitQueryBuilder gitCommitQueryBuilder;
    private final GithubIssueQueryBuilder githubIssueQueryBuilder;
    private final GithubPullRequestQueryBuilder githubPullRequestQueryBuilder;
    private final GithubCommentQueryBuilder githubCommentQueryBuilder;

    /**
     * Get entities of a specific type that have expert ratings
     * OPTIMIZED: Only queries the requested entity type
     */
    public Map<String, EntityData> getExpertRatedEntitiesWithContentForType(Integer orderId, de.leipzig.htwk.gitrdf.expertise.model.EntityType entityType) {
        log.info("🔍 Fetching expert-rated {} entities with content for order {}", entityType, orderId);
        
        Map<String, EntityData> entityDataMap = new HashMap<>();
        EntityQueryBuilder builder = getQueryBuilderForEntityType(entityType);
        
        String entityTypeName = extractSimpleTypeName(builder.getRdfType());
        log.info("🔍 Fetching {} entities...", entityTypeName);
        
        try {
            String sparqlQuery = builder.buildExpertRatedEntitiesQuery();
            List<Map<String, Object>> results = executeSelectQuery(orderId, sparqlQuery);
            
            log.info("📊 Found {} {} entities with ratings", results.size(), entityTypeName);
            
            for (Map<String, Object> row : results) {
                processEntityRow(row, entityTypeName, entityDataMap, builder);
            }
            
        } catch (Exception e) {
            log.error(" Error fetching {} entities for order {}: {}", entityTypeName, orderId, e.getMessage());
        }
        
        log.info(" Successfully loaded {} {} entities with content", entityDataMap.size(), entityTypeName);
        return entityDataMap;
    }
    
    /**
     * Process a single entity row from SPARQL results
     */
    private void processEntityRow(Map<String, Object> row, String entityTypeName, 
                                Map<String, EntityData> entityDataMap, EntityQueryBuilder builder) {
        // All SPARQL queries now consistently use ?entity as the variable name
        String entityUri = (String) row.get("entity");
        String metricId = (String) row.get("metricId");
        Object ratingValueObj = row.get("ratingValue");
        
        // Check for null entity URI first
        if (entityUri == null) {
            log.error(" SPARQL returned NULL entityUri for {} entity", entityTypeName);
            log.error(" Available row keys: {}", row.keySet());
            log.error(" Row data: {}", row);
            return;
        }
        
        // DEBUG: Check for null metricId
        if (metricId == null || metricId.trim().isEmpty()) {
            String entityId = entityUri.substring(entityUri.lastIndexOf('/') + 1);
            if (entityId.length() > 10) entityId = entityId.substring(0, 10);
            log.error(" SPARQL returned NULL metricId for entity {}: {}", entityId, entityUri);
            log.error(" SPARQL row data: {}", row);
            return;
        }
        
        // Parse rating value
        Double ratingValue = null;
        if (ratingValueObj != null) {
            try {
                ratingValue = Double.parseDouble(ratingValueObj.toString());
            } catch (NumberFormatException e) {
                log.warn("⚠️ Could not parse rating value '{}' for entity {} metric {}", ratingValueObj, entityUri, metricId);
            }
        }
        
        // Create or update entity data
        if (entityDataMap.containsKey(entityUri)) {
            EntityData existingData = entityDataMap.get(entityUri);
            existingData.addMetricRating(metricId, ratingValue);
        } else {
            // Use builder to create semantic context
            String semanticContext = builder.buildSemanticContext(row);
            EntityData entityData = new EntityData(entityUri, entityTypeName, row, semanticContext);
            entityData.addMetricRating(metricId, ratingValue);
            entityDataMap.put(entityUri, entityData);
        }
    }
    
    /**
     * Get the appropriate query builder for the entity type
     */
    private EntityQueryBuilder getQueryBuilderForEntityType(de.leipzig.htwk.gitrdf.expertise.model.EntityType entityType) {
        return switch (entityType) {
            case COMMIT -> gitCommitQueryBuilder;
            case ISSUE -> githubIssueQueryBuilder;
            case PULL_REQUEST -> githubPullRequestQueryBuilder;
            case COMMENT -> githubCommentQueryBuilder;
        };
    }

    /**
     * Get all entities that have expert ratings (analysis:hasRatingResult)
     * along with their semantic properties using entity-specific queries
     */
    public Map<String, EntityData> getExpertRatedEntitiesWithContent(Integer orderId) {
        log.info("🔍 Fetching expert-rated entities with content for order {}", orderId);
        
        Map<String, EntityData> entityDataMap = new HashMap<>();
        List<EntityQueryBuilder> queryBuilders = List.of(
            gitCommitQueryBuilder,
            githubIssueQueryBuilder, 
            githubPullRequestQueryBuilder,
            githubCommentQueryBuilder
        );

        for (EntityQueryBuilder builder : queryBuilders) {
            String entityTypeName = extractSimpleTypeName(builder.getRdfType());
            log.info("🔍 Fetching {} entities...", entityTypeName);
            
            try {
                String sparqlQuery = builder.buildExpertRatedEntitiesQuery();
                List<Map<String, Object>> results = executeSelectQuery(orderId, sparqlQuery);
                
                log.info("📊 Found {} {} entities with ratings", results.size(), entityTypeName);
                
                for (Map<String, Object> row : results) {
                    // All SPARQL queries now consistently use ?entity as the variable name
                    String entityUri = (String) row.get("entity");
                    String metricId = (String) row.get("metricId");
                    Object ratingValueObj = row.get("ratingValue");
                    
                    // Check for null entity URI first
                    if (entityUri == null) {
                        log.error(" SPARQL returned NULL entityUri for {} entity", entityTypeName);
                        log.error(" Available row keys: {}", row.keySet());
                        log.error(" Row data: {}", row);
                        continue;
                    }
                    
                    // DEBUG: Check for null metricId
                    if (metricId == null || metricId.trim().isEmpty()) {
                        String entityId = entityUri.substring(entityUri.lastIndexOf('/') + 1);
                        if (entityId.length() > 10) entityId = entityId.substring(0, 10);
                        log.error(" SPARQL returned NULL metricId for entity {}: {}", entityId, entityUri);
                        log.error(" SPARQL row data: {}", row);
                        continue;
                    }
                    
                    // Parse rating value
                    Double ratingValue = null;
                    if (ratingValueObj != null) {
                        try {
                            ratingValue = Double.parseDouble(ratingValueObj.toString());
                        } catch (NumberFormatException e) {
                            log.warn("⚠️ Could not parse rating value '{}' for entity {} metric {}", ratingValueObj, entityUri, metricId);
                        }
                    }
                    
                    // Create or update entity data
                    if (entityDataMap.containsKey(entityUri)) {
                        EntityData existingData = entityDataMap.get(entityUri);
                        existingData.addMetricRating(metricId, ratingValue);
                    } else {
                        // Use builder to create semantic context
                        String semanticContext = builder.buildSemanticContext(row);
                        EntityData entityData = new EntityData(entityUri, entityTypeName, row, semanticContext);
                        entityData.addMetricRating(metricId, ratingValue);
                        entityDataMap.put(entityUri, entityData);
                    }
                }
                
            } catch (Exception e) {
                log.error(" Failed to fetch {} entities: {}", entityTypeName, e.getMessage());
                // Continue with other entity types instead of failing completely
            }
        }
        
        log.info(" Retrieved {} expert-rated entities total across all types", entityDataMap.size());
        return entityDataMap;
    }

    /**
     * Get metric-relevant content for all expert-rated entities (for embedding computation)
     * Uses the buildExpertRatedEntitiesQuery method which fetches expert-rated entities with their properties
     */
    public Map<String, List<Map<String, Object>>> getMetricContentForAllEntityTypes(Integer orderId) {
        log.info("🔍 Fetching metric-relevant content for all entity types for order {}", orderId);
        
        Map<String, List<Map<String, Object>>> entityTypeContentMap = new HashMap<>();
        List<EntityQueryBuilder> queryBuilders = List.of(
            gitCommitQueryBuilder,
            githubIssueQueryBuilder, 
            githubPullRequestQueryBuilder,
            githubCommentQueryBuilder
        );

        for (EntityQueryBuilder builder : queryBuilders) {
            String entityTypeName = extractSimpleTypeName(builder.getRdfType());
            log.info("🔍 Fetching metric content for {} entities...", entityTypeName);
            
            try {
                String sparqlQuery = builder.buildExpertRatedEntitiesQuery();
                List<Map<String, Object>> results = executeSelectQuery(orderId, sparqlQuery);
                
                log.info("📊 Found {} {} entities with metric content", results.size(), entityTypeName);
                entityTypeContentMap.put(entityTypeName, results);
                
            } catch (Exception e) {
                log.error(" Failed to fetch metric content for {} entities: {}", entityTypeName, e.getMessage());
                entityTypeContentMap.put(entityTypeName, List.of()); // Empty list on error
            }
        }
        
        log.info(" Retrieved metric content for all entity types");
        return entityTypeContentMap;
    }

    /**
     * Execute a SELECT SPARQL query and return results as List of Maps
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> executeSelectQuery(Integer orderId, String sparqlQuery) {
        String url = queryServiceUrl + "/query-service/api/v1/github/rdf/query-expert/" + orderId;
        
        log.info("🔗 Executing SPARQL query POST to: {}", url);
        log.debug("📝 SPARQL Query: {}", sparqlQuery);
        
        // Set up headers for SPARQL query
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/sparql-query"));
        headers.setAccept(List.of(MediaType.valueOf("application/sparql-results+json")));
        
        // Create HTTP entity with SPARQL query as body
        HttpEntity<String> requestEntity = new HttpEntity<>(sparqlQuery, headers);
        
        try {
            ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                requestEntity,
                new ParameterizedTypeReference<Map<String, Object>>() {}
            );
            
            Map<String, Object> responseBody = response.getBody();
            if (responseBody != null && responseBody.containsKey("results")) {
                Map<String, Object> results = (Map<String, Object>) responseBody.get("results");
                if (results.containsKey("bindings")) {
                    List<Map<String, Object>> bindings = (List<Map<String, Object>>) results.get("bindings");
                    return extractBindingValues(bindings);
                }
            }
            
            log.warn("No results found in response: {}", responseBody);
            return List.of();
            
        } catch (Exception e) {
            log.error("SPARQL query failed for order {}: {}", orderId, e.getMessage());
            throw new RuntimeException("SPARQL query execution failed", e);
        }
    }

    /**
     * Extract values from SPARQL JSON result bindings
     */
    private List<Map<String, Object>> extractBindingValues(List<Map<String, Object>> bindings) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        for (Map<String, Object> binding : bindings) {
            Map<String, Object> row = new HashMap<>();
            
            for (Map.Entry<String, Object> entry : binding.entrySet()) {
                String varName = entry.getKey();
                Map<String, Object> valueObj = (Map<String, Object>) entry.getValue();
                Object value = valueObj.get("value");
                row.put(varName, value);
            }
            
            results.add(row);
        }
        
        return results;
    }

    /**
     * Extract simple entity type from full RDF type URI
     */
    private String extractEntityType(String typeUri) {
        if (typeUri == null) return "unknown";
        
        if (typeUri.contains("GitCommit")) return "commit";
        if (typeUri.contains("GithubIssue")) return "issue";
        if (typeUri.contains("GithubPullRequest")) return "pull_request";
        if (typeUri.contains("GithubComment")) return "comment";
        
        return "unknown";
    }
    
    /**
     * Extract simple type name from RDF type URI for logging
     */
    private String extractSimpleTypeName(String rdfTypeUri) {
        if (rdfTypeUri.contains("GitCommit")) return "GitCommit";
        if (rdfTypeUri.contains("GithubIssue")) return "GithubIssue";
        if (rdfTypeUri.contains("GithubPullRequest")) return "GithubPullRequest";
        if (rdfTypeUri.contains("GithubComment")) return "GithubComment";
        return "Unknown";
    }

    public EntityData getSingleEntityContent(Integer orderId, String entityUri, String entityType) {
        log.info("🔍 Fetching content for specific entity: {} (type: {})", entityUri, entityType);
        
        try {
            // Get the appropriate query builder based on entity type
            EntityQueryBuilder queryBuilder = getQueryBuilderForType(entityType);
            if (queryBuilder == null) {
                log.error(" Unknown entity type: {}", entityType);
                return null;
            }
            
            // Build single entity query
            String sparqlQuery = queryBuilder.buildSingleEntityQuery(entityUri);
            
            // Execute query against regular /query endpoint (not /query-expert)
            List<Map<String, Object>> results = executeSingleEntityQuery(orderId, sparqlQuery);
            
            if (results.isEmpty()) {
                log.warn("⚠️ No data found for entity {} of type {}", entityUri, entityType);
                return null;
            }
            
            // Use first result to build EntityData
            Map<String, Object> row = results.get(0);
            String semanticContext = queryBuilder.buildSemanticContext(row);
            String simpleTypeName = extractSimpleTypeName(queryBuilder.getRdfType());
            
            EntityData entityData = new EntityData(entityUri, simpleTypeName, row, semanticContext);
            
            log.info(" Successfully retrieved entity {} with context length: {}", entityUri, semanticContext.length());
            return entityData;
            
        } catch (Exception e) {
            log.error(" Failed to fetch single entity content for {}: {}", entityUri, e.getMessage());
            return null;
        }
    }
    

    public List<EntityData> queryEntitiesWithRatingsForOrder(Integer orderId) {
        log.info("🔍 Querying entities with ratings for order {}", orderId);
        
        Map<String, EntityData> entityDataMap = getExpertRatedEntitiesWithContent(orderId);
        return new ArrayList<>(entityDataMap.values());
    }
    
    /**
     * Load entities with metrics for embedding-based evaluation
     */
    public List<EntityData> loadEntitiesWithMetrics(Integer orderId, de.leipzig.htwk.gitrdf.expertise.model.EntityType entityType) {
        log.info("🔍 Loading {} entities with metrics for order {}", entityType, orderId);
        
        // FIXED: Only query the specific entity type instead of all types
        Map<String, EntityData> entityDataMap = getExpertRatedEntitiesWithContentForType(orderId, entityType);
        
        return new ArrayList<>(entityDataMap.values());
    }

    public EntityData getEntityContent(Integer orderId, String entityUri) {
        log.info("🔍 Fetching content for specific entity: {}", entityUri);
        
        try {
            // First try to get it from expert-rated entities (most comprehensive)
            Map<String, EntityData> expertRatedEntities = getExpertRatedEntitiesWithContent(orderId);
            if (expertRatedEntities.containsKey(entityUri)) {
                log.info(" Found entity {} in expert-rated entities", entityUri);
                return expertRatedEntities.get(entityUri);
            }
            
            log.warn("⚠️ Entity {} not found in expert-rated entities, it may not have ratings", entityUri);
            
            // If not found, we can't get content without buildEntityPropertiesQuery
            // This is expected since we removed that method as requested
            return null;
            
        } catch (Exception e) {
            log.error(" Failed to fetch entity content for {}: {}", entityUri, e.getMessage());
            return null;
        }
    }
    
    /**
     * Get the appropriate query builder for the given entity type
     */
    private EntityQueryBuilder getQueryBuilderForType(String entityType) {
        return switch (entityType.toLowerCase()) {
            case "gitcommit", "commit" -> gitCommitQueryBuilder;
            case "githubissue", "issue" -> githubIssueQueryBuilder;
            case "githubpullrequest", "pullrequest", "pull_request" -> githubPullRequestQueryBuilder;
            case "githubcomment", "comment" -> githubCommentQueryBuilder;
            default -> null;
        };
    }
    
    /**
     * Execute a SPARQL query against the regular /query endpoint (not /query-expert)
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> executeSingleEntityQuery(Integer orderId, String sparqlQuery) {
        String url = queryServiceUrl + "/query-service/api/v1/github/rdf/query/" + orderId;
        
        log.info("🔗 Executing single entity SPARQL query POST to: {}", url);
        log.debug("📝 SPARQL Query: {}", sparqlQuery);
        
        // Set up headers for SPARQL query
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/sparql-query"));
        headers.setAccept(List.of(MediaType.valueOf("application/sparql-results+json")));
        
        // Create HTTP entity with SPARQL query as body
        HttpEntity<String> requestEntity = new HttpEntity<>(sparqlQuery, headers);
        
        try {
            ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                requestEntity,
                new ParameterizedTypeReference<Map<String, Object>>() {}
            );
            
            Map<String, Object> responseBody = response.getBody();
            if (responseBody != null && responseBody.containsKey("results")) {
                Map<String, Object> results = (Map<String, Object>) responseBody.get("results");
                if (results.containsKey("bindings")) {
                    List<Map<String, Object>> bindings = (List<Map<String, Object>>) results.get("bindings");
                    return extractBindingValues(bindings);
                }
            }
            
            log.warn("No results found in response: {}", responseBody);
            return List.of();
            
        } catch (Exception e) {
            log.error("SPARQL single entity query failed for order {}: {}", orderId, e.getMessage());
            throw new RuntimeException("SPARQL single entity query execution failed", e);
        }
    }


    /**
     * Data class for entity semantic content
     */
    public static class EntityData {
        public final String entityUri;
        public final String entityType;
        public final String title;
        public final String body;
        public final String message;
        public final String commentBody;
        public final String precomputedSemanticContext;
        private final Set<String> metricIds = new HashSet<>();
        private final Map<String, Double> metricRatings = new HashMap<>();

        // Legacy constructor for backward compatibility
        public EntityData(String entityUri, String entityType, String title, String body, String message, String commentBody) {
            this.entityUri = entityUri;
            this.entityType = entityType;
            this.title = title;
            this.body = body;
            this.message = message;
            this.commentBody = commentBody;
            this.precomputedSemanticContext = null;
        }
        
        // New constructor for entity-specific data with precomputed semantic context
        public EntityData(String entityUri, String entityType, Map<String, Object> properties, String semanticContext) {
            this.entityUri = entityUri;
            this.entityType = entityType;
            this.title = (String) properties.get("title");
            this.body = (String) properties.get("body");
            this.message = (String) properties.get("message");
            // ONLY set commentBody if it exists in SPARQL results - NO FALLBACKS!
            this.commentBody = (String) properties.get("commentBody");
            this.precomputedSemanticContext = semanticContext;
        }
        
        public void addMetricId(String metricId) {
            if (metricId != null && !metricId.trim().isEmpty()) {
                this.metricIds.add(metricId);
            }
        }
        
        public void addMetricRating(String metricId, Double ratingValue) {
            if (metricId != null && !metricId.trim().isEmpty()) {
                this.metricIds.add(metricId);
                if (ratingValue != null) {
                    // Handle multiple ratings for same metric by averaging them
                    if (metricRatings.containsKey(metricId)) {
                        Double existingValue = metricRatings.get(metricId);
                        Double averageValue = (existingValue + ratingValue) / 2.0;
                        metricRatings.put(metricId, averageValue);
                    } else {
                        metricRatings.put(metricId, ratingValue);
                    }
                }
            }
        }
        
        public Set<String> getMetricIds() {
            return new HashSet<>(metricIds);
        }
        
        public String getPrimaryMetricId() {
            return metricIds.isEmpty() ? null : metricIds.iterator().next();
        }
        
        public Map<String, Double> getMetricRatings() {
            return new HashMap<>(metricRatings);
        }
        
        /**
         * Get entity ID for embedding-based evaluation
         */
        public String entityId() {
            return entityUri;
        }
        
        /**
         * Get all properties as a map for embedding-based evaluation
         */
        public Map<String, Object> properties() {
            Map<String, Object> props = new HashMap<>();
            if (title != null) props.put("title", title);
            if (body != null) props.put("body", body);  
            if (message != null) props.put("message", message);
            if (commentBody != null) props.put("commentBody", commentBody);
            
            // Add all metric ratings
            props.putAll(metricRatings);
            
            return props;
        }
        
        public Double getRatingForMetric(String metricId) {
            return metricRatings.get(metricId);
        }

        /**
         * Build semantic context from available content
         */
        public String buildSemanticContext() {
            // Use precomputed semantic context if available (from entity-specific builders)
            if (precomputedSemanticContext != null && !precomputedSemanticContext.trim().isEmpty()) {
                return precomputedSemanticContext;
            }
            
            // Fallback to legacy context building for backward compatibility
            List<String> parts = new ArrayList<>();
            
            // Add entity type and repository info
            parts.add(entityType);
            addRepositoryInfo(entityUri, parts);
            
            // Add semantic content based on type
            switch (entityType.toLowerCase()) {
                case "commit":
                    if (message != null && !message.trim().isEmpty()) {
                        parts.add("message: " + message);
                        parts.add(message); // Duplicate for emphasis
                    }
                    break;
                    
                case "issue":
                case "pull_request":
                    if (body != null && !body.trim().isEmpty()) {
                        parts.add("description: " + truncate(body, 300));
                        parts.add(truncate(body, 200)); // Duplicate for emphasis
                    }
                    if (title != null && !title.trim().isEmpty()) {
                        parts.add("title: " + title);
                    }
                    break;
                    
                case "comment":
                    if (commentBody != null && !commentBody.trim().isEmpty()) {
                        parts.add("comment: " + truncate(commentBody, 200));
                        parts.add(truncate(commentBody, 150)); // Duplicate for emphasis
                    }
                    break;
            }
            
            // Fallback if no semantic content
            if (parts.size() <= 2) {
                parts.add(entityType + " " + extractEntityId(entityUri));
            }
            
            return String.join(" ", parts);
        }

        private void addRepositoryInfo(String entityUri, List<String> parts) {
            if (entityUri.contains("github.com")) {
                String[] uriParts = entityUri.split("/");
                if (uriParts.length >= 5) {
                    parts.add("repository " + uriParts[3] + "/" + uriParts[4]);
                }
            }
        }

        private String extractEntityId(String entityUri) {
            if (entityUri.contains("/issues/")) {
                return "#" + entityUri.substring(entityUri.lastIndexOf("/") + 1);
            } else if (entityUri.contains("/pull/")) {
                return "PR#" + entityUri.substring(entityUri.lastIndexOf("/") + 1);
            } else if (entityUri.contains("/commit/")) {
                String hash = entityUri.substring(entityUri.lastIndexOf("/") + 1);
                return hash.length() > 7 ? hash.substring(0, 7) : hash;
            }
            return "";
        }

        private String truncate(String text, int maxLength) {
            if (text == null || text.length() <= maxLength) return text;
            int lastSpace = text.substring(0, maxLength).lastIndexOf(' ');
            if (lastSpace > maxLength * 0.8) {
                return text.substring(0, lastSpace) + "...";
            }
            return text.substring(0, maxLength) + "...";
        }
    }
}
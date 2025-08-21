package de.leipzig.htwk.gitrdf.expertise.service.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * SPARQL query builder for GitHub Comments
 * Key properties: platform:commentBody, platform:createdAt, platform:author
 */
@Component
public class GithubCommentQueryBuilder implements EntityQueryBuilder {

    @Override
    public String getRdfType() {
        return "https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#GithubComment";
    }

    @Override
    public String buildExpertRatedEntitiesQuery() {
        return """
            PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX git:      <https://purl.archive.org/git2rdf/v2/git2RDFLab-git#>
            PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
            PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
            PREFIX analysis: <https://purl.archive.org/git2rdf/v2/git2RDFLab-analysis#>
            SELECT DISTINCT ?entity ?metricId ?ratingValue ?commentBody ?author ?createdAt ?id ?commentLineLength
            WHERE {
                ?entity a github:GithubComment ;
                        analysis:hasRatingResult ?rating ;
                        platform:commentBody ?commentBody ;
                        platform:commentAuthor ?author ;
                        platform:commentedAt ?createdAt ;
                        platform:commentId ?id .
                ?rating analysis:metricId ?metricId ;
                        analysis:value ?ratingValue .
                BIND(STRLEN(STR(?commentBody)) AS ?commentLineLength)
            }
            ORDER BY ?entity ?metricId
            """;
    }
    
    @Override
    public String buildSingleEntityQuery(String entityUri) {
        return String.format("""
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX github: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
                    PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>

                    SELECT DISTINCT ?entity ?body ?author ?createdAt ?id ?commentLineLength
                    WHERE {
                        BIND(<%s> AS ?entity)
                        ?entity rdf:type github:GithubComment ;
                                platform:commentBody ?body ;
                                platform:commentAuthor ?author ;
                                platform:commentedAt ?createdAt ;
                                platform:commentId ?id .
                        BIND(STRLEN(?body) AS ?commentLineLength)
                    }
                """, entityUri);
    }
    
    public String fetchMultipleEntity() {
        return """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX github: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
                PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>

                SELECT DISTINCT ?entity ?body ?author ?createdAt ?id ?commentLineLength
                WHERE {
                    ?entity rdf:type github:GithubComment ;
                            platform:commentBody ?body ;
                            platform:commentAuthor ?author ;
                            platform:commentedAt ?createdAt ;
                            platform:commentId ?id .
                    BIND(STRLEN(?body) AS ?commentLineLength)
                }
                ORDER BY ?entity
                LIMIT 100
            """;
    }


    @Override
    public Map<String, Double> getPropertyWeights() {
        return Map.of(
            "commentBody", 3.0,  // Most important
            "authorName", 0.0,   // Author context
            "createdAt", 0.0     // Temporal context
        );
    }

    @Override
    public Map<String, Double> getMetricSpecificWeights(String metricName) {
        return switch (metricName) {
            case "commentConstructivenessScore" -> Map.of(
                "commentBody", 0.9,  // Primary content focus for constructiveness
                "author", 0.1        // Author context may indicate expertise/contribution patterns
            );
            
            case "commentToneAppropriatenessScore" -> Map.of(
                "commentBody", 1.0   // Pure content focus for tone assessment
            );
            
            default -> null; // Unsupported metric for comment entities
        };
    }

    @Override
    public String buildSemanticContext(Map<String, Object> properties) {
        // DEBUG: Log all available properties
        System.out.println("DEBUG GithubComment properties available: " + properties.keySet());
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            System.out.println("  " + entry.getKey() + " = " + (entry.getValue() != null ? "'" + entry.getValue() + "'" : "null"));
        }
        
        List<String> parts = new ArrayList<>();
        
        // Add entity type
        parts.add("comment");
        
        // Add repository info from entity URI if available
        String entityUri = (String) properties.get("entity");
        if (entityUri != null) {
            addRepositoryInfo(entityUri, parts);
        }
        
        // Add comment body (highest weight) - match SPARQL variable names
        String commentBody = (String) properties.get("body");  // Changed from "commentBody" to "body"
        if (commentBody != null && !commentBody.trim().isEmpty()) {
            String truncatedBody = truncate(commentBody, 250);
            parts.add("comment: " + truncatedBody);
            parts.add(truncate(commentBody, 150)); // Shorter version for emphasis
            parts.add(truncate(commentBody, 100)); // Even shorter for extra emphasis
        }
        
        // Add author context - match SPARQL variable names  
        String authorName = (String) properties.get("author");  // Changed from "authorName" to "author"
        if (authorName != null && !authorName.trim().isEmpty()) {
            parts.add("author: " + authorName);
        }
        
        // Add temporal context
        Object createdAt = properties.get("createdAt");
        if (createdAt != null) {
            parts.add("created: " + createdAt.toString());
        }
        
        // Fallback if no semantic content
        if (parts.size() <= 2) {
            parts.add("github comment");
        }
        
        String result = String.join(" ", parts);
        System.out.println("DEBUG GithubComment context parts: " + parts);
        System.out.println("DEBUG GithubComment final context: '" + result + "'");
        
        return result;
    }
    
    private void addRepositoryInfo(String entityUri, List<String> parts) {
        if (entityUri.contains("github.com")) {
            String[] uriParts = entityUri.split("/");
            if (uriParts.length >= 5) {
                parts.add("repository " + uriParts[3] + "/" + uriParts[4]);
            }
        }
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
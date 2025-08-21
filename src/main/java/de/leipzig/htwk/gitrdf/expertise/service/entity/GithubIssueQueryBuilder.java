package de.leipzig.htwk.gitrdf.expertise.service.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * SPARQL query builder for GitHub Issues
 * Key properties: platform:title, platform:body, platform:state, platform:labels
 */
@Component
public class GithubIssueQueryBuilder implements EntityQueryBuilder {

    @Override
    public String getRdfType() {
        return "https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#GithubIssue";
    }

    // @Override
    // public String buildExpertRatedEntitiesQuery() {
    //     return """
    //             PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    //             PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
    //             PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
    //             PREFIX analysis: <https://purl.archive.org/git2rdf/v2/git2RDFLab-analysis#>

    //             SELECT DISTINCT
    //                 ?entity ?metricId ?ratingValue
    //                 ?body ?title ?locked ?number ?state ?submitter
    //                 ?issueTitleLength ?issueBodyLength
    //             WHERE {
    //                 ?entity a github:GithubIssue ;
    //                         analysis:hasRatingResult ?rating ;
    //                         platform:body ?body ;
    //                         platform:title ?title ;
    //                         platform:locked ?locked ;
    //                         platform:number ?number ;
    //                         platform:state ?state ;
    //                         platform:submitter ?submitter .

    //                 ?rating analysis:metricId ?metricId ;
    //                         analysis:value ?ratingValue .

    //                 BIND(STRLEN(STR(?title)) AS ?issueTitleLength)
    //                 BIND(STRLEN(STR(?body))  AS ?issueBodyLength)
    //             }
    //             ORDER BY ?entity ?metricId

    //             """;
    // }
    @Override
    public String buildExpertRatedEntitiesQuery() {
        return """
            PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
            PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
            PREFIX analysis: <https://purl.archive.org/git2rdf/v2/git2RDFLab-analysis#>
            SELECT DISTINCT
                ?entity ?metricId ?ratingValue
                ?body ?title ?locked ?number ?state ?submitter
                ?issueTitleLength ?issueBodyLength
            WHERE {
                ?entity a ?type .
                VALUES ?type { github:GithubIssue github:GithubPullRequest }
                ?entity
                    analysis:hasRatingResult ?rating ;
                    platform:body ?body ;
                    platform:title ?title ;
                    platform:locked ?locked ;
                    platform:number ?number ;
                    platform:state ?state ;
                    platform:submitter ?submitter .
                ?rating
                    analysis:metricId ?metricId ;
                    analysis:value ?ratingValue .
                BIND(STRLEN(STR(?title)) AS ?issueTitleLength)
                BIND(STRLEN(STR(?body))  AS ?issueBodyLength)
            }
            ORDER BY ?entity ?metricId
            """;
    }
    
    // @Override
    // public String buildSingleEntityQuery(String entityUri) {
    //     return String.format("""
    //             PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    //             PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
    //             PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
    //             SELECT DISTINCT
    //                 ?entity ?body ?title ?locked ?number ?state ?submitter
    //                 ?issueTitleLength ?issueBodyLength
    //             WHERE {
    //                 BIND(<%s> AS ?entity)
    //                 ?entity a github:GithubIssue ;
    //                         platform:body ?body ;
    //                         platform:title ?title ;
    //                         platform:locked ?locked ;
    //                         platform:number ?number ;
    //                         platform:state ?state ;
    //                         platform:submitter ?submitter .
    //                 BIND(STRLEN(STR(?title)) AS ?issueTitleLength)
    //                 BIND(STRLEN(STR(?body))  AS ?issueBodyLength)
    //             }
    //             """, entityUri);
    // }

    @Override
    public String buildSingleEntityQuery(String entityUri) {
        return String.format("""
                PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
                PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
                SELECT DISTINCT
                    ?entity ?body ?title ?locked ?number ?state ?submitter
                    ?issueTitleLength ?issueBodyLength
                WHERE {
                    BIND(<%s> AS ?entity)
                    ?entity a ?type .
                    VALUES ?type { github:GithubIssue github:GithubPullRequest }
                    ?entity
                        platform:body ?body ;
                        platform:title ?title ;
                        platform:locked ?locked ;
                        platform:number ?number ;
                        platform:state ?state ;
                        platform:submitter ?submitter .
                    BIND(STRLEN(STR(?title)) AS ?issueTitleLength)
                    BIND(STRLEN(STR(?body))  AS ?issueBodyLength)
                }
                """, entityUri);
    }
    

    public String fetchMultipleEntity() {
        return """
                PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
                PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>

                SELECT DISTINCT
                    ?entity ?body ?title ?locked ?number ?state ?submitter
                    ?issueTitleLength ?issueBodyLength
                WHERE {
                    ?entity a ?type .
                    VALUES ?type { github:GithubIssue github:GithubPullRequest }
                    ?entity
                        platform:body ?body ;
                        platform:title ?title ;
                        platform:locked ?locked ;
                        platform:number ?number ;
                        platform:state ?state ;
                        platform:submitter ?submitter .
                    BIND(STRLEN(STR(?title)) AS ?issueTitleLength)
                    BIND(STRLEN(STR(?body))  AS ?issueBodyLength)
                }
                ORDER BY ?entity
                LIMIT 100
                """;
    }



    @Override
    public Map<String, Double> getPropertyWeights() {
        return Map.of(
            "title", 3.0,        // Most important - the issue title
            "body", 2.5,         // Very important - the issue description
            "authorName", 0.8,   // Author context
            "state", 0.5,        // Issue state (open/closed)
            "number", 0.3,       // Issue number
            "createdAt", 0.2     // Temporal context
        );
    }

    @Override
    public Map<String, Double> getMetricSpecificWeights(String metricName) {
        return switch (metricName) {
            case "issueTitleClarityScore" -> Map.of(
                "title", 0.8,  // Primary focus on title clarity
                "body", 0.2    // Body provides context for understanding title
            );
            
            case "issueBodyClarityScore" -> Map.of(
                "title", 0.2,  // Title provides context
                "body", 0.8    // Primary focus on body clarity and detail
            );
            
            case "issueBodyConstructivenessScore" -> Map.of(
                "title", 0.3,  // Title indicates issue type and approach
                "body", 0.7    // Body contains constructive problem description
            );
            
            case "issueReproducibilityScore" -> Map.of(
                "title", 0.2,  // Title provides context
                "body", 0.8    // Body contains reproduction steps and details
            );
            
            case "issueImpactAssessmentScore" -> Map.of(
                "title", 0.4,  // Title often indicates severity level
                "body", 0.6    // Body provides detailed impact description
            );
            
            case "issueBugOrFixRelated" -> Map.of(
                "title", 0.6,  // Title often contains keywords indicating bug/fix
                "body", 0.4    // Body provides supporting context for bug/fix classification
            );
            
            case "issueDocRelated" -> Map.of(
                "title", 0.7,  // Title often contains keywords indicating documentation
                "body", 0.3    // Body provides supporting context for documentation classification
            );
            
            case "issueFeatureRelated" -> Map.of(
                "title", 0.6,  // Title often contains keywords indicating feature request
                "body", 0.4    // Body provides detailed feature description
            );
            
            default -> null; // Unsupported metric for issue entities
        };
    }

    @Override
    public String buildSemanticContext(Map<String, Object> properties) {
        List<String> parts = new ArrayList<>();
        
        // Add entity type
        parts.add("issue");
        
        // Add repository info from entity URI if available
        String entityUri = (String) properties.get("entity");
        if (entityUri != null) {
            addRepositoryInfo(entityUri, parts);
        }
        
        // Add issue title (highest weight)
        String title = (String) properties.get("title");
        if (title != null && !title.trim().isEmpty()) {
            parts.add("title: " + title);
            parts.add(title); // Duplicate for emphasis
        }
        
        // Add issue body (high weight)
        String body = (String) properties.get("body");
        if (body != null && !body.trim().isEmpty()) {
            String truncatedBody = truncate(body, 300);
            parts.add("description: " + truncatedBody);
            parts.add(truncate(body, 200)); // Shorter version for emphasis
        }
        
        // Add author context
        String authorName = (String) properties.get("authorName");
        if (authorName != null && !authorName.trim().isEmpty()) {
            parts.add("author: " + authorName);
        }
        
        // Add state context
        String state = (String) properties.get("state");
        if (state != null && !state.trim().isEmpty()) {
            parts.add("state: " + state);
        }
        
        // Add issue number context
        Object number = properties.get("number");
        if (number != null) {
            parts.add("issue #" + number.toString());
        }
        
        // Fallback if no semantic content
        if (parts.size() <= 2) {
            if (number != null) {
                parts.add("github issue #" + number.toString());
            } else {
                parts.add("github issue");
            }
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
    
    private String truncate(String text, int maxLength) {
        if (text == null || text.length() <= maxLength) return text;
        int lastSpace = text.substring(0, maxLength).lastIndexOf(' ');
        if (lastSpace > maxLength * 0.8) {
            return text.substring(0, lastSpace) + "...";
        }
        return text.substring(0, maxLength) + "...";
    }
}
package de.leipzig.htwk.gitrdf.expertise.service.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * SPARQL query builder for GitHub Pull Requests
 * Key properties: platform:title, platform:body, platform:state, platform:mergeable
 */
@Component
public class GithubPullRequestQueryBuilder implements EntityQueryBuilder {

    @Override
    public String getRdfType() {
        return "https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#-github#GithubPullRequest";
    }

    @Override
    public String buildExpertRatedEntitiesQuery() {
        return """
            PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
            PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
            PREFIX analysis: <https://purl.archive.org/git2rdf/v2/git2RDFLab-analysis#>
            SELECT DISTINCT
                ?entity ?metricId ?ratingValue
                ?body ?title ?locked ?merged ?mergeCommitSha ?submitter ?number ?state
                ?issueTitleLength ?issueBodyLength
            WHERE {
                # Start with PR URI and its properties
                ?entity a github:GithubPullRequest ;
                        platform:body ?body ;
                        platform:title ?title ;
                        platform:locked ?locked ;
                        platform:merged ?merged ;
                        platform:mergeCommitSha ?mergeCommitSha ;
                        platform:submitter ?submitter ;
                        platform:number ?number ;
                        platform:state ?state .
                # If you truly start from an Issue IRI and need the PR IRI, keep this:
                BIND(IRI(REPLACE(STR(?entity), "/issues/", "/pull/")) AS ?pr)
                # Ratings attached to the PR IRI
                ?pr analysis:hasRatingResult ?rating .
                ?rating analysis:metricId ?metricId ;
                        analysis:value ?ratingValue .
                # Lengths (use STR() to handle possible language tags)
                BIND(STRLEN(STR(?title)) AS ?issueTitleLength)
                BIND(STRLEN(STR(?body))  AS ?issueBodyLength)
            }
            ORDER BY ?entity ?metricId
            """;
    }
    
    @Override
    public String buildSingleEntityQuery(String entityUri) {
        return String.format("""
                PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
                PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
                SELECT DISTINCT
                    ?entity ?body ?title ?locked ?merged ?mergeCommitSha ?submitter ?number ?state
                    ?issueTitleLength ?issueBodyLength
                WHERE {
                    BIND(<%s> AS ?entity)
                    ?entity a github:GithubPullRequest ;
                            platform:body ?body ;
                            platform:title ?title ;
                            platform:locked ?locked ;
                            platform:merged ?merged ;
                            platform:mergeCommitSha ?mergeCommitSha ;
                            platform:submitter ?submitter ;
                            platform:number ?number ;
                            platform:state ?state .
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
                ?entity ?body ?title ?locked ?merged ?mergeCommitSha ?submitter ?number ?state
                ?issueTitleLength ?issueBodyLength
            WHERE {
                ?entity a github:GithubPullRequest ;
                        platform:body ?body ;
                        platform:title ?title ;
                        platform:locked ?locked ;
                        platform:merged ?merged ;
                        platform:mergeCommitSha ?mergeCommitSha ;
                        platform:submitter ?submitter ;
                        platform:number ?number ;
                        platform:state ?state .
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
            "title", 3.0,        // Most important
            "body", 2.5,         // Very important 
            "authorName", 0.8,   // Author context
            "state", 0.6,        // PR state (open/closed/merged)
            "mergeable", 0.4,    // Technical mergeability status
            "number", 0.3,       // PR number
            "createdAt", 0.2     // Temporal context
        );
    }

    @Override
    public Map<String, Double> getMetricSpecificWeights(String metricName) {
        return switch (metricName) {
            case "pullRequestTitleClarityScore" -> Map.of(
                "title", 0.8,  // Primary focus on title clarity and descriptiveness
                "body", 0.2    // Body provides context for understanding title
            );
            
            case "pullRequestBodyClarityScore" -> Map.of(
                "title", 0.2,  // Title provides context
                "body", 0.8    // Primary focus on body clarity and implementation details
            );
            
            case "pullRequestDescriptionQualityScore" -> Map.of(
                "title", 0.3,  // Title indicates scope and purpose
                "body", 0.7    // Body contains comprehensive description of changes
            );
            
            default -> null; // Unsupported metric for pull request entities
        };
    }

    @Override
    public String buildSemanticContext(Map<String, Object> properties) {
        List<String> parts = new ArrayList<>();
        
        // Add entity type
        parts.add("pull request");
        
        // Add repository info from entity URI if available
        String entityUri = (String) properties.get("entity");
        if (entityUri != null) {
            addRepositoryInfo(entityUri, parts);
        }
        
        // Add PR title (highest weight)
        String title = (String) properties.get("title");
        if (title != null && !title.trim().isEmpty()) {
            parts.add("title: " + title);
            parts.add(title); // Duplicate for emphasis
        }
        
        // Add PR body (high weight)
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
        
        // Add mergeable status
        Object mergeable = properties.get("mergeable");
        if (mergeable != null) {
            parts.add("mergeable: " + mergeable.toString());
        }
        
        // Add PR number context
        Object number = properties.get("number");
        if (number != null) {
            parts.add("pull request #" + number.toString());
        }
        
        // Fallback if no semantic content
        if (parts.size() <= 2) {
            if (number != null) {
                parts.add("github pull request #" + number.toString());
            } else {
                parts.add("github pull request");
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
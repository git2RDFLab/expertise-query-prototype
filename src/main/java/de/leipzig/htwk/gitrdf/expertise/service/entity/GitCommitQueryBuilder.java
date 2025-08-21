package de.leipzig.htwk.gitrdf.expertise.service.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * SPARQL query builder for Git Commits
 * Key properties: git:message, git:author, git:committer, git:date
 */
@Component
public class GitCommitQueryBuilder implements EntityQueryBuilder {

    @Override
    public String getRdfType() {
        return "https://purl.archive.org/git2rdf/v2/git2RDFLab-git#GitCommit";
    }

    @Override
    public String buildExpertRatedEntitiesQuery() {
        return """
            PREFIX analysis: <https://purl.archive.org/git2rdf/v2/git2RDFLab-analysis#>
            PREFIX git:      <https://purl.archive.org/git2rdf/v2/git2RDFLab-git#>
            PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
            PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
            PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT
                ?entity ?metricId ?ratingValue
                ?message ?authorName ?authorEmail ?authorDate
                ?committerName ?committerEmail ?commitDate ?hash
                ?linkedIssueTitle
                (COUNT(DISTINCT ?diff) AS ?filesModified)
                (SUM(?linesChanged)    AS ?commitLinesModified)
            WHERE {
                ?entity a git:GitCommit ;
                        analysis:hasRatingResult ?rating ;
                        git:message ?message ;
                        git:hash ?hash ;
                        git:authorName ?authorName ;
                        git:authorEmail ?authorEmail ;
                        git:authorDate ?authorDate ;
                        git:committerName ?committerName ;
                        git:committerEmail ?committerEmail ;
                        git:commitDate ?commitDate .
                ?rating analysis:metricId ?metricId ;
                        analysis:value ?ratingValue .
                        
                # Optional linked GitHub issue title
                OPTIONAL {
                    ?entity github:mergedIntoIssue|github:partOfIssue ?linkedIssue .
                    ?linkedIssue platform:title ?linkedIssueTitle .
                }
                OPTIONAL {
                ?entity git:hasDiffEntry ?diff .
                OPTIONAL {
                    ?diff git:hasEdit ?edit .
                    ?edit git:oldLineStart ?oldStart ;
                        git:oldLineEnd   ?oldEnd ;
                        git:newLineStart ?newStart ;
                        git:newLineEnd   ?newEnd .
                    BIND( ( (ABS(?newEnd - ?newStart) + 1) + (ABS(?oldEnd - ?oldStart) + 1) ) AS ?linesChanged )
                }
                }
            }
            GROUP BY
                ?entity ?metricId ?ratingValue
                ?message ?authorName ?authorEmail ?authorDate
                ?committerName ?committerEmail ?commitDate ?hash
                ?linkedIssueTitle
            ORDER BY ?entity ?metricId
            """;
    }
    
    @Override
    public String buildSingleEntityQuery(String entityUri) {
        return String.format("""
                PREFIX git:      <https://purl.archive.org/git2rdf/v2/git2RDFLab-git#>
                PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
                PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
                PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT DISTINCT
                    ?entity ?message ?authorName ?authorEmail ?authorDate
                    ?committerName ?committerEmail ?commitDate ?hash
                    ?linkedIssueTitle
                    (COUNT(DISTINCT ?diff) AS ?filesModified)
                    (SUM(?linesChanged) AS ?commitLinesModified)
                WHERE {
                    BIND(<%s> AS ?entity)
                    ?entity a git:GitCommit ;
                            git:message ?message ;
                            git:hash ?hash ;
                            git:authorName ?authorName ;
                            git:authorEmail ?authorEmail ;
                            git:authorDate ?authorDate ;
                            git:committerName ?committerName ;
                            git:committerEmail ?committerEmail ;
                            git:commitDate ?commitDate .

                    # Optional linked GitHub issue title
                    OPTIONAL {
                        ?entity github:mergedIntoIssue|github:partOfIssue ?linkedIssue .
                        ?linkedIssue platform:title ?linkedIssueTitle .
                    }
                    OPTIONAL {
                    ?entity git:hasDiffEntry ?diff .
                    OPTIONAL {
                        ?diff git:hasEdit ?edit .
                        ?edit git:oldLineStart ?oldStart ;
                            git:oldLineEnd ?oldEnd ;
                            git:newLineStart ?newStart ;
                            git:newLineEnd ?newEnd .
                        BIND( (ABS(?newEnd - ?newStart) + 1) + (ABS(?oldEnd - ?oldStart) + 1) AS ?linesChanged )
                    }
                    }
                }
                GROUP BY ?entity ?message ?authorName ?authorEmail ?authorDate
                            ?committerName ?committerEmail ?commitDate ?hash
                            ?linkedIssueTitle
                """, entityUri);
    }

    public String fetchMultipleEntity() {
        return """
                PREFIX git:      <https://purl.archive.org/git2rdf/v2/git2RDFLab-git#>
                PREFIX github:   <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform-github#>
                PREFIX platform: <https://purl.archive.org/git2rdf/v2/git2RDFLab-platform#>
                PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

                SELECT DISTINCT
                    ?entity ?message ?authorName ?authorEmail ?authorDate
                    ?committerName ?committerEmail ?commitDate ?hash
                    ?linkedIssueTitle
                    (COUNT(DISTINCT ?diff) AS ?filesModified)
                    (SUM(?linesChanged) AS ?commitLinesModified)
                WHERE {
                    ?entity a git:GitCommit ;
                            git:message ?message ;
                            git:hash ?hash ;
                            git:authorName ?authorName ;
                            git:authorEmail ?authorEmail ;
                            git:authorDate ?authorDate ;
                            git:committerName ?committerName ;
                            git:committerEmail ?committerEmail ;
                            git:commitDate ?commitDate .

                    # Optional linked GitHub issue title
                    OPTIONAL {
                        ?entity github:mergedIntoIssue|github:partOfIssue ?linkedIssue .
                        ?linkedIssue platform:title ?linkedIssueTitle .
                    }
                    OPTIONAL {
                        ?entity git:hasDiffEntry ?diff .
                        OPTIONAL {
                            ?diff git:hasEdit ?edit .
                            ?edit git:oldLineStart ?oldStart ;
                                git:oldLineEnd ?oldEnd ;
                                git:newLineStart ?newStart ;
                                git:newLineEnd ?newEnd .
                            BIND( (ABS(?newEnd - ?newStart) + 1) + (ABS(?oldEnd - ?oldStart) + 1) AS ?linesChanged )
                        }
                    }
                }
                GROUP BY ?entity ?message ?authorName ?authorEmail ?authorDate
                         ?committerName ?committerEmail ?commitDate ?hash
                         ?linkedIssueTitle
                ORDER BY ?entity
                LIMIT 100
                """;
    }

    @Override
    public Map<String, Double> getPropertyWeights() {
        return Map.of(
            "message", 3.0,            
            "linkedIssueTitle", 2.0,    
            "authorName", 1.0,          
            "committerName", 0.5,       
            "date", 0.3,               
            "hash", 0.1                 
        );
    }

    @Override
    public Map<String, Double> getMetricSpecificWeights(String metricName) {
        return switch (metricName) {
            case "commitMessageClarityScore" -> Map.of(
                "message", 1.0  // Pure message focus for clarity assessment
            );
            
            case "commitAtomicityScore" -> Map.of(
                "message", 0.7,           // Message indicates scope and purpose
                "filesModified", 0.2,     // File count indicates change scope
                "commitLinesModified", 0.1 // Line count indicates change size
            );
            
            case "commitRationaleConstructivenessScore" -> Map.of(
                "message", 0.9,       // Message contains reasoning and justification
                "authorName", 0.1     // Author context may indicate expertise
            );
            
            case "commitResolutionClarityScore" -> Map.of(
                "message", 1.0        // Pure message focus for problem-solution clarity
            );
            
            case "commitRiskAssessmentScore" -> Map.of(
                "message", 0.6,           // Message describes changes and risks
                "filesModified", 0.2,     // Scope indicates potential risk
                "commitLinesModified", 0.2 // Size indicates potential impact
            );
            
            case "commitMessageClarityScoreWithStats" -> Map.of(
                "message", 1.0        // Pure message focus for clarity with statistics
            );
            
            case "commitResolutionClarityScoreWithStats" -> Map.of(
                "message", 1.0        // Pure message focus for resolution clarity with statistics
            );
            
            case "commitMessageFeatureRelated" -> Map.of(
                "message", 0.8,           // Message content for feature detection
                "linkedIssueTitle", 0.2   // Linked issue may indicate feature work
            );
            
            case "commitMessageDocRelated" -> Map.of(
                "message", 0.9,           // Message content for documentation detection
                "filesModified", 0.1      // File modifications may indicate doc changes
            );
            
            case "commitMessageFixRelated" -> Map.of(
                "message", 0.8,           // Message content for fix detection
                "linkedIssueTitle", 0.2   // Linked issue may indicate bug fix
            );
            
            default -> null; // Unsupported metric for commit entities
        };
    }

    @Override
    public String buildSemanticContext(Map<String, Object> properties) {
        List<String> parts = new ArrayList<>();
        
        // Add entity type
        parts.add("commit");
        
        // Add repository info from entity URI if available
        String entityUri = (String) properties.get("entity");
        if (entityUri != null) {
            addRepositoryInfo(entityUri, parts);
        }
        
        // Add commit message (highest weight)
        String message = (String) properties.get("message");
        if (message != null && !message.trim().isEmpty()) {
            parts.add("message: " + message);
            parts.add(message); // Duplicate for emphasis due to high weight
            parts.add(message); // Triple for maximum emphasis
        }
        
        // Add linked issue title context (high weight)
        String linkedIssueTitle = (String) properties.get("linkedIssueTitle");
        if (linkedIssueTitle != null && !linkedIssueTitle.trim().isEmpty()) {
            parts.add("related to issue: " + linkedIssueTitle);
            parts.add(linkedIssueTitle); // Duplicate for emphasis
        }
        
        // Add author context
        String authorName = (String) properties.get("authorName");
        if (authorName != null && !authorName.trim().isEmpty()) {
            parts.add("author: " + authorName);
        }
        
        // Add committer context if different from author
        String committerName = (String) properties.get("committerName");
        if (committerName != null && !committerName.trim().isEmpty() && 
            !committerName.equals(authorName)) {
            parts.add("committer: " + committerName);
        }
        
        // Add temporal context
        Object date = properties.get("date");
        if (date != null) {
            parts.add("date: " + date.toString());
        }
        
        // Fallback if no semantic content
        if (parts.size() <= 2) {
            String hash = (String) properties.get("hash");
            if (hash != null) {
                String shortHash = hash.length() > 7 ? hash.substring(0, 7) : hash;
                parts.add("commit " + shortHash);
            } else {
                parts.add("git commit");
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
}
package de.leipzig.htwk.gitrdf.expertise.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import lombok.extern.slf4j.Slf4j;

/**
 * Centralized service for building entity contexts consistently
 * across all embedding generation paths (stored and on-the-fly)
 */
@Service
@Slf4j
public class ContextBuilderService {
    
    /**
     * Build lexical context for an entity - SINGLE SOURCE OF TRUTH
     * Used by both stored embedding generation and on-the-fly query embedding generation
     */
    public String buildLexicalContext(SparqlQueryService.EntityData entity) {
        List<String> parts = new ArrayList<>();
        
        switch (entity.entityType.toLowerCase()) {
            case "commit", "gitcommit" -> {
                // Only message content matters for commit metrics
                if (entity.message != null && !entity.message.trim().isEmpty()) {
                    parts.add(entity.message);
                    parts.add(entity.message); // Duplicate for emphasis
                }
            }
            case "issue", "githubissue", "pull_request", "githubpullrequest" -> {
                // Only title and body matter for issue/PR metrics
                if (entity.title != null && !entity.title.trim().isEmpty()) {
                    parts.add(entity.title);
                    parts.add(entity.title); // Duplicate for emphasis
                }
                if (entity.body != null && !entity.body.trim().isEmpty()) {
                    String truncatedBody = truncate(entity.body, 300);
                    parts.add(truncatedBody);
                    parts.add(truncate(entity.body, 200)); // Shorter version
                }
            }
            case "comment", "githubcomment" -> {
                // Match GithubCommentQueryBuilder format EXACTLY
                parts.add("comment"); // Base context marker
                
                // Add repository information - match GithubCommentQueryBuilder format
                if (entity.entityUri != null) {
                    String repositoryName = extractRepositoryName(entity.entityUri);
                    if (repositoryName != null) {
                        parts.add("repository " + repositoryName);
                    }
                }
                
                // Add comment body (highest weight) - match GithubCommentQueryBuilder format
                if (entity.commentBody != null && !entity.commentBody.trim().isEmpty()) {
                    String truncatedBody = truncate(entity.commentBody, 250);
                    parts.add("comment: " + truncatedBody);
                    parts.add(truncate(entity.commentBody, 150)); // Shorter version for emphasis
                    parts.add(truncate(entity.commentBody, 100)); // Even shorter for extra emphasis
                }
                
                // Note: Author and timestamp information would need to be added to EntityData
                // for full consistency with GithubCommentQueryBuilder format
                // For now, we focus on comment body which is the most important
            }
            default -> {
                log.error(" Unknown entity type '{}' for entity: {}", entity.entityType, entity.entityUri);
            }
        }
        
        return String.join(" ", parts);
    }
    
    /**
     * Build structural context - NOT IMPLEMENTED
     */
    public String buildStructuralContext(SparqlQueryService.EntityData entity) {
        throw new UnsupportedOperationException("Structural context building not implemented - no fallbacks allowed");
    }
    
    /**
     * Build hybrid context - NOT IMPLEMENTED
     */
    public String buildHybridContext(SparqlQueryService.EntityData entity) {
        throw new UnsupportedOperationException("Hybrid context building not implemented - no fallbacks allowed");
    }
    
    /**
     * Extract repository name from entity URI - match GithubCommentQueryBuilder format
     */
    private String extractRepositoryName(String entityUri) {
        if (entityUri != null && entityUri.contains("github.com")) {
            String[] uriParts = entityUri.split("/");
            if (uriParts.length >= 5) {
                return uriParts[3] + "/" + uriParts[4];
            }
        }
        return null;
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
}
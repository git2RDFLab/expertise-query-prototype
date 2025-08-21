package de.leipzig.htwk.gitrdf.expertise.model;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Normalized entity types for the expertise service
 * Provides consistent naming and mapping from various input formats
 */
public enum EntityType {
    COMMIT("commit"),
    ISSUE("issue"), 
    PULL_REQUEST("pull_request"),
    COMMENT("comment");
    
    private final String normalizedName;
    
    EntityType(String normalizedName) {
        this.normalizedName = normalizedName;
    }
    
    public String getNormalizedName() {
        return normalizedName;
    }
    
    /**
     * Convert various input formats to normalized entity type
     */
    public static EntityType fromString(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity type cannot be null or empty");
        }
        
        String normalized = input.toLowerCase().trim();
        
        // Handle various naming conventions
        return switch (normalized) {
            // Commit variants
            case "commit", "gitcommit", "git_commit" -> COMMIT;
            
            // Issue variants  
            case "issue", "githubissue", "github_issue", "gitlabissue", "gitlab_issue" -> ISSUE;
            
            // Pull request variants
            case "pull_request", "pullrequest", "pr", "githubpullrequest", "github_pull_request", 
                 "merge_request", "mergerequest", "mr", "gitlabmergerequest", "gitlab_merge_request" -> PULL_REQUEST;
            
            // Comment variants
            case "comment", "githubcomment", "github_comment", "issuecomment", "issue_comment",
                 "prcomment", "pr_comment", "pullrequestcomment", "pull_request_comment" -> COMMENT;
            
            default -> throw new IllegalArgumentException("Unsupported entity type: " + input + 
                ". Supported types: " + getSupportedTypes());
        };
    }
    
    /**
     * Get all supported entity type strings for error messages
     */
    public static String getSupportedTypes() {
        return Arrays.stream(EntityType.values())
            .map(EntityType::getNormalizedName)
            .collect(Collectors.joining(", "));
    }
    
    /**
     * Get all normalized names as a set for validation
     */
    public static Set<String> getAllNormalizedNames() {
        return Arrays.stream(EntityType.values())
            .map(EntityType::getNormalizedName)
            .collect(Collectors.toSet());
    }
    
    /**
     * Validate if a string represents a supported entity type
     */
    public static boolean isSupported(String input) {
        try {
            fromString(input);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
    
    /**
     * Convert URI to entity type (fallback method for legacy compatibility)
     */
    public static EntityType fromUri(String entityUri) {
        if (entityUri == null || entityUri.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity URI cannot be null or empty");
        }
        
        // GitHub-specific patterns (can be extended for other platforms)
        if (entityUri.contains("/commit/")) return COMMIT;
        if (entityUri.contains("#issuecomment-")) return COMMENT;
        if (entityUri.contains("/issues/") && !entityUri.contains("#")) return ISSUE;
        if (entityUri.contains("/pull/")) return PULL_REQUEST;
        
        // GitLab patterns (examples for future extension)
        if (entityUri.contains("/-/merge_requests/")) return PULL_REQUEST;
        if (entityUri.contains("/-/issues/")) return ISSUE;
        
        throw new IllegalArgumentException("Cannot determine entity type from URI: " + entityUri);
    }
    
    @Override
    public String toString() {
        return normalizedName;
    }
}
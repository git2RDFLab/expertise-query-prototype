package de.leipzig.htwk.gitrdf.expertise.service.embedding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.model.EntityType;
import de.leipzig.htwk.gitrdf.expertise.service.query.SparqlQueryService;
import lombok.extern.slf4j.Slf4j;

/**
 * Context Builder
 *
 */
@Service
@Slf4j
public class SmartContextBuilder {
    
    // Sentence boundary pattern for smart truncation
    private static final Pattern SENTENCE_BOUNDARY = Pattern.compile("[.!?]+\\s+");

    public String buildSmartContext(SparqlQueryService.EntityData entity, EntityType entityType) {
        return buildSmartContextWithWeights(entity, entityType, null);
    }

    public String buildSmartContextWithWeights(SparqlQueryService.EntityData entity, EntityType entityType, Map<String, Double> propertyWeights) {
        return switch (entityType) {
            case COMMIT -> buildCommitContextWithWeights(entity, propertyWeights);
            case ISSUE -> buildIssueContextWithWeights(entity, propertyWeights);
            case PULL_REQUEST -> buildPullRequestContextWithWeights(entity, propertyWeights);
            case COMMENT -> buildCommentContextWithWeights(entity, propertyWeights);
        };
    }
    
    

    private String buildCommitContextWithWeights(SparqlQueryService.EntityData entity, Map<String, Double> propertyWeights) {
        List<WeightedText> parts = new ArrayList<>();
        
        // Default weights if none provided
        Map<String, Double> weights = propertyWeights != null ? propertyWeights : getDefaultCommitWeights();
        
        // Commit message (PRIMARY) - Available as 'message' field
        if (entity.message != null && !entity.message.trim().isEmpty()) {
            double weight = weights.getOrDefault("commitMessage", 4.0);
            String truncatedMessage = smartTruncate(entity.message, 1200, true);
            parts.add(new WeightedText(truncatedMessage, weight, "message"));
        }
        
        // Repository context (SECONDARY) - extracted from entity URI
        String repositoryInfo = extractRepositoryInfo(entity.entityUri);
        if (repositoryInfo != null) {
            double weight = weights.getOrDefault("repositoryContext", 1.5);
            parts.add(new WeightedText(repositoryInfo, weight, "repository"));
        }
        

        return buildWeightedContext(parts, 450);
    }
    
   
    private Map<String, Double> getDefaultCommitWeights() {
        Map<String, Double> defaults = new HashMap<>();
        defaults.put("commitMessage", 4.0);        // Primary content
        defaults.put("repositoryContext", 1.5);    // Repository context
        // Note: Other properties (authorName, authorEmail, hash, filesModified, linesChanged) 
        // are not available in current EntityData but are prepared for future enhancement
        return defaults;
    }
    
    
    /**
     * Build context for issue entities with CUSTOM property weights  
     * Properties: issueTitle, issueBody, repositoryContext, labels, assignee, milestone
     */
    private String buildIssueContextWithWeights(SparqlQueryService.EntityData entity, Map<String, Double> propertyWeights) {
        List<WeightedText> parts = new ArrayList<>();
        
        // Default weights if none provided
        Map<String, Double> weights = propertyWeights != null ? propertyWeights : getDefaultIssueWeights();
        
        // Issue title (PRIMARY)
        if (entity.title != null && !entity.title.trim().isEmpty()) {
            double weight = weights.getOrDefault("issueTitle", 4.0);
            parts.add(new WeightedText(entity.title.trim(), weight, "title"));
        }
        
        // Issue body (PRIMARY)
        if (entity.body != null && !entity.body.trim().isEmpty()) {
            double weight = weights.getOrDefault("issueBody", 3.0);
            String truncatedBody = smartTruncate(entity.body, 1000, true);
            parts.add(new WeightedText(truncatedBody, weight, "body"));
        }
        
        // Repository context (SECONDARY) - extracted from entity URI
        String repositoryInfo = extractRepositoryInfo(entity.entityUri);
        if (repositoryInfo != null) {
            double weight = weights.getOrDefault("repositoryContext", 1.5);
            parts.add(new WeightedText(repositoryInfo, weight, "repository"));
        }
        
        
        return buildWeightedContext(parts, 500);
    }
    
   
    private Map<String, Double> getDefaultIssueWeights() {
        Map<String, Double> defaults = new HashMap<>();
        defaults.put("issueTitle", 4.0);         // Primary content
        defaults.put("issueBody", 3.0);          // Primary content
        defaults.put("repositoryContext", 1.5);  // Repository context
        defaults.put("labels", 2.0);            // Classification context
        defaults.put("assignee", 1.0);          // Workflow context
        defaults.put("milestone", 1.0);         // Planning context
        return defaults;
    }
    
    

    private String buildPullRequestContextWithWeights(SparqlQueryService.EntityData entity, Map<String, Double> propertyWeights) {
        List<WeightedText> parts = new ArrayList<>();
        
        // Default weights if none provided
        Map<String, Double> weights = propertyWeights != null ? propertyWeights : getDefaultPullRequestWeights();
        
        // PR title (PRIMARY) - same as issue title
        if (entity.title != null && !entity.title.trim().isEmpty()) {
            double weight = weights.getOrDefault("prTitle", 4.0);
            parts.add(new WeightedText(entity.title.trim(), weight, "title"));
        }
        
        // PR body (PRIMARY) - same as issue body
        if (entity.body != null && !entity.body.trim().isEmpty()) {
            double weight = weights.getOrDefault("prBody", 3.0);
            String truncatedBody = smartTruncate(entity.body, 1000, true);
            parts.add(new WeightedText(truncatedBody, weight, "body"));
        }
        
        // Repository context (SECONDARY) - extracted from entity URI
        String repositoryInfo = extractRepositoryInfo(entity.entityUri);
        if (repositoryInfo != null) {
            double weight = weights.getOrDefault("repositoryContext", 1.5);
            parts.add(new WeightedText(repositoryInfo, weight, "repository"));
        }
        
        // Note: Future features like target branch, source branch can be added here
        
        return buildWeightedContext(parts, 500);
    }
    
   
    private Map<String, Double> getDefaultPullRequestWeights() {
        Map<String, Double> defaults = new HashMap<>();
        defaults.put("prTitle", 4.0);            // Primary content
        defaults.put("prBody", 3.0);             // Primary content
        defaults.put("repositoryContext", 1.5);  // Repository context
        defaults.put("targetBranch", 1.0);       // Merge context
        defaults.put("sourceBranch", 0.5);       // Development context
        defaults.put("changedFiles", 1.0);       // Change scope
        return defaults;
    }
    

    private String buildCommentContextWithWeights(SparqlQueryService.EntityData entity, Map<String, Double> propertyWeights) {
        List<WeightedText> parts = new ArrayList<>();
        
        // Default weights if none provided
        Map<String, Double> weights = propertyWeights != null ? propertyWeights : getDefaultCommentWeights();
        
        // Repository context (SECONDARY)
        String repositoryInfo = extractRepositoryInfo(entity.entityUri);
        if (repositoryInfo != null) {
            double weight = weights.getOrDefault("repositoryContext", 1.0);
            parts.add(new WeightedText(repositoryInfo, weight, "repository"));
        }
        
        // Comment body (PRIMARY)
        if (entity.commentBody != null && !entity.commentBody.trim().isEmpty()) {
            double weight = weights.getOrDefault("commentBody", 5.0);
            String truncatedBody = smartTruncate(entity.commentBody, 1200, true);
            parts.add(new WeightedText(truncatedBody, weight, "comment"));
        }
        
        return buildWeightedContext(parts, 400);
    }
    
   
    private Map<String, Double> getDefaultCommentWeights() {
        Map<String, Double> defaults = new HashMap<>();
        defaults.put("commentBody", 5.0);         // Primary content
        defaults.put("repositoryContext", 1.0);  // Repository context
        return defaults;
    }
    
    /**
     * Smart truncation that preserves sentence boundaries and important keywords
     * 
     * @param text Input text to truncate
     * @param maxLength Maximum character length
     * @param preserveSentences Whether to preserve complete sentences
     * @return Truncated text with semantic awareness
     */
    private String smartTruncate(String text, int maxLength, boolean preserveSentences) {
        if (text == null || text.length() <= maxLength) {
            return text;
        }
        
        if (!preserveSentences) {
            // Simple word-boundary truncation
            return wordBoundaryTruncate(text, maxLength);
        }
        
        // Sentence-aware truncation
        List<String> sentences = Arrays.asList(SENTENCE_BOUNDARY.split(text + " "));
        StringBuilder result = new StringBuilder();
        
        for (String sentence : sentences) {
            String trimmedSentence = sentence.trim();
            if (trimmedSentence.isEmpty()) continue;
            
            // Check if adding this sentence would exceed the limit
            String candidateResult = result.length() == 0 ? 
                trimmedSentence : 
                result + ". " + trimmedSentence;
                
            if (candidateResult.length() <= maxLength) {
                if (result.length() > 0) {
                    result.append(". ");
                }
                result.append(trimmedSentence);
            } else {
                // If we can't fit the whole sentence, try to fit part of it
                if (result.length() == 0) {
                    // First sentence is too long, truncate it at word boundary
                    return wordBoundaryTruncate(trimmedSentence, maxLength);
                }
                break;
            }
        }
        
        String finalResult = result.toString();
        
        // Ensure we don't return empty string
        if (finalResult.trim().isEmpty()) {
            return wordBoundaryTruncate(text, maxLength);
        }
        
        return finalResult;
    }
    
    /**
     * Truncate at word boundary while preserving important words
     */
    private String wordBoundaryTruncate(String text, int maxLength) {
        if (text.length() <= maxLength) {
            return text;
        }
        
        // Find the last space before the limit
        int lastSpace = text.substring(0, maxLength).lastIndexOf(' ');
        
        if (lastSpace > maxLength * 0.7) { // At least 70% of desired length
            return text.substring(0, lastSpace) + "...";
        }
        
        // If no good word boundary, truncate at character level
        return text.substring(0, maxLength - 3) + "...";
    }
    
    /**
     * Build weighted context by combining multiple text parts with importance weights
     * Uses space-efficient concatenation instead of duplication
     */
    private String buildWeightedContext(List<WeightedText> parts, int maxTotalLength) {
        if (parts.isEmpty()) {
            return "";
        }
        
        // Sort by weight (highest first) to prioritize important content
        parts.sort((a, b) -> Double.compare(b.weight, a.weight));
        
        StringBuilder context = new StringBuilder();
        int remainingLength = maxTotalLength;
        
        for (WeightedText part : parts) {
            if (remainingLength <= 10) break; // Need some minimum space
            
            String separator = context.length() > 0 ? " " : "";
            String prefix = generateWeightedPrefix(part);
            
            // Calculate available space for this part
            int prefixAndSeparatorLength = separator.length() + prefix.length();
            int availableSpace = remainingLength - prefixAndSeparatorLength;
            
            if (availableSpace <= 10) continue; // Not enough space
            
            // Truncate the part if necessary
            String truncatedText = part.text.length() <= availableSpace ? 
                part.text : 
                wordBoundaryTruncate(part.text, availableSpace);
            
            // Append to context
            context.append(separator).append(prefix).append(truncatedText);
            remainingLength -= (separator.length() + prefix.length() + truncatedText.length());
        }
        
        return context.toString();
    }
    

    private String generateWeightedPrefix(WeightedText part) {
        // High weight fields get minimal prefixes to save space
        if (part.weight >= 0.8) {
            return ""; // No prefix for high-priority content
        } else if (part.weight >= 0.5) {
            return part.fieldType + ": ";
        } else {
            return "[" + part.fieldType + "] ";
        }
    }

    private String extractRepositoryInfo(String entityUri) {
        if (entityUri == null || !entityUri.contains("github.com")) {
            return null;
        }
        
        String[] parts = entityUri.split("/");
        if (parts.length >= 5) {
            return parts[3] + "/" + parts[4]; // owner/repo
        }
        
        return null;
    }
    


    private static class WeightedText {
        final String text;
        final double weight;
        final String fieldType;
        
        WeightedText(String text, double weight, String fieldType) {
            this.text = text;
            this.weight = weight;
            this.fieldType = fieldType;
        }
        
        @Override
        public String toString() {
            return String.format("WeightedText{weight=%.2f, fieldType='%s', text='%s...'}", 
                weight, fieldType, text.substring(0, Math.min(30, text.length())));
        }
    }
}
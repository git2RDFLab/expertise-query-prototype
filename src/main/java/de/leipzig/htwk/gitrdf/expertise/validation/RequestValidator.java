package de.leipzig.htwk.gitrdf.expertise.validation;

import de.leipzig.htwk.gitrdf.expertise.exception.RequestValidationException;
import de.leipzig.htwk.gitrdf.expertise.exception.RequestValidationException.ValidationError;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Smart validation utility using best practices
 */
@Component
@Slf4j
public class RequestValidator {
    
    /**
     * Validate semantic model parameter
     */
    public static void validateSemanticModel(String semanticModel, String fieldName) {
        List<ValidationError> errors = new ArrayList<>();
        
        if (semanticModel == null || semanticModel.trim().isEmpty()) {
            errors.add(new ValidationError(
                fieldName,
                semanticModel,
                "Semantic model is required for dual-model architecture",
                List.of("fast", "quality"),
                "Use 'fast' for 384-dimensional embeddings (faster, lower quality) or 'quality' for 768-dimensional embeddings (slower, higher quality)"
            ));
        } else {
            String normalizedModel = semanticModel.trim().toLowerCase();
            if (!List.of("fast", "quality").contains(normalizedModel)) {
                errors.add(new ValidationError(
                    fieldName,
                    semanticModel,
                    String.format("Invalid semantic model '%s'", semanticModel),
                    List.of("fast", "quality"),
                    String.format("Did you mean '%s'?", 
                        normalizedModel.startsWith("f") ? "fast" : 
                        normalizedModel.startsWith("q") ? "quality" : 
                        "fast or quality")
                ));
            }
        }
        
        if (!errors.isEmpty()) {
            throw new RequestValidationException("similarity calculation", errors, Map.of(
                "semanticModel", "fast",
                "explanation", "Use 'fast' for quick results with 384D embeddings, 'quality' for better results with 768D embeddings"
            ));
        }
    }
    
    /**
     * Validate similarity threshold
     */
    public static void validateSimilarityThreshold(Double threshold, String fieldName) {
        if (threshold == null) return; // Optional parameter
        
        List<ValidationError> errors = new ArrayList<>();
        
        if (threshold < 0.0 || threshold > 1.0) {
            errors.add(new ValidationError(
                fieldName,
                threshold,
                String.format("Similarity threshold %.3f is out of valid range", threshold),
                List.of("0.0 to 1.0"),
                String.format("Use %.1f for more results or %.1f for fewer, higher-quality results", 
                    threshold > 1.0 ? 0.8 : Math.min(0.9, threshold + 0.3),
                    threshold < 0.0 ? 0.2 : Math.max(0.1, threshold - 0.3))
            ));
        }
        
        if (!errors.isEmpty()) {
            throw new RequestValidationException("similarity threshold validation", errors, Map.of(
                "similarityThreshold", 0.7,
                "explanation", "Values closer to 1.0 return fewer, more similar results. Values closer to 0.0 return more, less similar results."
            ));
        }
    }
    
    /**
     * Validate scale type
     */
    public static void validateScaleType(String scaleType, String fieldName) {
        if (scaleType == null || scaleType.trim().isEmpty()) return; // Optional
        
        List<ValidationError> errors = new ArrayList<>();
        List<String> validScales = List.of("best", "worst", "center", "randomcenter");
        
        String normalized = scaleType.trim().toLowerCase();
        if (!validScales.contains(normalized)) {
            String suggestion = findClosestMatch(normalized, validScales);
            errors.add(new ValidationError(
                fieldName,
                scaleType,
                String.format("Invalid scale type '%s'", scaleType),
                validScales,
                String.format("Did you mean '%s'?", suggestion)
            ));
        }
        
        if (!errors.isEmpty()) {
            throw new RequestValidationException("scale type validation", errors, Map.of(
                "scaleType", "best",
                "explanation", Map.of(
                    "best", "Returns highest-rated examples",
                    "worst", "Returns lowest-rated examples", 
                    "center", "Returns balanced mix (good + bad examples)",
                    "randomcenter", "Returns random selection from best/undecided/worst rating groups"
                )
            ));
        }
    }
    
    /**
     * Validate required string field
     */
    public static void validateRequired(Object value, String fieldName, String purpose) {
        if (value == null || (value instanceof String str && str.trim().isEmpty())) {
            throw new RequestValidationException("required field validation", List.of(
                new ValidationError(
                    fieldName,
                    value,
                    String.format("%s is required %s", fieldName, purpose),
                    null,
                    String.format("Provide a valid %s value", fieldName)
                )
            ));
        }
    }
    
    /**
     * Validate positive integer
     */
    public static void validatePositiveInteger(Integer value, String fieldName, String purpose) {
        if (value == null) return; // Assume optional
        
        if (value <= 0) {
            throw new RequestValidationException("positive integer validation", List.of(
                new ValidationError(
                    fieldName,
                    value,
                    String.format("%s must be positive for %s", fieldName, purpose),
                    List.of("1 or greater"),
                    String.format("Use a value like %d", Math.max(1, Math.abs(value)))
                )
            ));
        }
    }
    
    /**
     * Find closest string match for suggestions
     */
    private static String findClosestMatch(String input, List<String> candidates) {
        return candidates.stream()
            .min((a, b) -> Integer.compare(
                levenshteinDistance(input, a.toLowerCase()),
                levenshteinDistance(input, b.toLowerCase())
            ))
            .orElse(candidates.get(0));
    }
    
    /**
     * Simple Levenshtein distance for suggestions
     */
    private static int levenshteinDistance(String s1, String s2) {
        int[][] dp = new int[s1.length() + 1][s2.length() + 1];
        
        for (int i = 0; i <= s1.length(); i++) dp[i][0] = i;
        for (int j = 0; j <= s2.length(); j++) dp[0][j] = j;
        
        for (int i = 1; i <= s1.length(); i++) {
            for (int j = 1; j <= s2.length(); j++) {
                int cost = s1.charAt(i-1) == s2.charAt(j-1) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(dp[i-1][j] + 1, dp[i][j-1] + 1), dp[i-1][j-1] + cost);
            }
        }
        
        return dp[s1.length()][s2.length()];
    }
}
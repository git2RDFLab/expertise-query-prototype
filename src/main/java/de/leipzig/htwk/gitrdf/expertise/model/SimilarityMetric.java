package de.leipzig.htwk.gitrdf.expertise.model;

import lombok.Getter;

/**
 * Supported similarity metrics for vector comparison using PostgreSQL pgvector operators
 * 
 * All metrics are natively supported by pgvector extension:
 * - COSINE: Uses <=> operator, measures angle between vectors (best for semantic similarity)
 * - EUCLIDEAN: Uses <-> operator, measures straight-line distance (sensitive to magnitude)
 * - DOT_PRODUCT: Uses <#> operator, measures alignment and magnitude (negative inner product)
 */
@Getter
public enum SimilarityMetric {
    COSINE("cosine", "Cosine distance using <=> operator - best for semantic similarity"),
    EUCLIDEAN("euclidean", "Euclidean distance using <-> operator - straight-line distance"),
    DOT_PRODUCT("dot_product", "Dot product using <#> operator - alignment and magnitude");
    
    private final String value;
    private final String description;
    
    SimilarityMetric(String value, String description) {
        this.value = value;
        this.description = description;
    }
    
    /**
     * Parse similarity metric from string, case-insensitive
     */
    public static SimilarityMetric fromString(String input) {
        if (input == null || input.trim().isEmpty()) {
            return COSINE; // Default to cosine similarity
        }
        
        String normalized = input.toLowerCase().trim();
        return switch (normalized) {
            case "cosine", "cos", "cosine_similarity", "cosine_distance" -> COSINE;
            case "euclidean", "l2", "euclidean_distance" -> EUCLIDEAN;
            case "dot_product", "dot", "inner_product", "negative_inner_product" -> DOT_PRODUCT;
            default -> throw new IllegalArgumentException(
                String.format("Invalid similarity metric: '%s'. Supported metrics are: %s", 
                    input, getSupportedMetrics()));
        };
    }
    
    /**
     * Get comma-separated list of supported metrics for error messages
     */
    public static String getSupportedMetrics() {
        return String.join(", ", 
            COSINE.value, EUCLIDEAN.value, DOT_PRODUCT.value);
    }
    
    /**
     * Check if this metric returns a similarity score (higher = more similar)
     * vs a distance score (lower = more similar)
     */
    public boolean isSimilarityScore() {
        return this == COSINE || this == DOT_PRODUCT;
    }
    
    /**
     * Check if this metric returns a distance score (lower = more similar)
     * vs a similarity score (higher = more similar)
     */
    public boolean isDistanceScore() {
        return this == EUCLIDEAN; // Only Euclidean is a distance metric now
    }
    
    /**
     * Get the pgvector operator for this similarity metric
     */
    public String getPgVectorOperator() {
        return switch (this) {
            case COSINE -> "<=>";
            case EUCLIDEAN -> "<->";  
            case DOT_PRODUCT -> "<#>";
        };
    }
}
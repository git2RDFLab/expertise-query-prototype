package de.leipzig.htwk.gitrdf.expertise.model;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Dynamic embedding model configuration based on environment variables
 */
@Component
public class EmbeddingModel {
    
    @Value("${expert.embeddings.model-id}")
    private String modelId;
    
    @Value("${expert.embeddings.dimensions}")
    private int dimensions;
    
    private static final String DEFAULT_KEY = "default";
    private static final String DEFAULT_DESCRIPTION = "Configurable embedding model";
    
    public String getKey() {
        return DEFAULT_KEY;
    }
    
    public String getModelId() {
        return modelId;
    }
    
    public int getDimensions() {
        return dimensions;
    }
    
    public String getDescription() {
        return DEFAULT_DESCRIPTION + " (" + modelId + ", " + dimensions + " dimensions)";
    }
    
    /**
     * Get available keys - only one model supported
     */
    public static String[] getAvailableKeys() {
        return new String[]{DEFAULT_KEY};
    }
}
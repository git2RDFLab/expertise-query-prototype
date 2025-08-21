package de.leipzig.htwk.gitrdf.expertise.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import de.leipzig.htwk.gitrdf.expertise.model.EmbeddingModel;
import lombok.Data;

/**
 * Configuration for single embedding service
 * Quality model removed as per user request
 */
@Component
@ConfigurationProperties(prefix = "expert.embeddings")
@Data
public class EmbeddingServiceConfig {
    
    // Single model configuration
    private String serviceUrl;
    private String modelId = "qwen3-embedding-8b";
    private int dimensions = 4096;
    private int batchSize = 32;
    private int maxRetries = 3;
    private int timeoutSeconds = 15;
    
    // Global settings
    private String inputType = "passage";
    private int maxParallelBatches = 4;
    private int maxEntitiesPerOrder = 10000;
    private boolean enableSampling = false;
    private boolean fastMode = false;
    private boolean useRdf2vec = false;
    private int maxParallelThreads = 4;
    private boolean autoResetOnDimensionChange = false;
    
    /**
     * Get the single model configuration
     */
    public EmbeddingServiceConfig getModelConfig() {
        return this;
    }
    
    /**
     * Legacy support - always returns the single model config
     */
    public EmbeddingServiceConfig getModelConfig(EmbeddingModel model) {
        return this;
    }
    
    /**
     * Legacy support - always returns the single model config
     */
    public EmbeddingServiceConfig getModelConfig(String modelKey) {
        return this;
    }
    
    /**
     * Get available model keys (only 'fast' now)
     */
    public String[] getAvailableModels() {
        return new String[]{"fast"};
    }
    
    /**
     * Get default model key
     */
    public String getDefaultModel() {
        return "fast";
    }
    
    /**
     * Validate that required configuration is present
     */
    public void validateConfiguration() {
        if (serviceUrl == null || serviceUrl.trim().isEmpty()) {
            throw new IllegalStateException("Embedding service URL is not configured");
        }
        if (modelId == null || modelId.trim().isEmpty()) {
            throw new IllegalStateException("Model ID is not configured");
        }
        if (dimensions <= 0) {
            throw new IllegalStateException("Dimensions must be positive");
        }
    }
}
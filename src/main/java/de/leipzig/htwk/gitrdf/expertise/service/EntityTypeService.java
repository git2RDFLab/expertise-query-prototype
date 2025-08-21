package de.leipzig.htwk.gitrdf.expertise.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.model.EntityType;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing entity type conversions and validation
 * Centralizes all entity type logic and provides migration support
 */
@Service
@Slf4j
public class EntityTypeService {
    
    // Cache for frequently used conversions
    private final Map<String, EntityType> conversionCache = new ConcurrentHashMap<>();
    
    /**
     * Convert any input to normalized entity type
     * Logs conversion for debugging and migration tracking
     */
    public EntityType normalizeEntityType(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity type cannot be null or empty");
        }
        
        // Check cache first
        EntityType cached = conversionCache.get(input);
        if (cached != null) {
            return cached;
        }
        
        try {
            EntityType normalized = EntityType.fromString(input);
            
            // Log conversion for migration tracking
            if (!input.equals(normalized.getNormalizedName())) {
                log.debug("🔄 Entity type conversion: '{}' → '{}'", input, normalized.getNormalizedName());
            }
            
            // Cache the result
            conversionCache.put(input, normalized);
            
            return normalized;
            
        } catch (IllegalArgumentException e) {
            log.error(" Unsupported entity type: '{}'. {}", input, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Validate entity type from API request
     * Throws descriptive error for invalid types
     */
    public EntityType validateAndNormalize(String entityType) {
        try {
            return normalizeEntityType(entityType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                "Invalid entity type: '%s'. Supported types are: %s", 
                entityType, EntityType.getSupportedTypes()
            ));
        }
    }
    
    /**
     * Convert legacy database entity types to normalized form
     * Used for migration and compatibility
     */
    public String migrateFromLegacyType(String legacyType) {
        if (legacyType == null) {
            return null;
        }
        
        try {
            EntityType normalized = EntityType.fromString(legacyType);
            
            if (!legacyType.equals(normalized.getNormalizedName())) {
                log.info("🔄 Migrating legacy entity type: '{}' → '{}'", legacyType, normalized.getNormalizedName());
            }
            
            return normalized.getNormalizedName();
            
        } catch (IllegalArgumentException e) {
            log.warn("⚠️ Cannot migrate legacy entity type: '{}' - {}", legacyType, e.getMessage());
            return legacyType; // Keep as-is if cannot convert
        }
    }
    
    /**
     * Get normalized name for storage
     */
    public String getNormalizedName(EntityType entityType) {
        return entityType.getNormalizedName();
    }
    
    /**
     * Clear conversion cache (useful for testing)
     */
    public void clearCache() {
        conversionCache.clear();
        log.debug("🧹 Cleared entity type conversion cache");
    }
    
    /**
     * Get cache statistics for monitoring
     */
    public Map<String, Object> getCacheStats() {
        return Map.of(
            "cacheSize", conversionCache.size(),
            "cachedConversions", Map.copyOf(conversionCache)
        );
    }
}
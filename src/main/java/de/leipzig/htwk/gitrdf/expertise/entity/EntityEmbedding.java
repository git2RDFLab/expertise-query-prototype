package de.leipzig.htwk.gitrdf.expertise.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Entity
@Table(name = "entity_embeddings", indexes = {
    @Index(name = "idx_entity_uri", columnList = "entity_uri"),
    @Index(name = "idx_order_id", columnList = "order_id"),
    @Index(name = "idx_entity_type", columnList = "entity_type"),
    @Index(name = "idx_metric_type", columnList = "metric_type"),
    @Index(name = "idx_rating_value", columnList = "rating_value"),
    @Index(name = "idx_strategy", columnList = "strategy")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EntityEmbedding {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "entity_uri", nullable = false, length = 1000)
    private String entityUri;
    
    @Column(name = "order_id", nullable = false)
    private Integer orderId;
    
    @Column(name = "entity_type", length = 50)
    private String entityType;
    
    @Column(name = "metric_type", length = 50)
    private String metricType;
    
    @Column(name = "rating_value")
    private Double ratingValue;
    
    @Column(name = "strategy", length = 20)
    private String strategy; // text-based
    
    // Vector embedding (dimensions vary by model)
    @Column(name = "embedding", columnDefinition = "vector")
    private String embedding;
    
    // Vector dimensions (384 for fast model, 768 for quality model)
    @Column(name = "dimensions")
    private Integer dimensions;
    
    // Model identifier for this embedding
    @Column(name = "model_name", length = 100)
    private String modelName;
    
    // Character length of the original text that was embedded
    @Column(name = "character_length")
    private Integer characterLength;
    
    @Column(name = "created_at", nullable = false)
    private java.time.LocalDateTime createdAt = java.time.LocalDateTime.now();
    
    @Column(name = "updated_at")
    private java.time.LocalDateTime updatedAt;
    
    @PreUpdate
    public void preUpdate() {
        this.updatedAt = java.time.LocalDateTime.now();
    }
}
package de.leipzig.htwk.gitrdf.expertise.repository;

import de.leipzig.htwk.gitrdf.expertise.entity.EntityEmbedding;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface EntityEmbeddingRepository extends JpaRepository<EntityEmbedding, Long> {
    
    Optional<EntityEmbedding> findByEntityUriAndOrderId(String entityUri, Integer orderId);
    
    List<EntityEmbedding> findAllByEntityUriAndOrderId(String entityUri, Integer orderId);
    
    List<EntityEmbedding> findByOrderId(Integer orderId);
    
    List<EntityEmbedding> findByEntityType(String entityType);
    
    List<EntityEmbedding> findByMetricType(String metricType);
    
    List<EntityEmbedding> findByOrderIdAndMetricType(Integer orderId, String metricType);
    
    @Query("SELECT e FROM EntityEmbedding e WHERE e.entityUri IN :entityUris")
    List<EntityEmbedding> findByEntityUriIn(@Param("entityUris") List<String> entityUris);
    
    @Query("SELECT COUNT(e) FROM EntityEmbedding e WHERE e.orderId = :orderId")
    long countByOrderId(@Param("orderId") Integer orderId);
    
    @Query("SELECT DISTINCT e.entityType FROM EntityEmbedding e")
    List<String> findDistinctEntityTypes();
    
    @Query("SELECT DISTINCT e.metricType FROM EntityEmbedding e")
    List<String> findDistinctMetricTypes();
    
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri " +
                   "ORDER BY embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVector(@Param("queryVector") String queryVector, 
                                      @Param("excludeUri") String excludeUri, 
                                      @Param("limit") int limit);
    
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND order_id = :orderId " +
                   "ORDER BY embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndOrderId(@Param("queryVector") String queryVector, 
                                                 @Param("excludeUri") String excludeUri,
                                                 @Param("orderId") Integer orderId,
                                                 @Param("limit") int limit);
    
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "ORDER BY embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetric(@Param("queryVector") String queryVector, 
                                               @Param("excludeUri") String excludeUri,
                                               @Param("metricType") String metricType,
                                               @Param("limit") int limit);
                                                       
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "ORDER BY rating_value DESC, embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetricOrderByRatingDesc(@Param("queryVector") String queryVector, 
                                                                @Param("excludeUri") String excludeUri,
                                                                @Param("metricType") String metricType,
                                                                @Param("limit") int limit);
                                                                        
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "ORDER BY rating_value ASC, embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetricOrderByRatingAsc(@Param("queryVector") String queryVector, 
                                                               @Param("excludeUri") String excludeUri,
                                                               @Param("metricType") String metricType,
                                                               @Param("limit") int limit);
                                                                       
    // Entity type filtered versions
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "ORDER BY embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricAndEntityType(@Param("queryVector") String queryVector, 
                                                         @Param("excludeUri") String excludeUri,
                                                         @Param("metricType") String metricType,
                                                         @Param("entityType") String entityType,
                                                         @Param("limit") int limit);
                                                                
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "ORDER BY rating_value DESC, embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricAndEntityTypeOrderByRatingDesc(@Param("queryVector") String queryVector, 
                                                                          @Param("excludeUri") String excludeUri,
                                                                          @Param("metricType") String metricType,
                                                                          @Param("entityType") String entityType,
                                                                          @Param("limit") int limit);
                                                                                  
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "ORDER BY rating_value ASC, embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricAndEntityTypeOrderByRatingAsc(@Param("queryVector") String queryVector, 
                                                                         @Param("excludeUri") String excludeUri,
                                                                         @Param("metricType") String metricType,
                                                                         @Param("entityType") String entityType,
                                                                         @Param("limit") int limit);
    
    // ========================================
    // SIMILARITY METRIC SPECIFIC METHODS
    // Using different pgvector operators for cosine, euclidean, and dot product
    // ========================================
    
    /**
     * COSINE DISTANCE queries using <=> operator
     */
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "ORDER BY embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetricCosine(@Param("queryVector") String queryVector, 
                                                     @Param("excludeUri") String excludeUri,
                                                     @Param("metricType") String metricType,
                                                     @Param("limit") int limit);
    
    /**
     * DOT PRODUCT queries using <#> operator (negative inner product)
     */
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <#> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "ORDER BY embedding <#> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetricDotProduct(@Param("queryVector") String queryVector, 
                                                         @Param("excludeUri") String excludeUri,
                                                         @Param("metricType") String metricType,
                                                         @Param("limit") int limit);
    
    /**
     * Entity type specific versions with custom similarity operators
     */
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "ORDER BY embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricAndEntityTypeCosine(@Param("queryVector") String queryVector, 
                                                               @Param("excludeUri") String excludeUri,
                                                               @Param("metricType") String metricType,
                                                               @Param("entityType") String entityType,
                                                               @Param("limit") int limit);
    
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <#> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "ORDER BY embedding <#> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricAndEntityTypeDotProduct(@Param("queryVector") String queryVector, 
                                                                   @Param("excludeUri") String excludeUri,
                                                                   @Param("metricType") String metricType,
                                                                   @Param("entityType") String entityType,
                                                                   @Param("limit") int limit);
    
    // ========================================
    // DUAL MODEL SUPPORT - Filter by dimensions/model_name
    // ========================================
    
    /**
     * COSINE DISTANCE queries with model filtering by dimensions
     */
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "  AND dimensions = :dimensions " +
                   "ORDER BY embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetricCosineWithModel(@Param("queryVector") String queryVector, 
                                                              @Param("excludeUri") String excludeUri,
                                                              @Param("metricType") String metricType,
                                                              @Param("dimensions") Integer dimensions,
                                                              @Param("limit") int limit);
    
    /**
     * EUCLIDEAN DISTANCE queries with model filtering by dimensions
     */
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <-> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "  AND dimensions = :dimensions " +
                   "ORDER BY embedding <-> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetricEuclideanWithModel(@Param("queryVector") String queryVector, 
                                                                 @Param("excludeUri") String excludeUri,
                                                                 @Param("metricType") String metricType,
                                                                 @Param("dimensions") Integer dimensions,
                                                                 @Param("limit") int limit);
    
    /**
     * DOT PRODUCT queries with model filtering by dimensions
     */
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <#> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType " +
                   "  AND dimensions = :dimensions " +
                   "ORDER BY embedding <#> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorAndMetricDotProductWithModel(@Param("queryVector") String queryVector, 
                                                                  @Param("excludeUri") String excludeUri,
                                                                  @Param("metricType") String metricType,
                                                                  @Param("dimensions") Integer dimensions,
                                                                  @Param("limit") int limit);
    
    /**
     * Entity type specific versions with model filtering by dimensions
     */
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "  AND dimensions = :dimensions " +
                   "ORDER BY embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricAndEntityTypeCosineWithModel(@Param("queryVector") String queryVector, 
                                                                        @Param("excludeUri") String excludeUri,
                                                                        @Param("metricType") String metricType,
                                                                        @Param("entityType") String entityType,
                                                                        @Param("dimensions") Integer dimensions,
                                                                        @Param("limit") int limit);

    // Length-aware versions that include character_length for filtering
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance, character_length " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "  AND dimensions = :dimensions " +
                   "  AND character_length BETWEEN :minLength AND :maxLength " +
                   "ORDER BY embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricAndEntityTypeCosineWithModelAndLength(@Param("queryVector") String queryVector, 
                                                                                 @Param("excludeUri") String excludeUri,
                                                                                 @Param("metricType") String metricType,
                                                                                 @Param("entityType") String entityType,
                                                                                 @Param("dimensions") Integer dimensions,
                                                                                 @Param("minLength") int minLength,
                                                                                 @Param("maxLength") int maxLength,
                                                                                 @Param("limit") int limit);

    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance, character_length " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "  AND dimensions = :dimensions " +
                   "  AND character_length BETWEEN :minLength AND :maxLength " +
                   "ORDER BY rating_value DESC, embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingDescWithLength(@Param("queryVector") String queryVector,
                                                                                              @Param("excludeUri") String excludeUri,
                                                                                              @Param("metricType") String metricType,
                                                                                              @Param("entityType") String entityType,
                                                                                              @Param("dimensions") Integer dimensions,
                                                                                              @Param("minLength") int minLength,
                                                                                              @Param("maxLength") int maxLength,
                                                                                              @Param("limit") int limit);

    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance, character_length " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "  AND dimensions = :dimensions " +
                   "  AND character_length BETWEEN :minLength AND :maxLength " +
                   "ORDER BY rating_value ASC, embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingAscWithLength(@Param("queryVector") String queryVector,
                                                                                             @Param("excludeUri") String excludeUri,
                                                                                             @Param("metricType") String metricType,
                                                                                             @Param("entityType") String entityType,
                                                                                             @Param("dimensions") Integer dimensions,
                                                                                             @Param("minLength") int minLength,
                                                                                             @Param("maxLength") int maxLength,
                                                                                             @Param("limit") int limit);
    
    // Rating-sorted versions with dimension filtering (for best/worst/center scale selection)
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "  AND dimensions = :dimensions " +
                   "ORDER BY rating_value DESC, embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingDesc(@Param("queryVector") String queryVector, 
                                                                        @Param("excludeUri") String excludeUri,
                                                                        @Param("metricType") String metricType,
                                                                        @Param("entityType") String entityType,
                                                                        @Param("dimensions") Integer dimensions,
                                                                        @Param("limit") int limit);
                                                                        
    @Query(value = "SELECT entity_uri, entity_type, metric_type, order_id, rating_value, (embedding <=> CAST(:queryVector AS vector)) AS distance " +
                   "FROM entity_embeddings " +
                   "WHERE entity_uri != :excludeUri AND metric_type = :metricType AND entity_type = :entityType " +
                   "  AND dimensions = :dimensions " +
                   "ORDER BY rating_value ASC, embedding <=> CAST(:queryVector AS vector) " +
                   "LIMIT :limit", nativeQuery = true)
    List<Object[]> findSimilarByVectorMetricEntityTypeAndDimensionsOrderByRatingAsc(@Param("queryVector") String queryVector, 
                                                                        @Param("excludeUri") String excludeUri,
                                                                        @Param("metricType") String metricType,
                                                                        @Param("entityType") String entityType,
                                                                        @Param("dimensions") Integer dimensions,
                                                                        @Param("limit") int limit);
    
    void deleteByOrderId(Integer orderId);
}
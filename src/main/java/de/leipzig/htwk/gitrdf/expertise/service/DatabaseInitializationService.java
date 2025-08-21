package de.leipzig.htwk.gitrdf.expertise.service;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * Service to ensure database extensions and tables are created on startup
 * This handles pgvector installation and table creation
 */
@Service
@Slf4j
public class DatabaseInitializationService {

    private final JdbcTemplate jdbcTemplate;

    @Value("${expert.embeddings.auto-reset-on-dimension-change:false}")
    private boolean autoResetOnDimensionChange;

    public DatabaseInitializationService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Bean
    public ApplicationRunner initializeDatabase() {
        return args -> {
            log.info("Checking database initialization...");
            
            try {
                // CRITICAL: Install pgvector extension FIRST before any table operations
                installPgvectorExtension();
                
                // Check if entity_embeddings table exists
                String checkTableSql = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'entity_embeddings'
                    )
                    """;
                
                Boolean tableExists = jdbcTemplate.queryForObject(checkTableSql, Boolean.class);
                
                if (Boolean.TRUE.equals(tableExists)) {
                    log.info("entity_embeddings table already exists");
                    
                    // Check for missing columns and add them
                    ensureRequiredColumns();
                    
                    // Ensure embedding column dimension matches expected and migrate if configured
                    ensureEmbeddingDimension();
                    
                } else {
                    log.warn("entity_embeddings table does not exist, creating it...");
                    createEntityEmbeddingsTable();
                }
                
                // Verify pgvector extension
                checkPgvectorExtension();
                
            } catch (Exception e) {
                log.error("Failed to initialize database: {}", e.getMessage());
                log.error("This might cause issues with embedding storage and retrieval");
                // Don't throw exception - let the application start anyway
            }
        };
    }
    
    /**
     * Check if pgvector extension is available
     */
    public boolean isPgvectorAvailable() {
        try {
            String checkExtensionSql = """
                SELECT EXISTS (
                    SELECT FROM pg_extension 
                    WHERE extname = 'vector'
                )
                """;
            
            Boolean extensionExists = jdbcTemplate.queryForObject(checkExtensionSql, Boolean.class);
            return Boolean.TRUE.equals(extensionExists);
            
        } catch (Exception e) {
            log.debug("Could not check pgvector extension: {}", e.getMessage());
            return false;
        }
    }
    
    private void ensureRequiredColumns() {
        log.info("Checking required columns in entity_embeddings table...");
        
        String[] requiredColumns = {
            "entity_type", "metric_type", "rating_value", "strategy", 
            "embedding", "dimensions", "created_at", "updated_at"
        };
        
        for (String columnName : requiredColumns) {
            try {
                String checkColumnSql = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = 'entity_embeddings'
                        AND column_name = ?
                    )
                    """;
                
                Boolean columnExists = jdbcTemplate.queryForObject(checkColumnSql, Boolean.class, columnName);
                
                if (Boolean.FALSE.equals(columnExists)) {
                    log.warn("Column '{}' missing, adding it...", columnName);
                    addMissingColumn(columnName);
                    log.info("Added column '{}' to entity_embeddings table", columnName);
                }
                
            } catch (Exception e) {
                log.warn("Could not check/add column '{}': {}", columnName, e.getMessage());
            }
        }
    }
    
    private void addMissingColumn(String columnName) {
        String alterSql = switch (columnName) {
            case "entity_type" -> "ALTER TABLE entity_embeddings ADD COLUMN entity_type VARCHAR(50)";
            case "metric_type" -> "ALTER TABLE entity_embeddings ADD COLUMN metric_type VARCHAR(50)";
            case "rating_value" -> "ALTER TABLE entity_embeddings ADD COLUMN rating_value DOUBLE PRECISION";
            case "strategy" -> "ALTER TABLE entity_embeddings ADD COLUMN strategy VARCHAR(20)";
            case "embedding" -> "ALTER TABLE entity_embeddings ADD COLUMN embedding vector(4096)";
            case "dimensions" -> "ALTER TABLE entity_embeddings ADD COLUMN dimensions INTEGER NOT NULL DEFAULT 4096";
            case "model_name" -> "ALTER TABLE entity_embeddings ADD COLUMN model_name VARCHAR(100)";
            case "character_length" -> "ALTER TABLE entity_embeddings ADD COLUMN character_length INTEGER";
            case "created_at" -> "ALTER TABLE entity_embeddings ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT NOW()";
            case "updated_at" -> "ALTER TABLE entity_embeddings ADD COLUMN updated_at TIMESTAMP";
            default -> throw new IllegalArgumentException("Unknown column: " + columnName);
        };
        
        jdbcTemplate.execute(alterSql);
    }
    
    private void createEntityEmbeddingsTable() {
        log.info("Creating entity_embeddings table...");
        
        String createTableSql = """
            CREATE TABLE public.entity_embeddings (
                id BIGSERIAL PRIMARY KEY,
                entity_uri VARCHAR(1000) NOT NULL,
                order_id INTEGER NOT NULL,
                entity_type VARCHAR(50),
                metric_type VARCHAR(50),
                rating_value DOUBLE PRECISION,
                strategy VARCHAR(20),
                embedding vector(4096),
                dimensions INTEGER NOT NULL DEFAULT 4096,
                model_name VARCHAR(100),
                character_length INTEGER,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP
            )
            """;
        
        try {
            jdbcTemplate.execute(createTableSql);
            log.info("Created entity_embeddings table");
            
            // Create indexes
            createIndexes();
            
        } catch (Exception e) {
            log.error("Failed to create entity_embeddings table: {}", e.getMessage());
            throw new RuntimeException("Could not create entity_embeddings table", e);
        }
    }
    
    private void ensureEmbeddingDimension() {
        try {
            String sql = "SELECT atttypid::regtype::text AS type FROM pg_attribute WHERE attrelid = 'public.entity_embeddings'::regclass AND attname = 'embedding'";
            String type = jdbcTemplate.queryForObject(sql, String.class);
            if (type != null && type.startsWith("vector(")) {
                int start = type.indexOf('(') + 1;
                int end = type.indexOf(')');
                int dim = Integer.parseInt(type.substring(start, end));
                if (dim != 4096) {
                    log.warn("Embedding vector dimension is {} but expected 4096", dim);
                    if (autoResetOnDimensionChange) {
                        log.warn("AUTO RESET ENABLED: Dropping and recreating entity_embeddings table to use vector(4096)");
                        jdbcTemplate.execute("DROP TABLE IF EXISTS public.entity_embeddings");
                        createEntityEmbeddingsTable();
                    } else {
                        log.error("Dimension mismatch: please migrate embedding column to vector(4096) manually or set expert.embeddings.auto-reset-on-dimension-change=true to auto-migrate (drops table!)");
                    }
                }
            } else {
                log.warn("Could not detect embedding column type, got: {}", type);
            }
        } catch (Exception e) {
            log.warn("Failed to verify embedding dimension: {}", e.getMessage());
        }
    }

    private void createIndexes() {
        log.info("Creating indexes for entity_embeddings table...");
        
        String[] indexSqls = {
            "CREATE INDEX IF NOT EXISTS idx_entity_uri ON public.entity_embeddings (entity_uri)",
            "CREATE INDEX IF NOT EXISTS idx_order_id ON public.entity_embeddings (order_id)",
            "CREATE INDEX IF NOT EXISTS idx_entity_type ON public.entity_embeddings (entity_type)",
            "CREATE INDEX IF NOT EXISTS idx_metric_type ON public.entity_embeddings (metric_type)",
            "CREATE INDEX IF NOT EXISTS idx_rating_value ON public.entity_embeddings (rating_value)",
            "CREATE INDEX IF NOT EXISTS idx_strategy ON public.entity_embeddings (strategy)",
            "CREATE INDEX IF NOT EXISTS idx_order_metric_strategy ON public.entity_embeddings (order_id, metric_type, strategy)"
        };
        
        for (String indexSql : indexSqls) {
            try {
                jdbcTemplate.execute(indexSql);
            } catch (Exception e) {
                log.warn("Could not create index: {}", e.getMessage());
            }
        }
        
        log.info("Created indexes for entity_embeddings table");
    }
    
    private void installPgvectorExtension() {
        log.info("Installing pgvector extension...");
        
        try {
            // Try to create the extension (will be ignored if already exists)
            jdbcTemplate.execute("CREATE EXTENSION IF NOT EXISTS vector");
            log.info("pgvector extension installed/verified");
            
            // Test vector functionality
            jdbcTemplate.execute("SELECT '[1,2,3]'::vector");
            log.info("pgvector functionality confirmed");
            
        } catch (Exception e) {
            log.error("Failed to install pgvector extension: {}", e.getMessage());
            log.error("Make sure you're using pgvector/pgvector:pg15 Docker image");
            log.error("Vector similarity search will not work without pgvector");
            throw new RuntimeException("pgvector extension is required but not available", e);
        }
    }
    
    private void checkPgvectorExtension() {
        try {
            String checkExtensionSql = """
                SELECT EXISTS (
                    SELECT FROM pg_extension 
                    WHERE extname = 'vector'
                )
                """;
            
            Boolean extensionExists = jdbcTemplate.queryForObject(checkExtensionSql, Boolean.class);
            
            if (Boolean.TRUE.equals(extensionExists)) {
                log.info("pgvector extension is available");
            } else {
                log.error("pgvector extension is NOT installed!");
                log.error("Vector similarity search will not work properly");
                log.error("Contact your database administrator to install pgvector extension");
            }
            
        } catch (Exception e) {
            log.warn("Could not check pgvector extension: {}", e.getMessage());
        }
    }
}
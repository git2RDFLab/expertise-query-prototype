package de.leipzig.htwk.gitrdf.expertise.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Ensures pgvector extension is installed BEFORE Hibernate creates any tables
 * This prevents Hibernate from creating bytea columns instead of vector columns
 */
@Component
@Slf4j
public class PgvectorInitializer implements InitializingBean {

    private final JdbcTemplate jdbcTemplate;

    public PgvectorInitializer(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("🔧 Installing pgvector extension BEFORE Hibernate initialization...");
        
        try {
            // Install pgvector extension immediately
            jdbcTemplate.execute("CREATE EXTENSION IF NOT EXISTS vector");
            
            // Test vector functionality
            jdbcTemplate.execute("SELECT '[1,2,3]'::vector");
            
            log.info(" pgvector extension confirmed available before Hibernate");
            
        } catch (Exception e) {
            log.error(" CRITICAL: pgvector extension not available before Hibernate!");
            log.error(" Error: {}", e.getMessage());
            log.error(" Make sure you're using pgvector/pgvector:pg15 Docker image");
            
            // Fail hard - don't let the application start with broken vector support
            throw new RuntimeException("pgvector extension is required but not available", e);
        }
    }
}
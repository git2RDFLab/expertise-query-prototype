package de.leipzig.htwk.gitrdf.expertise.service.storage;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Collections;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderAnalysisEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.enums.AnalysisType;
import de.leipzig.htwk.gitrdf.database.common.repository.GithubRepositoryOrderAnalysisRepository;
import de.leipzig.htwk.gitrdf.database.common.repository.GithubRepositoryOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Extended storage service that saves RDF models to both files and database
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class StorageService {

    private final GithubRepositoryOrderRepository orderRepository;
    private final GithubRepositoryOrderAnalysisRepository analysisRepository;

    public String saveStatisticsToFile(String statisticsId, Integer targetId, String taskId, Model model) {
        // Extract short form of task ID (first 8 characters of UUID)
        String shortTaskId = taskId.length() > 8 ? taskId.substring(0, 8) : taskId;

        // Extract simple name from statisticsId to match ranking pattern
        // e.g., "atomic_metrics" -> "atomic", "gitc_metrics" -> "gitc"
        String simpleStatisticsId = statisticsId.contains("/")
                ? statisticsId.substring(statisticsId.lastIndexOf("/") + 1)
                : statisticsId;

        // Further simplify by taking the first part before underscore (like rankings)
        if (simpleStatisticsId.contains("_")) {
            simpleStatisticsId = simpleStatisticsId.substring(0, simpleStatisticsId.indexOf("_"));
        }

        String path = String.format("/analysis/statistics/%s_%d_%s.ttl", simpleStatisticsId, targetId, shortTaskId);

        try {
            // Ensure directory exists
            Path filePath = Paths.get(path);
            Files.createDirectories(filePath.getParent());

            // Write RDF model to TTL file using RDFDataMgr for better prefix handling
            try (FileOutputStream out = new FileOutputStream(path)) {
                RDFDataMgr.write(out, model, RDFFormat.TURTLE_PRETTY);
            }

            log.info("Successfully saved statistics {} for target {} to {} ({} triples)",
                    statisticsId, targetId, path, model.size());

        } catch (IOException e) {
            log.error("Failed to save statistics {} for target {} to {}: {}",
                    statisticsId, targetId, path, e.getMessage());
        }

        return path;
    }

    public Path saveRankingToFile(String rankingId, Integer targetId, String taskId, String provider, Model model) {
        // Extract short form of task ID (first 8 characters of UUID)
        String shortTaskId = taskId.length() > 8 ? taskId.substring(0, 8) : taskId;

        // Clean provider name for filename (remove special characters)
        String cleanProvider = provider != null ? provider.replaceAll("[^a-zA-Z0-9]", "").toLowerCase() : "unknown";

        String pathString = String.format("/analysis/rankings/%s_%s_%d_%s.ttl", rankingId, cleanProvider, targetId,
                shortTaskId);
        Path filePath = Paths.get(pathString);

        try {
            // Ensure directory exists
            Files.createDirectories(filePath.getParent());

            // Write RDF model to TTL file using RDFDataMgr for better prefix handling
            try (FileOutputStream out = new FileOutputStream(pathString)) {
                RDFDataMgr.write(out, model, RDFFormat.TURTLE_PRETTY);
            }

            log.info("Successfully saved ranking {} ({}) for target {} to {} ({} triples)",
                    rankingId, cleanProvider, targetId, pathString, model.size());

        } catch (IOException e) {
            log.error("Failed to save ranking {} ({}) for target {} to {}: {}",
                    rankingId, cleanProvider, targetId, pathString, e.getMessage());
        }

        return filePath;
    }

    public void checkAndFlushIfComplete(String targetId, String sessionId) {
        log.debug("Checking and flushing for target {} session {}", targetId, sessionId);
        // This method can be used for additional cleanup or aggregation logic
    }

    @Transactional
    public Long saveStatisticsToDatabase(String statisticsId, Integer targetId, String sessionId, Model model) {
        return saveAnalysisToDatabase(statisticsId, targetId, sessionId, model, AnalysisType.STATISTIC);
    }

    @Transactional
    public Long saveRankingToDatabase(String rankingId, Integer targetId, String sessionId, Model model) {
        return saveAnalysisToDatabase(rankingId, targetId, sessionId, model, AnalysisType.EXPERT);
    }


    @Transactional
    public Long saveAnalysisToDatabase(String metricId, Integer targetId, String sessionId,
            Model rdfModel, AnalysisType analysisType) {
        try {
            log.debug("Storing {} {} for target {} to database (session: {})",
                    analysisType.name().toLowerCase(), metricId, targetId, sessionId);

            // 1. Find or verify the GithubRepositoryOrder exists
            GithubRepositoryOrderEntity order = orderRepository.findById(targetId.longValue())
                    .orElseThrow(() -> new IllegalArgumentException(
                            String.format("GithubRepositoryOrder with ID %d not found", targetId)));

            // 2. Always create new analysis (allow multiple runs for same metric/target)
            GithubRepositoryOrderAnalysisEntity analysis = new GithubRepositoryOrderAnalysisEntity(order, metricId, analysisType);
            log.debug("Creating new {} analysis for metric {} and target {} (session: {})",
                    analysisType.name().toLowerCase(), metricId, targetId, sessionId);

            // 3. Convert RDF model to blob
            Blob rdfBlob = convertModelToBlob(rdfModel);
            analysis.setRdfBlob(rdfBlob);
            analysis.setCreatedAt(LocalDateTime.now());

            // 4. Save to database
            GithubRepositoryOrderAnalysisEntity savedAnalysis = analysisRepository.save(analysis);

            log.info("Successfully stored {} {} for target {} to database with ID {} ({} triples)",
                    analysisType.name().toLowerCase(), metricId, targetId,
                    savedAnalysis.getId(), rdfModel.size());

            return savedAnalysis.getId();

        } catch (Exception e) {
            log.error("Failed to store {} {} for target {} to database: {}",
                    analysisType.name().toLowerCase(), metricId, targetId, e.getMessage(), e);
            throw new RuntimeException(
                    String.format("Failed to store %s %s to database", analysisType.name().toLowerCase(), metricId), e);
        }
    }

    public void saveStatistics(String statisticsId, Integer targetId, String sessionId, Model model,
            boolean saveToFile, boolean storeToDatabase) {
        if (saveToFile) {
            saveStatisticsToFile(statisticsId, targetId, sessionId, model);
        }

        if (storeToDatabase) {
            saveStatisticsToDatabase(statisticsId, targetId, sessionId, model);
        }
    }

    public void saveRanking(String rankingId, Integer targetId, String sessionId, String provider, Model model,
            boolean saveToFile, boolean storeToDatabase) {
        if (saveToFile) {
            saveRankingToFile(rankingId, targetId, sessionId, provider, model);
        }

        if (storeToDatabase) {
            saveRankingToDatabase(rankingId, targetId, sessionId, model);
        }
    }

    private Blob convertModelToBlob(Model model) throws SQLException, IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            // Write RDF model as Turtle to byte array
            RDFDataMgr.write(outputStream, model, RDFFormat.TURTLE_PRETTY);
            byte[] rdfBytes = outputStream.toByteArray();

            // Create SQL Blob from byte array
            return new SerialBlob(rdfBytes);
        }
    }

    public boolean analysisExists(Integer targetId, String metricId) {
        return analysisRepository.findByGithubRepositoryOrderIdAndMetricId(
                targetId.longValue(), metricId).isPresent();
    }

    public Path saveRatingToFile(String expertName, Integer targetId, String sessionId, Model model) {
        // Extract short form of session ID (first 8 characters of UUID)
        String shortSessionId = sessionId != null && sessionId.length() > 8 ? 
                sessionId.substring(0, 8) : 
                (sessionId != null ? sessionId : "unknown");

        // Clean expert name for filename
        String cleanExpertName = expertName != null ? 
                expertName.replaceAll("[^a-zA-Z0-9]", "").toLowerCase() : "unknown";

        String pathString = String.format("/expertise/%s_%d_%s.ttl", 
                cleanExpertName, targetId, shortSessionId);
        Path filePath = Paths.get(pathString);

        try {
            // Ensure directory exists
            Files.createDirectories(filePath.getParent());

            // Write RDF model to TTL file using RDFDataMgr for better prefix handling
            try (FileOutputStream out = new FileOutputStream(pathString)) {
                RDFDataMgr.write(out, model, RDFFormat.TURTLE_PRETTY);
            }

            log.info("Successfully saved rating from {} for target {} to {} ({} triples)",
                    expertName, targetId, pathString, model.size());

        } catch (IOException e) {
            log.error("Failed to save rating from {} for target {} to {}: {}",
                    expertName, targetId, pathString, e.getMessage());
        }

        return filePath;
    }

    @Transactional
    public Long saveRatingToDatabase(String expertName, Integer targetId, String sessionId, Model model) {
        String metricId = "rating_" + (expertName != null ? 
                expertName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase() : "unknown");
        return saveAnalysisToDatabase(metricId, targetId, sessionId, model, AnalysisType.EXPERT);
    }

    public void saveRating(String expertName, Integer targetId, String sessionId, Model model,
            boolean saveToFile, boolean storeToDatabase) {
        if (saveToFile) {
            saveRatingToFile(expertName, targetId, sessionId, model);
        }

        if (storeToDatabase) {
            saveRatingToDatabase(expertName, targetId, sessionId, model);
        }
    }

    /**
     * Check if an order ID exists in the database
     * @param orderId The order ID to check
     * @return true if the order exists, false otherwise
     */
    public boolean orderExists(Integer orderId) {
        if (orderId == null) {
            return false;
        }
        try {
            return orderRepository.existsById(orderId.longValue());
        } catch (Exception e) {
            log.error("Error checking if order {} exists in database", orderId, e);
            return false;
        }
    }

    /**
     * Get all available order IDs from the database
     */
    public List<Integer> getAllOrderIds() {
        try {
            return orderRepository.findAll().stream()
                .map(order -> order.getId().intValue())
                .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Error retrieving all order IDs from database", e);
            return Collections.emptyList();
        }
    }
}
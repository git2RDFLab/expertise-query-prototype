package de.leipzig.htwk.gitrdf.expertise.service.data;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertFile;
import de.leipzig.htwk.gitrdf.expertise.service.storage.StorageService;
import de.leipzig.htwk.gitrdf.expertise.service.metrics.WordCountMetricsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ExpertRatingService {

    private final ExcelReadingService excelReadingService;
    private final RdfService rdfService;
    private final StorageService storageService;
    private final WordCountMetricsService wordCountMetricsService;

    @Value("${expert.ratings.directory:/expertise/imports}")
    private String expertsDirectory;

    @Value("${expert.ratings.save-to-file:true}")
    private boolean saveToFile;

    @Value("${expert.ratings.save-to-database:true}")
    private boolean saveToDatabase;

    public List<String> processAllExpertFiles() throws IOException {
        return processExpertFiles(null);
    }

    public List<String> processExpertFiles(String sessionId) throws IOException {
        return processExpertFiles(sessionId, null, saveToFile, saveToDatabase);
    }

    public List<String> processAllExpertFiles(String sessionId, boolean saveToFile, boolean saveToDatabase) throws IOException {
        return processExpertFiles(sessionId, null, saveToFile, saveToDatabase);
    }

    public List<String> processExpertFiles(String sessionId, Integer targetId, boolean saveToFile, boolean saveToDatabase) throws IOException {
        Path expertsPath = Paths.get(expertsDirectory);
        String actualSessionId = sessionId != null ? sessionId : UUID.randomUUID().toString();
        
        log.info("Starting to process expert Excel files from directory: {} for targetId: {}", expertsPath, targetId);
        
        List<ExpertFile> expertFiles = excelReadingService.readExcelFilesFromDirectory(expertsPath, true); // Force includeRatings=true
        List<String> processedFiles = new ArrayList<>();
        
        if (expertFiles.isEmpty()) {
            log.warn("No Excel files found in directory: {}", expertsPath);
            return processedFiles;
        }

        // Filter by targetId if specified
        if (targetId != null) {
            expertFiles = expertFiles.stream()
                    .filter(file -> targetId.equals(file.getTargetId()))
                    .toList();
            
            log.info("Filtered to {} expert files matching targetId: {}", expertFiles.size(), targetId);
        }

        for (ExpertFile expertFile : expertFiles) {
            try {
                // Collect word count metrics before processing
                wordCountMetricsService.collectWordCountMetrics(expertFile, actualSessionId);
                
                processExpertFile(expertFile, actualSessionId, saveToFile, saveToDatabase);
                processedFiles.add(expertFile.getFileName());
                log.info("Successfully processed expert file: {} for expert: {} (target: {})", 
                        expertFile.getFileName(), expertFile.getExpertName(), expertFile.getTargetId());
            } catch (Exception e) {
                log.error("Failed to process expert file: {}", expertFile.getFileName(), e);
                // Continue processing other files even if one fails
            }
        }

        log.info("Completed processing {} expert files. {} successful, {} failed.", 
                expertFiles.size(), processedFiles.size(), expertFiles.size() - processedFiles.size());
        
        // Calculate and save word count metrics after all files are processed
        try {
            // Use the expertise directory pattern from StorageService
            Path rdfOutputDirectory = Paths.get("/expertise/metrics");
            if (targetId != null) {
                // Calculate metrics for specific target ID
                wordCountMetricsService.calculateAndSaveMetrics(actualSessionId, targetId, rdfOutputDirectory);
            } else {
                // Calculate metrics for all target IDs in the session
                wordCountMetricsService.calculateAndSaveAllMetrics(actualSessionId, rdfOutputDirectory);
            }
        } catch (Exception e) {
            log.error("Failed to calculate and save word count metrics for session {}: {}", actualSessionId, e.getMessage(), e);
        }
        
        return processedFiles;
    }

    private void processExpertFile(ExpertFile expertFile, String sessionId) {
        processExpertFile(expertFile, sessionId, saveToFile, saveToDatabase);
    }

    private void processExpertFile(ExpertFile expertFile, String sessionId, boolean saveToFile, boolean saveToDatabase) {
        if (expertFile.getTargetId() == null) {
            log.warn("Skipping expert file {} - no target ID found", expertFile.getFileName());
            return;
        }

        if (expertFile.getSheets() == null || expertFile.getSheets().isEmpty()) {
            log.warn("Skipping expert file {} - no rating sheets found", expertFile.getFileName());
            return;
        }

        log.debug("Converting expert file {} to RDF", expertFile.getFileName());
        
        // Convert to RDF
        Model rdfModel = rdfService.convertFileToRdf(expertFile, sessionId);
        
        if (rdfModel.isEmpty()) {
            log.warn("Generated empty RDF model for expert file: {}", expertFile.getFileName());
            return;
        }

        // Save the RDF model
        storageService.saveRating(
                expertFile.getExpertName(),
                expertFile.getTargetId(),
                sessionId,
                rdfModel,
                saveToFile,
                saveToDatabase
        );

        log.info("Successfully processed expert file {} - generated {} RDF triples for target {}", 
                expertFile.getFileName(), rdfModel.size(), expertFile.getTargetId());
    }

    public ExpertFile processExpertFile(Path filePath) throws IOException {
        return processExpertFile(filePath, null);
    }

    public ExpertFile processExpertFile(Path filePath, String sessionId) throws IOException {
        log.info("Processing single expert file: {}", filePath);
        
        ExpertFile expertFile = excelReadingService.readExcelFile(filePath);
        String actualSessionId = sessionId != null ? sessionId : UUID.randomUUID().toString();
        
        // Collect word count metrics
        wordCountMetricsService.collectWordCountMetrics(expertFile, actualSessionId);
        
        processExpertFile(expertFile, actualSessionId);
        
        // Calculate and save metrics for this single file processing
        if (expertFile.getTargetId() != null) {
            try {
                Path rdfOutputDirectory = Paths.get("/expertise/metrics");
                wordCountMetricsService.calculateAndSaveMetrics(actualSessionId, expertFile.getTargetId(), rdfOutputDirectory);
            } catch (Exception e) {
                log.error("Failed to calculate and save word count metrics for single file processing: {}", e.getMessage(), e);
            }
        }
        
        return expertFile;
    }

    public List<ExpertFile> getExpertFilesInfo() throws IOException {
        Path expertsPath = Paths.get(expertsDirectory);
        return excelReadingService.readExcelFilesFromDirectory(expertsPath, false);
    }

    public List<ExpertFile> getExpertFilesInfoWithRatings() throws IOException {
        Path expertsPath = Paths.get(expertsDirectory);
        return excelReadingService.readExcelFilesFromDirectory(expertsPath, true);
    }

    public boolean checkDirectoryExists() {
        Path expertsPath = Paths.get(expertsDirectory);
        return expertsPath.toFile().exists() && expertsPath.toFile().isDirectory();
    }
}
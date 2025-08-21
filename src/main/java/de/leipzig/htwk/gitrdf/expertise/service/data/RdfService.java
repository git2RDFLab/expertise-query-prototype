package de.leipzig.htwk.gitrdf.expertise.service.data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertFile;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertRating;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertSheet;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class RdfService {

    private static final String ANALYSIS_NS = "https://purl.archive.org/git2rdf/v2/git2RDFLab-analysis#";
    private static final String XSD_NS = "http://www.w3.org/2001/XMLSchema#";
    
    // Analysis vocabulary properties
    private static final Property ANALYSIS_VALUE = ResourceFactory.createProperty(ANALYSIS_NS + "value");
    private static final Property ANALYSIS_METRIC_ID = ResourceFactory.createProperty(ANALYSIS_NS + "metricId");
    private static final Property ANALYSIS_EXECUTION_TIMESTAMP = ResourceFactory.createProperty(ANALYSIS_NS + "executionTimestamp");
    private static final Property ANALYSIS_STATUS = ResourceFactory.createProperty(ANALYSIS_NS + "status");
    private static final Property ANALYSIS_MEASURED_ENTITY = ResourceFactory.createProperty(ANALYSIS_NS + "measuredEntity");
    private static final Property ANALYSIS_BASED_ON_METRIC = ResourceFactory.createProperty(ANALYSIS_NS + "basedOnMetric");
    private static final Property ANALYSIS_WAS_GENERATED_BY = ResourceFactory.createProperty(ANALYSIS_NS + "wasGeneratedBy");
    private static final Property ANALYSIS_AGENT_NAME = ResourceFactory.createProperty(ANALYSIS_NS + "agentName");
    private static final Property ANALYSIS_AFFILIATION = ResourceFactory.createProperty(ANALYSIS_NS + "affiliation");
    private static final Property ANALYSIS_EXPERTISE = ResourceFactory.createProperty(ANALYSIS_NS + "expertise");
    private static final Property ANALYSIS_PURPOSE = ResourceFactory.createProperty(ANALYSIS_NS + "purpose");
    private static final Property ANALYSIS_SCOPE = ResourceFactory.createProperty(ANALYSIS_NS + "scope");
    private static final Property ANALYSIS_TASK_ID = ResourceFactory.createProperty(ANALYSIS_NS + "taskId");
    private static final Property ANALYSIS_HAS_RATING_RESULT = ResourceFactory.createProperty(ANALYSIS_NS + "hasRatingResult");
    private static final Property ANALYSIS_WAS_ATTRIBUTED_TO = ResourceFactory.createProperty(ANALYSIS_NS + "wasAttributedTo");
    private static final Property ANALYSIS_WAS_ASSOCIATED_WITH = ResourceFactory.createProperty(ANALYSIS_NS + "wasAssociatedWith");
    private static final Property ANALYSIS_METRIC_TYPE = ResourceFactory.createProperty(ANALYSIS_NS + "metricType");
    private static final Property ANALYSIS_DESCRIPTION = ResourceFactory.createProperty(ANALYSIS_NS + "description");
    
    // Analysis vocabulary classes
    private static final Resource ANALYSIS_RESULT = ResourceFactory.createResource(ANALYSIS_NS + "Result");
    private static final Resource ANALYSIS_METRIC = ResourceFactory.createResource(ANALYSIS_NS + "Metric");
    private static final Resource ANALYSIS_EXECUTION = ResourceFactory.createResource(ANALYSIS_NS + "Execution");
    private static final Resource ANALYSIS_HUMAN_AGENT = ResourceFactory.createResource(ANALYSIS_NS + "HumanAgent");
    private static final Resource ANALYSIS_SUCCESS = ResourceFactory.createResource(ANALYSIS_NS + "SUCCESS");
    private static final Resource ANALYSIS_RATING = ResourceFactory.createResource(ANALYSIS_NS + "rating");

    public Model convertFileToRdf(ExpertFile expertFile, String sessionId) {
        Model model = ModelFactory.createDefaultModel();
        
        // Set up namespaces
        model.setNsPrefix("analysis", ANALYSIS_NS);
        model.setNsPrefix("xsd", XSD_NS);
        model.setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        model.setNsPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        
        // Add essential ontology definitions for SHACL validation
        addEssentialOntologyDefinitions(model);
        
        String timestamp = parseRatingDateToTimestamp(expertFile.getRatingDate());
        String taskId = sessionId != null ? sessionId : UUID.randomUUID().toString();
        
        // Create human agent resource
        Resource humanAgent = createHumanAgent(model, expertFile);
        
        // Create execution resource
        Resource execution = createExecution(model, taskId, timestamp, humanAgent);
        
        // Process each sheet in the expert file
        for (ExpertSheet sheet : expertFile.getSheets()) {
            processSheet(model, sheet, expertFile, humanAgent, execution, taskId, timestamp);
        }
        
        log.info("Generated RDF model for expert file {} with {} triples", 
                expertFile.getFileName(), model.size());
        
        return model;
    }

    /**
     * Parse the rating date from Excel and convert it to ISO timestamp format.
     * Supports formats like "8/4/2025", "08/04/2025", "2025-08-04", etc.
     * If parsing fails, falls back to current timestamp.
     */
    private String parseRatingDateToTimestamp(String ratingDate) {
        if (ratingDate == null || ratingDate.trim().isEmpty()) {
            log.warn("No rating date provided, using current timestamp");
            return LocalDateTime.now().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
        }

        String trimmedDate = ratingDate.trim();
        log.info("Parsing rating date: {}", trimmedDate);

        // Try different date formats commonly found in Excel
        DateTimeFormatter[] formatters = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),  // 2025-08-04 02:50:44
            DateTimeFormatter.ofPattern("M/d/yyyy"),     // 8/4/2025
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),   // 08/04/2025
            DateTimeFormatter.ofPattern("d/M/yyyy"),     // 4/8/2025
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),   // 04/08/2025
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),   // 2025-08-04
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),   // 2025/08/04
            DateTimeFormatter.ofPattern("dd.MM.yyyy"),   // 04.08.2025
            DateTimeFormatter.ofPattern("d.M.yyyy")      // 4.8.2025
        };

        for (DateTimeFormatter formatter : formatters) {
            try {
                // Try parsing as LocalDateTime first (for formats with time)
                if (formatter.toString().contains("HH:mm:ss")) {
                    LocalDateTime dateTime = LocalDateTime.parse(trimmedDate, formatter);
                    String timestamp = dateTime.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
                    log.info("Successfully parsed rating date '{}' to timestamp: {}", trimmedDate, timestamp);
                    return timestamp;
                } else {
                    // Parse as LocalDate and convert to start of day
                    LocalDate date = LocalDate.parse(trimmedDate, formatter);
                    LocalDateTime dateTime = date.atStartOfDay();
                    String timestamp = dateTime.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
                    log.info("Successfully parsed rating date '{}' to timestamp: {}", trimmedDate, timestamp);
                    return timestamp;
                }
            } catch (DateTimeParseException e) {
                // Try next formatter
                log.debug("Failed to parse '{}' with format {}: {}", trimmedDate, formatter.toString(), e.getMessage());
            }
        }

        // If all parsing attempts fail, log warning and use current timestamp
        log.warn("Could not parse rating date '{}', falling back to current timestamp", trimmedDate);
        return LocalDateTime.now().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    private Resource createHumanAgent(Model model, ExpertFile expertFile) {
        String agentUri = ANALYSIS_NS + "expert_" + sanitizeForUri(expertFile.getExpertName());
        Resource agent = model.createResource(agentUri, ANALYSIS_HUMAN_AGENT);
        
        // Required property for human agents
        agent.addProperty(ANALYSIS_AGENT_NAME, expertFile.getExpertName());
        
        // Optional properties if available
        if (expertFile.getExpertise() != null && !expertFile.getExpertise().trim().isEmpty()) {
            agent.addProperty(ANALYSIS_EXPERTISE, expertFile.getExpertise());
        }
        
        // Could add rating date as additional metadata if needed
        // Rating date is now used as execution timestamp, not as a comment
        
        return agent;
    }

    private Resource createExecution(Model model, String taskId, String timestamp, Resource humanAgent) {
        String executionUri = ANALYSIS_NS + "execution_" + taskId.replace("-", "_");
        Resource execution = model.createResource(executionUri, ANALYSIS_EXECUTION);
        
        execution.addProperty(ANALYSIS_EXECUTION_TIMESTAMP, 
                model.createTypedLiteral(timestamp, XSD_NS + "dateTime"));
        execution.addProperty(ANALYSIS_PURPOSE, "ExpertRatingAnalysis");
        execution.addProperty(ANALYSIS_SCOPE, "Expert ratings from Excel files");
        execution.addProperty(ANALYSIS_TASK_ID, taskId);
        execution.addProperty(ANALYSIS_WAS_ASSOCIATED_WITH, humanAgent);
        
        return execution;
    }

    private void processSheet(Model model, ExpertSheet sheet, ExpertFile expertFile, 
                            Resource humanAgent, Resource execution, String taskId, String timestamp) {
        
        // Create metric resource
        Resource metric = createMetric(model, sheet);
        
        // Process each rating in the sheet
        for (int i = 0; i < sheet.getRatings().size(); i++) {
            ExpertRating rating = sheet.getRatings().get(i);
            createRatingResult(model, rating, sheet, expertFile, metric, humanAgent, execution, 
                             taskId, timestamp, i + 1);
        }
    }

    private Resource createMetric(Model model, ExpertSheet sheet) {
        // Use the metric ID from Excel, fallback to sheet name if not available
        String metricId = sheet.getMetricId() != null ? sheet.getMetricId() : sheet.getMetricName();
        String metricUri = ANALYSIS_NS + "metric_" + metricId;
        
        Resource metric = model.createResource(metricUri, ANALYSIS_METRIC);
        
        // Required properties following LLM pattern exactly
        metric.addProperty(ANALYSIS_METRIC_ID, metricId);
        metric.addProperty(ANALYSIS_METRIC_TYPE, ANALYSIS_RATING);
        
        // Add metric label if available (this is the human-readable name)
        if (sheet.getMetricLabel() != null && !sheet.getMetricLabel().trim().isEmpty()) {
            metric.addProperty(ResourceFactory.createProperty(ANALYSIS_NS + "metricLabel"), sheet.getMetricLabel());
        }
        
        return metric;
    }

    private void createRatingResult(Model model, ExpertRating rating, ExpertSheet sheet, ExpertFile expertFile,
                                  Resource metric, Resource humanAgent, Resource execution, 
                                  String taskId, String timestamp, int ratingIndex) {
        
        // Create unique result URI for EACH individual rating/row
        // Each result entity must correspond to exactly one row in the Excel table
        String shortTaskId = taskId.length() > 8 ? taskId.substring(0, 8) : taskId;
        String metricId = sheet.getMetricId() != null ? sheet.getMetricId() : sheet.getMetricName();
        String resultId = String.format("result_%s_%s_%d_%d_%s", 
                metricId, // Use the actual metric ID
                sanitizeForUri(expertFile.getExpertName()),
                ratingIndex, // Include rating index to make each result unique
                expertFile.getTargetId() != null ? expertFile.getTargetId() : 0,
                shortTaskId);
        
        String resultUri = ANALYSIS_NS + resultId;
        Resource result = model.createResource(resultUri, ANALYSIS_RESULT);
        
        // Core result properties following the ontology exactly
        // CRITICAL: Each result entity must have exactly ONE value and ONE measured entity
        result.addProperty(ANALYSIS_VALUE, model.createTypedLiteral(rating.getScore()));
        result.addProperty(ANALYSIS_METRIC_ID, metricId); // Use the actual metric ID
        result.addProperty(ANALYSIS_EXECUTION_TIMESTAMP, 
                model.createTypedLiteral(timestamp, XSD_NS + "dateTime"));
        result.addProperty(ANALYSIS_STATUS, ANALYSIS_SUCCESS);
        
        // Relationships
        result.addProperty(ANALYSIS_BASED_ON_METRIC, metric);
        result.addProperty(ANALYSIS_WAS_GENERATED_BY, execution);
        result.addProperty(ANALYSIS_WAS_ATTRIBUTED_TO, humanAgent);
        
        // Handle measured entity properly - EXACTLY ONE per result entity
        String entityUri = rating.getEntity(); // Use the entity field which contains the URI
        if (entityUri != null && !entityUri.trim().isEmpty()) {
            if (entityUri.startsWith("http")) {
                // It's a URL - create as resource
                Resource measuredEntity = model.createResource(entityUri);
                result.addProperty(ANALYSIS_MEASURED_ENTITY, measuredEntity);
                
                // Add reverse relationship from entity to result (following LLM pattern)
                measuredEntity.addProperty(ANALYSIS_HAS_RATING_RESULT, result);
            } else {
                // Not a URL - treat as IRI reference or create a resource
                // This might be a local reference like "commit1", "issue2", etc.
                result.addProperty(ANALYSIS_MEASURED_ENTITY, model.createResource(entityUri));
            }
        }
        
        log.debug("Created individual rating result {} for metric {} with score {} for entity {}", 
                resultId, metricId, rating.getScore(), rating.getRatedMetric());
    }

    private String sanitizeForUri(String input) {
        if (input == null) return "unknown";
        return input.toLowerCase()
                   .replaceAll("[^a-zA-Z0-9]", "_")
                   .replaceAll("_+", "_")
                   .replaceAll("^_|_$", "");
    }

    private String sanitizeMetricId(String metricName) {
        if (metricName == null) return "unknown_metric";
        
        // Convert to camelCase style metric ID
        String[] words = metricName.toLowerCase().split("[\\s_-]+");
        StringBuilder result = new StringBuilder(words[0]);
        
        for (int i = 1; i < words.length; i++) {
            if (!words[i].isEmpty()) {
                result.append(Character.toUpperCase(words[i].charAt(0)))
                      .append(words[i].substring(1));
            }
        }
        
        return result.toString();
    }
    
    private void addEssentialOntologyDefinitions(Model model) {
        // Add essential type declarations required by SHACL validation
        // These are minimal declarations - full definitions with labels are in git2RDFLab-analysis.ttl

        // Status instances
        Resource successStatus = model.createResource(ANALYSIS_NS + "SUCCESS");
        addTypeProperty(model, successStatus, "Status");

        Resource failedStatus = model.createResource(ANALYSIS_NS + "FAILED");
        addTypeProperty(model, failedStatus, "Status");

        // MetricType instances
        Resource ratingType = model.createResource(ANALYSIS_NS + "rating");
        addTypeProperty(model, ratingType, "MetricType");

        Resource measurementType = model.createResource(ANALYSIS_NS + "measurement");
        addTypeProperty(model, measurementType, "MetricType");

        log.debug("Added essential type declarations for SHACL validation");
    }
    
    private void addTypeProperty(Model model, Resource resource, String typeName) {
        resource.addProperty(ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                model.createResource(ANALYSIS_NS + typeName));
    }
}
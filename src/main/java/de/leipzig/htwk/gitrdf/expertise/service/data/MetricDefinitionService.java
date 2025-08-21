package de.leipzig.htwk.gitrdf.expertise.service.data;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.*;

/**
 * Service for parsing metric definitions from YAML configuration files
 * and providing mappings between metrics and their associated RDF fields
 */
@Service
@Slf4j
public class MetricDefinitionService {

    @Value("${metrics.definition.file:v001.yml}")
    private String metricsDefinitionFile;

    private Map<String, MetricDefinition> metricDefinitions;

    /**
     * Load and parse metric definitions from YAML file
     */
    public void loadMetricDefinitions() {
        if (metricDefinitions != null) {
            return; // Already loaded
        }
        
        try {
            log.info("Loading metric definitions from {}", metricsDefinitionFile);
            
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(metricsDefinitionFile);
            if (inputStream == null) {
                throw new RuntimeException("Cannot find metrics definition file: " + metricsDefinitionFile);
            }
            
            Yaml yaml = new Yaml();
            Map<String, Object> yamlData = yaml.load(inputStream);
            
            metricDefinitions = new HashMap<>();
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> metrics = (List<Map<String, Object>>) yamlData.get("metrics");
            
            if (metrics == null) {
                log.warn("No metrics found in {}", metricsDefinitionFile);
                return;
            }
            
            for (Map<String, Object> metricData : metrics) {
                String metricId = (String) metricData.get("metricId");
                if (metricId == null) continue;
                
                MetricDefinition definition = parseMetricDefinition(metricData);
                metricDefinitions.put(metricId, definition);
                
                log.debug("Loaded metric definition: {} with fields: {}", metricId, definition.getFields());
            }
            
            log.info("Loaded {} metric definitions", metricDefinitions.size());
            
        } catch (Exception e) {
            log.error("Failed to load metric definitions from {}", metricsDefinitionFile, e);
            throw new RuntimeException("Failed to load metric definitions", e);
        }
    }

    /**
     * Parse individual metric definition from YAML data
     */
    @SuppressWarnings("unchecked")
    private MetricDefinition parseMetricDefinition(Map<String, Object> metricData) {
        String metricId = (String) metricData.get("metricId");
        String metricLabel = (String) metricData.get("metricLabel");
        String metricDomain = (String) metricData.get("metricDomain");
        Boolean export = (Boolean) metricData.get("export");
        
        List<String> fields = new ArrayList<>();
        Map<String, Object> sparqlData = (Map<String, Object>) metricData.get("sparql");
        
        if (sparqlData != null) {
            Object fieldsObj = sparqlData.get("fields");
            if (fieldsObj instanceof List) {
                List<String> sparqlFields = (List<String>) fieldsObj;
                fields.addAll(sparqlFields);
            }
        }
        
        // Also get fields from prompt configuration
        Map<String, Object> promptData = (Map<String, Object>) metricData.get("prompt");
        if (promptData != null) {
            Object promptFieldsObj = promptData.get("fields");
            if (promptFieldsObj instanceof List) {
                List<String> promptFields = (List<String>) promptFieldsObj;
                // Add prompt fields that aren't already in the list
                for (String field : promptFields) {
                    if (!fields.contains(field)) {
                        fields.add(field);
                    }
                }
            }
        }
        
        return new MetricDefinition(metricId, metricLabel, metricDomain, export != null && export, fields);
    }

    /**
     * Get all available metric definitions
     */
    public Map<String, MetricDefinition> getAllMetrics() {
        loadMetricDefinitions();
        return new HashMap<>(metricDefinitions);
    }

    /**
     * Get metric definition by ID
     */
    public MetricDefinition getMetricDefinition(String metricId) {
        loadMetricDefinitions();
        return metricDefinitions.get(metricId);
    }

    /**
     * Get all metric IDs that are marked for export
     */
    public List<String> getExportableMetrics() {
        loadMetricDefinitions();
        return metricDefinitions.values().stream()
            .filter(MetricDefinition::isExport)
            .map(MetricDefinition::getMetricId)
            .toList();
    }

    /**
     * Get fields required for a specific metric
     */
    public List<String> getFieldsForMetric(String metricId) {
        loadMetricDefinitions();
        MetricDefinition definition = metricDefinitions.get(metricId);
        return definition != null ? definition.getFields() : List.of();
    }

    /**
     * Get all metrics that use a specific field
     */
    public List<String> getMetricsUsingField(String fieldName) {
        loadMetricDefinitions();
        return metricDefinitions.values().stream()
            .filter(def -> def.getFields().contains(fieldName))
            .map(MetricDefinition::getMetricId)
            .toList();
    }

    /**
     * Map RDF property names to more generic field names used in metrics
     */
    public String mapRdfPropertyToField(String rdfProperty) {
        // Map git2RDF properties to metric field names
        Map<String, String> propertyMapping = Map.of(
            "git:message", "message",
            "platform:title", "title", 
            "platform:body", "body",
            "platform:commentBody", "commentBody",
            "analysis:value", "value",
            "git:oldFileName", "fileName",
            "git:changeType", "changeType"
        );
        
        return propertyMapping.getOrDefault(rdfProperty, rdfProperty);
    }

    /**
     * Data class for metric definitions
     */
    public static class MetricDefinition {
        private final String metricId;
        private final String metricLabel;
        private final String metricDomain;
        private final boolean export;
        private final List<String> fields;

        public MetricDefinition(String metricId, String metricLabel, String metricDomain, 
                               boolean export, List<String> fields) {
            this.metricId = metricId;
            this.metricLabel = metricLabel;
            this.metricDomain = metricDomain;
            this.export = export;
            this.fields = new ArrayList<>(fields);
        }

        public String getMetricId() { return metricId; }
        public String getMetricLabel() { return metricLabel; }
        public String getMetricDomain() { return metricDomain; }
        public boolean isExport() { return export; }
        public List<String> getFields() { return new ArrayList<>(fields); }

        @Override
        public String toString() {
            return String.format("MetricDefinition{id='%s', label='%s', domain='%s', export=%s, fields=%s}", 
                metricId, metricLabel, metricDomain, export, fields);
        }
    }
}
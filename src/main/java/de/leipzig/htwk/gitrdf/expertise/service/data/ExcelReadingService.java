package de.leipzig.htwk.gitrdf.expertise.service.data;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertFile;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertInfo;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertRating;
import de.leipzig.htwk.gitrdf.expertise.model.Expert.ExpertSheet;
import de.leipzig.htwk.gitrdf.expertise.model.Metric.MetricInfo;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ExcelReadingService {

    private static final Pattern TARGET_ID_PATTERN = Pattern.compile(".*?target.*?[:\\s]+(\\d+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern EXPERT_NAME_PATTERN = Pattern.compile(".*?expertname.*?[:\\s]+([^\\n\\r]+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern REPOSITORY_PATTERN = Pattern.compile(".*?repository.*?[:\\s]+([^\\n\\r]+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public List<ExpertFile> readExcelFilesFromDirectory(Path directory) throws IOException {
        return readExcelFilesFromDirectory(directory, false);
    }

    public List<ExpertFile> readExcelFilesFromDirectory(Path directory, boolean includeRatings) throws IOException {
        List<ExpertFile> expertFiles = new ArrayList<>();
        
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            log.warn("Directory does not exist or is not a directory: {}", directory);
            return expertFiles;
        }

        try (var stream = Files.walk(directory)) {
            stream.filter(path -> path.toString().toLowerCase().endsWith(".xlsx"))
                  .filter(path -> !path.getFileName().toString().startsWith("~$")) // Exclude Excel temporary files
                  .forEach(path -> {
                      try {
                          ExpertFile expertFile = readExcelFile(path, includeRatings);
                          expertFiles.add(expertFile);
                          log.info("Successfully processed Excel file: {} with {} sheets", 
                                  path.getFileName(), expertFile.getSheets().size());
                      } catch (Exception e) {
                          log.error("Failed to process Excel file: {}", path, e);
                      }
                  });
        }

        return expertFiles;
    }

    public ExpertFile readExcelFile(Path filePath) throws IOException {
        return readExcelFile(filePath, true);
    }

    public ExpertFile readExcelFile(Path filePath, boolean includeRatings) throws IOException {
        log.debug("Reading Excel file: {}", filePath);
        
        try (InputStream inputStream = Files.newInputStream(filePath);
             Workbook workbook = new XSSFWorkbook(inputStream)) {
            
            List<ExpertSheet> sheets = new ArrayList<>();
            ExpertFile.ExpertFileBuilder fileBuilder = ExpertFile.builder()
                    .fileName(filePath.getFileName().toString())
                    .sheets(sheets);

            // Read summary sheet first (usually the first sheet)
            Sheet summarySheet = workbook.getSheetAt(0);
            
            // Extract expert info from summary using column-based approach
            ExpertInfo expertInfo = extractExpertInfo(summarySheet);
            String expertName = expertInfo.getExpertName();
            String repository = expertInfo.getRepository();
            Integer targetId = expertInfo.getTargetId();
            
            fileBuilder.expertName(expertName)
                      .expertise(expertInfo.getExpertise())
                      .ratingDate(expertInfo.getRatingDate())
                      .targetRepository(repository)
                      .targetId(targetId);

            // Process all sheets except the summary
            for (int i = 1; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                try {
                    log.info("About to process sheet {} with includeRatings={}", sheet.getSheetName(), includeRatings);
                    ExpertSheet expertSheet = processSheet(sheet, expertName, repository, targetId, includeRatings);
                    if (expertSheet != null) {
                        sheets.add(expertSheet);
                        log.info("Successfully added sheet {} with {} ratings", sheet.getSheetName(), 
                                expertSheet.getRatings() != null ? expertSheet.getRatings().size() : 0);
                    } else {
                        log.warn("Sheet {} returned null from processSheet", sheet.getSheetName());
                    }
                } catch (Exception e) {
                    log.warn("Failed to process sheet {} in file {}: {}", 
                            sheet.getSheetName(), filePath.getFileName(), e.getMessage());
                }
            }

            return fileBuilder.build();
        }
    }

    private ExpertSheet processSheet(Sheet sheet, String expertName, String repository, Integer targetId, boolean includeRatings) {
        String sheetName = sheet.getSheetName();
        log.info("Processing sheet: {} (includeRatings: {})", sheetName, includeRatings);

        // Extract metric information from the top of the sheet
        MetricInfo metricInfo = extractMetricInfo(sheet);
        String metricId = metricInfo.getMetricId() != null ? metricInfo.getMetricId() : sheetName;
        String metricLabel = metricInfo.getMetricLabel();
        String description = extractSheetDescription(sheet);
        
        // Find the data table (look for headers like "Entity", "Score", etc.)
        int dataStartRow = findDataStartRow(sheet);
        if (dataStartRow == -1) {
            log.warn("Could not find data table in sheet: {}", sheetName);
            return null;
        }
        
        log.info("Found data table starting at row {} in sheet {}", dataStartRow, sheetName);

        List<ExpertRating> ratings = new ArrayList<>();
        int ratingsCount = 0;
        
        Row headerRow = sheet.getRow(dataStartRow);
        
        // Column A (0) always contains the URIs
        int entityColumn = 0;
        
        // Find the "Score" column by exact name match
        int scoreColumn = -1;
        for (int col = 0; col < headerRow.getLastCellNum(); col++) {
            Cell headerCell = headerRow.getCell(col);
            String headerValue = getCellStringValue(headerCell);
            if (headerValue != null && headerValue.trim().equalsIgnoreCase("Score")) {
                scoreColumn = col;
                log.debug("Found Score column at index {} with header '{}'", col, headerValue);
                break;
            }
        }
        
        if (scoreColumn == -1) {
            log.warn("Could not find 'Score' column in sheet: {}", sheetName);
            return null;
        }
        
        log.debug("Found columns in sheet {}: entity={}, score={}", sheetName, entityColumn, scoreColumn);

        if (includeRatings) {

            // Read data rows
            for (int rowIndex = dataStartRow + 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null) continue;

                try {
                    String entity = null;
                    BigDecimal score = null;
                    
                    if (entityColumn >= 0 && entityColumn < row.getLastCellNum()) {
                        entity = getCellStringValue(row.getCell(entityColumn));
                    }
                    if (scoreColumn >= 0 && scoreColumn < row.getLastCellNum()) {
                        score = getCellNumericValue(row.getCell(scoreColumn));
                    }
                    
                    if (entity != null && !entity.trim().isEmpty() && score != null && isValidScore(score)) {
                        ExpertRating rating = ExpertRating.builder()
                                .ratedMetric(entity.trim()) // This is the URI of the measured entity
                                .score(score)
                                .metricType(sheetName) // Clean metric name 
                                .entity(entity.trim()) // Same as ratedMetric for now
                                .build();
                        ratings.add(rating);
                        log.debug("Added rating for entity '{}' with score {} in sheet {}", 
                                entity.trim(), score, sheetName);
                    } else {
                        if (entity != null && !entity.trim().isEmpty() && score != null && !isValidScore(score)) {
                            log.debug("Skipped row {} in sheet {} - invalid score: {} (must be between 0 and 10)", 
                                    rowIndex, sheetName, score);
                        } else {
                            log.debug("Skipped row {} in sheet {} - entity: '{}', score: {}", 
                                    rowIndex, sheetName, entity, score);
                        }
                    }
                } catch (Exception e) {
                    log.debug("Failed to process row {} in sheet {}: {}", rowIndex, sheetName, e.getMessage());
                }
            }
            ratingsCount = ratings.size();
        } else {
            // Just count the ratings without loading them
            if (dataStartRow != -1 && scoreColumn >= 0) {
                for (int rowIndex = dataStartRow + 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                    Row row = sheet.getRow(rowIndex);
                    if (row == null) continue;
                    
                    try {
                        // Check if row has valid entity and score (same logic as data processing)
                        String entity = null;
                        BigDecimal score = null;
                        
                        if (entityColumn < row.getLastCellNum()) {
                            entity = getCellStringValue(row.getCell(entityColumn));
                        }
                        if (scoreColumn < row.getLastCellNum()) {
                            score = getCellNumericValue(row.getCell(scoreColumn));
                        }
                        
                        if (entity != null && !entity.trim().isEmpty() && score != null && isValidScore(score)) {
                            ratingsCount++;
                        }
                    } catch (Exception e) {
                        log.debug("Failed to process row {} in sheet {} for counting: {}", rowIndex, sheetName, e.getMessage());
                    }
                }
            }
        }

        return ExpertSheet.builder()
                .expertName(expertName)
                .targetRepository(repository)
                .targetId(targetId)
                .metricName(sheetName)
                .metricId(metricId)
                .metricLabel(metricLabel)
                .description(description)
                .ratings(ratings)
                .ratingsCount(ratingsCount)
                .build();
    }

    private ExpertInfo extractExpertInfo(Sheet sheet) {
        ExpertInfo.ExpertInfoBuilder builder = ExpertInfo.builder();
        
        log.debug("Extracting expert info from sheet: {}", sheet.getSheetName());
        
        // Scan the summary sheet for the specific labels and their adjacent values
        for (int rowIndex = 0; rowIndex <= Math.min(20, sheet.getLastRowNum()); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) continue;
            
            // Look for label patterns in each row
            for (int cellIndex = 0; cellIndex < Math.min(row.getLastCellNum(), 10); cellIndex++) {
                Cell cell = row.getCell(cellIndex);
                String cellValue = getCellStringValue(cell);
                if (cellValue == null) continue;
                
                String lowerValue = cellValue.toLowerCase().trim();
                log.debug("Checking cell [{},{}]: '{}'", rowIndex, cellIndex, cellValue);
                
                // Check for labels and extract adjacent values - try multiple patterns
                if (lowerValue.contains("expert name") || lowerValue.equals("expert name:")) {
                    String expertName = getCellStringValue(row.getCell(cellIndex + 1));
                    if (expertName != null && !expertName.trim().isEmpty()) {
                        builder.expertName(expertName.trim());
                        log.info("Found Expert Name: {}", expertName.trim());
                    }
                } else if (lowerValue.contains("expertise") || lowerValue.equals("expertise:")) {
                    String expertise = getCellStringValue(row.getCell(cellIndex + 1));
                    if (expertise != null && !expertise.trim().isEmpty()) {
                        builder.expertise(expertise.trim());
                        log.info("Found Expertise: {}", expertise.trim());
                    }
                } else if (lowerValue.contains("rating date") || lowerValue.contains("date")) {
                    String ratingDate = getCellStringValue(row.getCell(cellIndex + 1));
                    if (ratingDate != null && !ratingDate.trim().isEmpty()) {
                        builder.ratingDate(ratingDate.trim());
                        log.info("Found Rating Date: {}", ratingDate.trim());
                    }
                } else if (lowerValue.contains("target") && (lowerValue.contains("id") || lowerValue.contains("repository"))) {
                    // Handle "Target ID (Repository):" format
                    String targetIdStr = getCellStringValue(row.getCell(cellIndex + 1));
                    if (targetIdStr != null && !targetIdStr.trim().isEmpty()) {
                        try {
                            // Extract numeric part if it contains other text
                            String numericPart = targetIdStr.replaceAll("[^\\d]", "");
                            if (!numericPart.isEmpty()) {
                                Integer targetId = Integer.valueOf(numericPart);
                                builder.targetId(targetId);
                                log.info("Found Target ID: {}", targetId);
                            }
                        } catch (NumberFormatException e) {
                            log.debug("Failed to parse target ID from: {}", targetIdStr);
                        }
                    }
                } else if (lowerValue.contains("repository") && !lowerValue.contains("target")) {
                    String repository = getCellStringValue(row.getCell(cellIndex + 1));
                    if (repository != null && !repository.trim().isEmpty()) {
                        builder.repository(repository.trim());
                        log.info("Found Repository: {}", repository.trim());
                    }
                }
            }
        }
        
        ExpertInfo result = builder.build();
        log.info("Extracted expert info - Name: '{}', Expertise: '{}', Date: '{}', TargetID: '{}'", 
                result.getExpertName(), result.getExpertise(), result.getRatingDate(), result.getTargetId());
        
        return result;
    }

    private MetricInfo extractMetricInfo(Sheet sheet) {
        MetricInfo.MetricInfoBuilder builder = MetricInfo.builder();
        
        // Scan the top of the sheet for metric information
        for (int rowIndex = 0; rowIndex <= Math.min(10, sheet.getLastRowNum()); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) continue;
            
            // Look for label patterns in each row
            for (int cellIndex = 0; cellIndex < Math.min(row.getLastCellNum(), 10); cellIndex++) {
                Cell cell = row.getCell(cellIndex);
                String cellValue = getCellStringValue(cell);
                if (cellValue == null) continue;
                
                String lowerValue = cellValue.toLowerCase().trim();
                
                // Check for labels and extract adjacent values
                if (lowerValue.contains("metric id") && cellIndex + 1 < row.getLastCellNum()) {
                    String metricId = getCellStringValue(row.getCell(cellIndex + 1));
                    if (metricId != null && !metricId.trim().isEmpty()) {
                        builder.metricId(metricId.trim());
                    }
                } else if (lowerValue.contains("metric label") && cellIndex + 1 < row.getLastCellNum()) {
                    String metricLabel = getCellStringValue(row.getCell(cellIndex + 1));
                    if (metricLabel != null && !metricLabel.trim().isEmpty()) {
                        builder.metricLabel(metricLabel.trim());
                    }
                }
            }
        }
        
        return builder.build();
    }

    private String extractSheetText(Sheet sheet) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i <= Math.min(10, sheet.getLastRowNum()); i++) {
            Row row = sheet.getRow(i);
            if (row != null) {
                for (Cell cell : row) {
                    String cellValue = getCellStringValue(cell);
                    if (cellValue != null && !cellValue.trim().isEmpty()) {
                        text.append(cellValue).append(" ");
                    }
                }
                text.append("\n");
            }
        }
        return text.toString();
    }

    private String extractSheetDescription(Sheet sheet) {
        StringBuilder description = new StringBuilder();
        for (int i = 0; i <= Math.min(5, sheet.getLastRowNum()); i++) {
            Row row = sheet.getRow(i);
            if (row != null) {
                for (Cell cell : row) {
                    String cellValue = getCellStringValue(cell);
                    if (cellValue != null && !cellValue.trim().isEmpty()) {
                        description.append(cellValue).append(" ");
                    }
                }
            }
        }
        return description.toString().trim();
    }

    private int findDataStartRow(Sheet sheet) {
        log.debug("Looking for data start row in sheet: {}", sheet.getSheetName());
        for (int i = 0; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            if (row != null) {
                // Look for typical header patterns
                StringBuilder rowContent = new StringBuilder();
                for (Cell cell : row) {
                    String cellValue = getCellStringValue(cell);
                    if (cellValue != null) {
                        rowContent.append(cellValue).append(" | ");
                        String lowerValue = cellValue.toLowerCase().trim();
                        if (lowerValue.equals("entity") || lowerValue.equals("commit") || 
                            lowerValue.equals("issue") || lowerValue.equals("metric") ||
                            lowerValue.equals("score") || lowerValue.equals("rating")) {
                            log.debug("Found data header row {} in sheet {}: {}", i, sheet.getSheetName(), rowContent.toString());
                            return i;
                        }
                    }
                }
                if (rowContent.length() > 0) {
                    log.debug("Row {} content: {}", i, rowContent.toString());
                }
            }
        }
        log.warn("No data header row found in sheet: {}", sheet.getSheetName());
        return -1;
    }

    private int findColumn(Row headerRow, String... columnNames) {
        if (headerRow == null) return -1;
        
        // First pass: exact match
        for (int i = 0; i < headerRow.getLastCellNum(); i++) {
            Cell cell = headerRow.getCell(i);
            String cellValue = getCellStringValue(cell);
            if (cellValue != null) {
                String lowerValue = cellValue.toLowerCase().trim();
                for (String columnName : columnNames) {
                    if (lowerValue.equals(columnName.toLowerCase())) {
                        log.debug("Found exact match for column '{}' at index {}", columnName, i);
                        return i;
                    }
                }
            }
        }
        
        // Second pass: contains match
        for (int i = 0; i < headerRow.getLastCellNum(); i++) {
            Cell cell = headerRow.getCell(i);
            String cellValue = getCellStringValue(cell);
            if (cellValue != null) {
                String lowerValue = cellValue.toLowerCase().trim();
                for (String columnName : columnNames) {
                    if (lowerValue.contains(columnName.toLowerCase())) {
                        log.debug("Found contains match for column '{}' at index {} with header '{}'", columnName, i, cellValue);
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private String getCellStringValue(Cell cell) {
        if (cell == null) return null;
        
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    double numericValue = cell.getNumericCellValue();
                    if (numericValue == Math.floor(numericValue)) {
                        return String.valueOf((long) numericValue);
                    } else {
                        return String.valueOf(numericValue);
                    }
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return String.valueOf(cell.getNumericCellValue());
                } catch (Exception e) {
                    return cell.getStringCellValue();
                }
            case BLANK:
            case ERROR:
            case _NONE:
            default:
                return null;
        }
    }

    private BigDecimal getCellNumericValue(Cell cell) {
        if (cell == null) return null;
        
        try {
            switch (cell.getCellType()) {
                case NUMERIC:
                    return BigDecimal.valueOf(cell.getNumericCellValue());
                case STRING:
                    String stringValue = cell.getStringCellValue().trim();
                    if (!stringValue.isEmpty()) {
                        return new BigDecimal(stringValue);
                    }
                    break;
                case FORMULA:
                    return BigDecimal.valueOf(cell.getNumericCellValue());
                case BOOLEAN:
                case BLANK:
                case ERROR:
                case _NONE:
                default:
                    break;
            }
        } catch (Exception e) {
            log.debug("Failed to parse numeric value from cell: {}", e.getMessage());
        }
        return null;
    }

    private boolean isValidScore(BigDecimal score) {
        if (score == null) {
            return false;
        }
        
        return score.compareTo(BigDecimal.ZERO) >= 0 && score.compareTo(BigDecimal.TEN) <= 0;
    }

    private String extractExpertName(String text) {
        Matcher matcher = EXPERT_NAME_PATTERN.matcher(text);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return "Unknown Expert";
    }

    private String extractRepository(String text) {
        Matcher matcher = REPOSITORY_PATTERN.matcher(text);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return "Unknown Repository";
    }

    private Integer extractTargetId(String text) {
        Matcher matcher = TARGET_ID_PATTERN.matcher(text);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                log.warn("Failed to parse target ID: {}", matcher.group(1));
            }
        }
        return null;
    }
}
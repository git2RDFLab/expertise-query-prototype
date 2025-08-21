package de.leipzig.htwk.gitrdf.expertise.exception;

import lombok.Getter;
import java.util.List;
import java.util.Map;

/**
 * Custom validation exception with detailed error information
 */
@Getter
public class ValidationException extends RuntimeException {
    
    private final String field;
    private final Object providedValue;
    private final List<String> allowedValues;
    private final String suggestion;
    private final Map<String, Object> additionalInfo;
    
    public ValidationException(String field, Object providedValue, String message) {
        this(field, providedValue, message, null, null, null);
    }
    
    public ValidationException(String field, Object providedValue, String message, List<String> allowedValues) {
        this(field, providedValue, message, allowedValues, null, null);
    }
    
    public ValidationException(String field, Object providedValue, String message, List<String> allowedValues, String suggestion) {
        this(field, providedValue, message, allowedValues, suggestion, null);
    }
    
    public ValidationException(String field, Object providedValue, String message, 
                             List<String> allowedValues, String suggestion, Map<String, Object> additionalInfo) {
        super(message);
        this.field = field;
        this.providedValue = providedValue;
        this.allowedValues = allowedValues;
        this.suggestion = suggestion;
        this.additionalInfo = additionalInfo;
    }
}
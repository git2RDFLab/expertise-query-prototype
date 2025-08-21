package de.leipzig.htwk.gitrdf.expertise.exception;

import lombok.Getter;
import java.util.List;
import java.util.Map;

/**
 * Exception for request-level validation errors with structured feedback
 */
@Getter
public class RequestValidationException extends RuntimeException {
    
    private final List<ValidationError> validationErrors;
    private final String requestType;
    private final Map<String, Object> validExample;
    
    public RequestValidationException(String requestType, List<ValidationError> validationErrors) {
        this(requestType, validationErrors, null);
    }
    
    public RequestValidationException(String requestType, List<ValidationError> validationErrors, Map<String, Object> validExample) {
        super(createErrorMessage(validationErrors));
        this.requestType = requestType;
        this.validationErrors = validationErrors;
        this.validExample = validExample;
    }
    
    private static String createErrorMessage(List<ValidationError> errors) {
        if (errors.isEmpty()) {
            return "Request validation failed";
        }
        if (errors.size() == 1) {
            return errors.get(0).getMessage();
        }
        return String.format("Multiple validation errors: %s", 
            errors.stream().map(ValidationError::getField).toList());
    }
    
    @Getter
    public static class ValidationError {
        private final String field;
        private final Object providedValue;
        private final String message;
        private final List<String> allowedValues;
        private final String suggestion;
        
        public ValidationError(String field, Object providedValue, String message) {
            this(field, providedValue, message, null, null);
        }
        
        public ValidationError(String field, Object providedValue, String message, List<String> allowedValues) {
            this(field, providedValue, message, allowedValues, null);
        }
        
        public ValidationError(String field, Object providedValue, String message, List<String> allowedValues, String suggestion) {
            this.field = field;
            this.providedValue = providedValue;
            this.message = message;
            this.allowedValues = allowedValues;
            this.suggestion = suggestion;
        }
    }
}
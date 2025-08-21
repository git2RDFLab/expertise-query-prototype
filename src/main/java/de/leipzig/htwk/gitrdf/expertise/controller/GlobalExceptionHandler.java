package de.leipzig.htwk.gitrdf.expertise.controller;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import com.fasterxml.jackson.core.JsonParseException;

import de.leipzig.htwk.gitrdf.expertise.exception.RequestValidationException;
import de.leipzig.htwk.gitrdf.expertise.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationErrors(MethodArgumentNotValidException ex) {
        Map<String, String> fieldErrors = ex.getBindingResult().getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        FieldError::getField,
                        error -> error.getDefaultMessage() != null ? error.getDefaultMessage() : "Invalid value",
                        (existing, replacement) -> existing + "; " + replacement
                ));

        return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Validation failed",
                "message", "Please fix the following field errors:",
                "fieldErrors", fieldErrors,
                "hint", "Check that all required fields are provided and have valid values"
        ));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleJsonParseError(HttpMessageNotReadableException ex) {
        String message = "Invalid JSON format";
        String hint = "Check your JSON syntax";
        
        if (ex.getCause() instanceof JsonParseException) {
            JsonParseException jsonEx = (JsonParseException) ex.getCause();
            message = "JSON parsing error: " + jsonEx.getOriginalMessage();
            hint = "Common issues: trailing commas, missing quotes, unescaped characters";
        } else if (ex.getMessage().contains("trailing comma")) {
            message = "Invalid JSON: trailing comma detected";
            hint = "Remove the trailing comma from your JSON";
        } else if (ex.getMessage().contains("Unexpected character")) {
            message = "Invalid JSON: unexpected character";
            hint = "Check for missing quotes, brackets, or commas";
        }

        log.warn("JSON parsing error: {}", ex.getMessage());

        return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Invalid JSON",
                "message", message,
                "hint", hint,
                "example", Map.of(
                        "correct", "\"field\": \"value\"",
                        "incorrect", "\"field\": \"value\","
                )
        ));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> handleMissingParameter(MissingServletRequestParameterException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Missing required parameter",
                "message", String.format("Required parameter '%s' is missing", ex.getParameterName()),
                "parameterName", ex.getParameterName(),
                "parameterType", ex.getParameterType(),
                "hint", "Add the missing parameter to your request"
        ));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Map<String, Object>> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Invalid parameter type",
                "message", String.format("Parameter '%s' should be of type %s but received: %s", 
                        ex.getName(), 
                        ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : "unknown",
                        ex.getValue()),
                "parameterName", ex.getName(),
                "expectedType", ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : "unknown",
                "receivedValue", ex.getValue(),
                "hint", "Check that the parameter value matches the expected type"
        ));
    }

    @ExceptionHandler(RequestValidationException.class)
    public ResponseEntity<Map<String, Object>> handleRequestValidation(RequestValidationException ex) {
        log.warn("Request validation failed for {}: {}", ex.getRequestType(), ex.getMessage());
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", "Request validation failed");
        response.put("requestType", ex.getRequestType());
        response.put("message", ex.getMessage());
        
        // Add field-specific errors
        List<Map<String, Object>> fieldErrors = ex.getValidationErrors().stream()
            .map(error -> {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("field", error.getField());
                errorMap.put("providedValue", error.getProvidedValue());
                errorMap.put("message", error.getMessage());
                if (error.getAllowedValues() != null && !error.getAllowedValues().isEmpty()) {
                    errorMap.put("allowedValues", error.getAllowedValues());
                }
                if (error.getSuggestion() != null) {
                    errorMap.put("suggestion", error.getSuggestion());
                }
                return errorMap;
            }).collect(Collectors.toList());
        
        response.put("validationErrors", fieldErrors);
        
        // Add valid example if provided
        if (ex.getValidExample() != null) {
            response.put("validExample", ex.getValidExample());
        }
        
        // Add helpful hints based on the error type
        if (ex.getRequestType().contains("semantic")) {
            response.put("documentation", "See API docs for semantic model options and their performance characteristics");
        }
        
        return ResponseEntity.badRequest().body(response);
    }
    
    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Map<String, Object>> handleFieldValidation(ValidationException ex) {
        log.warn("Field validation failed for {}: {}", ex.getField(), ex.getMessage());
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", "Field validation failed");
        response.put("field", ex.getField());
        response.put("providedValue", ex.getProvidedValue());
        response.put("message", ex.getMessage());
        
        if (ex.getAllowedValues() != null && !ex.getAllowedValues().isEmpty()) {
            response.put("allowedValues", ex.getAllowedValues());
        }
        
        if (ex.getSuggestion() != null) {
            response.put("suggestion", ex.getSuggestion());
        }
        
        if (ex.getAdditionalInfo() != null) {
            response.putAll(ex.getAdditionalInfo());
        }
        
        return ResponseEntity.badRequest().body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        // Parse common patterns for better error messages
        String message = ex.getMessage();
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", "Invalid argument");
        
        // Smart parsing of common validation messages
        if (message != null && message.contains("semanticModel")) {
            response.put("field", "semanticModel");
            response.put("message", message);
            response.put("allowedValues", List.of("fast", "quality"));
            response.put("suggestion", "Use 'fast' for 384D embeddings or 'quality' for 768D embeddings");
        } else if (message != null && message.contains("scaleType")) {
            response.put("field", "scaleType");
            response.put("message", message);
            response.put("allowedValues", List.of("best", "worst", "center", "randomcenter"));
            response.put("suggestion", "Use 'best' for top-rated, 'worst' for low-rated, 'center' for balanced results, or 'randomcenter' for random selection from rating groups");
        } else {
            response.put("message", message != null ? message : "Invalid argument provided");
            response.put("hint", "Check that all provided values are valid for this operation");
        }
        
        return ResponseEntity.badRequest().body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericError(Exception ex) {
        log.error("Unexpected error occurred", ex);
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "success", false,
                "error", "Internal server error",
                "message", "An unexpected error occurred",
                "hint", "If this persists, please check the server logs or contact support"
        ));
    }
}
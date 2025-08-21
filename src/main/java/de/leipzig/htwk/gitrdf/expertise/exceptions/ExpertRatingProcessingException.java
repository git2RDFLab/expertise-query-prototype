package de.leipzig.htwk.gitrdf.expertise.exceptions;

public class ExpertRatingProcessingException extends RuntimeException {
    
    public ExpertRatingProcessingException(String message) {
        super(message);
    }
    
    public ExpertRatingProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
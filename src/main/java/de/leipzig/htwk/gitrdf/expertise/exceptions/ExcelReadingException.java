package de.leipzig.htwk.gitrdf.expertise.exceptions;

public class ExcelReadingException extends RuntimeException {
    
    public ExcelReadingException(String message) {
        super(message);
    }
    
    public ExcelReadingException(String message, Throwable cause) {
        super(message, cause);
    }
}
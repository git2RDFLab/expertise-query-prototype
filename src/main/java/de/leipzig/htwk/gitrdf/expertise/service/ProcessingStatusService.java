package de.leipzig.htwk.gitrdf.expertise.service;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * Service to track embedding processing status
 */
@Service
@Slf4j
public class ProcessingStatusService {
    
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicReference<String> currentOperation = new AtomicReference<>("idle");
    private final AtomicInteger totalOrders = new AtomicInteger(0);
    private final AtomicInteger processedOrders = new AtomicInteger(0);
    private final AtomicReference<LocalDateTime> startTime = new AtomicReference<>();
    private final AtomicReference<LocalDateTime> endTime = new AtomicReference<>();
    private final AtomicReference<String> lastError = new AtomicReference<>();
    private final AtomicReference<Integer> currentOrderId = new AtomicReference<>();
    
    public void startProcessing(String operation, int totalOrderCount) {
        isProcessing.set(true);
        currentOperation.set(operation);
        totalOrders.set(totalOrderCount);
        processedOrders.set(0);
        startTime.set(LocalDateTime.now());
        endTime.set(null);
        lastError.set(null);
        currentOrderId.set(null);
        log.info("🚀 Started processing: {} (total orders: {})", operation, totalOrderCount);
    }
    
    public void updateCurrentOrder(Integer orderId) {
        currentOrderId.set(orderId);
        log.info("🔄 Processing order: {}", orderId);
    }
    
    public void completeOrder(Integer orderId) {
        processedOrders.incrementAndGet();
        log.info(" Completed order: {} ({}/{})", orderId, processedOrders.get(), totalOrders.get());
    }
    
    public void completeProcessing() {
        isProcessing.set(false);
        currentOperation.set("completed");
        endTime.set(LocalDateTime.now());
        currentOrderId.set(null);
        log.info(" Processing completed - {}/{} orders processed", processedOrders.get(), totalOrders.get());
    }
    
    public void failProcessing(String error) {
        isProcessing.set(false);
        currentOperation.set("failed");
        endTime.set(LocalDateTime.now());
        lastError.set(error);
        currentOrderId.set(null);
        log.error(" Processing failed: {}", error);
    }
    
    public ProcessingStatus getStatus() {
        return new ProcessingStatus(
            isProcessing.get(),
            currentOperation.get(),
            totalOrders.get(),
            processedOrders.get(),
            currentOrderId.get(),
            startTime.get(),
            endTime.get(),
            lastError.get()
        );
    }
    
    public boolean isCurrentlyProcessing() {
        return isProcessing.get();
    }
    
    public record ProcessingStatus(
        boolean isProcessing,
        String operation,
        int totalOrders,
        int processedOrders,
        Integer currentOrderId,
        LocalDateTime startTime,
        LocalDateTime endTime,
        String lastError
    ) {
        public double getProgressPercentage() {
            if (totalOrders == 0) return 0.0;
            return (double) processedOrders / totalOrders * 100.0;
        }
        
        public String getStatus() {
            if (isProcessing) {
                return currentOrderId != null ? 
                    String.format("Processing order %d (%d/%d)", currentOrderId, processedOrders, totalOrders) :
                    String.format("Processing (%d/%d)", processedOrders, totalOrders);
            } else if ("completed".equals(operation)) {
                return String.format("Completed (%d/%d orders processed)", processedOrders, totalOrders);
            } else if ("failed".equals(operation)) {
                return String.format("Failed (%d/%d orders processed) - %s", processedOrders, totalOrders, lastError);
            } else {
                return "Idle";
            }
        }
    }
}
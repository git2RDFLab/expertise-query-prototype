package de.leipzig.htwk.gitrdf.expertise.model.Expert;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExpertRating {
    private String ratedMetric;
    private BigDecimal score;
    private String metricType;
    private String entity;
}
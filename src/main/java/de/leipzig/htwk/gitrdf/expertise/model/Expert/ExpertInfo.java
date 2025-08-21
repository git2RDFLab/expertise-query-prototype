package de.leipzig.htwk.gitrdf.expertise.model.Expert;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExpertInfo {
    private String expertName;
    private String expertise;
    private String ratingDate;
    private String repository;
    private Integer targetId;
}
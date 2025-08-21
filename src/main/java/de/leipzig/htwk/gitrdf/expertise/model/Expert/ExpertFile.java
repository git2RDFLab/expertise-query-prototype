package de.leipzig.htwk.gitrdf.expertise.model.Expert;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExpertFile {
    private String expertName;
    private String expertise;
    private String ratingDate;
    private String targetRepository;
    private Integer targetId;
    private String fileName;
    private List<ExpertSheet> sheets;
}
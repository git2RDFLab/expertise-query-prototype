package de.leipzig.htwk.gitrdf.expertise;

import org.apache.jena.shared.impl.JenaParameters;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication(scanBasePackages = {
        "de.leipzig.htwk.gitrdf.expertise",
        "de.leipzig.htwk.gitrdf.database.common.entity",
        "de.leipzig.htwk.gitrdf.database.common.repository"
})
@EnableAsync
public class ExpertiseApplication {
    public static void main(String[] args) {

        JenaParameters.enableEagerLiteralValidation = true;
        JenaParameters.enableSilentAcceptanceOfUnknownDatatypes = false;

        SpringApplication.run(ExpertiseApplication.class, args);
    }
}
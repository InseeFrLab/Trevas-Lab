package fr.insee.vtl.lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.jackson.TrevasModule;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootApplication
public class VtlLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(VtlLabApplication.class, args);
    }

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new TrevasModule());
    }
}

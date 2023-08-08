package fr.insee.trevas.lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.insee.vtl.jackson.TrevasModule;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootApplication
public class TrevasLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(TrevasLabApplication.class, args);
    }

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new TrevasModule())
                .registerModule(new JavaTimeModule());
    }

}

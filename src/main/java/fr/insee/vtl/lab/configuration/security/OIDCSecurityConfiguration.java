package fr.insee.vtl.lab.configuration.security;

import fr.insee.vtl.lab.model.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.jwt.Jwt;

@Configuration
@ConditionalOnProperty(name = "auth.mode", havingValue = "OIDC")
@EnableWebSecurity
public class OIDCSecurityConfiguration extends WebSecurityConfigurerAdapter {

    @Value("${jwt.username-claim}")
    private String usernameClaim;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.httpBasic().disable()
                .csrf().disable()
                .authorizeRequests()
                .antMatchers("/api", "/api/").permitAll() // For swagger-ui redirection
                .antMatchers(HttpMethod.OPTIONS).permitAll()
                .antMatchers("/api/**").authenticated()
                .anyRequest().permitAll()
                .and()
                .oauth2ResourceServer()
                .jwt();
    }

    @Bean
    public UserProvider getUserProvider() {
        return auth -> {
            final User user = new User();
            final Jwt jwt = (Jwt) auth.getPrincipal();
            user.setId(jwt.getClaimAsString(usernameClaim));
            user.setAuthToken(jwt.getTokenValue());
            return user;
        };
    }
}

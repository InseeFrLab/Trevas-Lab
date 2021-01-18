package fr.insee.vtl.lab.configuration.security;

import fr.insee.vtl.lab.model.User;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Configuration
@EnableWebSecurity
@ConditionalOnSingleCandidate(WebSecurityConfigurerAdapter.class)
public class DefaultSecurityConfiguration extends WebSecurityConfigurerAdapter {

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.authorizeRequests()
                .antMatchers("/**").permitAll();
        http.headers().frameOptions().disable();
        http.csrf().disable();

    }

    @Bean
    public UserProvider getUserProvider() {
        return (a) -> {
            User user = new User("Name", "Token");
            return user;
        };
    }
}

package fr.insee.vtl.lab.configuration.security;

import fr.insee.vtl.lab.model.User;
import org.springframework.security.core.Authentication;


@FunctionalInterface
public interface UserProvider {

    public User getUser(Authentication auth);
}

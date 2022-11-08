package fr.insee.trevas.lab.configuration.security;

import fr.insee.trevas.lab.model.User;
import org.springframework.security.core.Authentication;


@FunctionalInterface
public interface UserProvider {

    public User getUser(Authentication auth);
}

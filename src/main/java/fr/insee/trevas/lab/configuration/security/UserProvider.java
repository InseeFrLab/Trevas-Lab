package fr.insee.trevas.lab.configuration.security;

import fr.insee.trevas.lab.model.User;
import org.springframework.security.core.Authentication;


@FunctionalInterface
public interface UserProvider {

    User getUser(Authentication auth);
}

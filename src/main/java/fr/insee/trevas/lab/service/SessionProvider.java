package fr.insee.trevas.lab.service;

import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class SessionProvider {

    private static final Map<String, Object> SESSION = new HashMap<>();

    public Map<String, Object> getSession(Authentication auth) {
        return SESSION;
    }
}

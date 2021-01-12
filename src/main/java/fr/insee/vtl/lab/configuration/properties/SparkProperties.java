package fr.insee.vtl.lab.configuration.properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkProperties {

    @Value("${spark.cluster.master}")
    private String master;

    @Value("${spark.hadoop.fs.s3a.access.key}")
    private String accessKey;

    @Value("${spark.hadoop.fs.s3a.secret.key}")
    private String secretKey;

    @Value("${spark.hadoop.fs.s3a.connection.ssl.enabled}")
    private String sslEnabled;

    @Value("${spark.hadoop.fs.s3a.session.token}")
    private String sessionToken;

    @Value("${spark.hadoop.fs.s3a.session.endpoint}")
    private String sessionEndpoint;

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(String sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public void setSessionToken(String sessionToken) {
        this.sessionToken = sessionToken;
    }

    public String getSessionEndpoint() {
        return sessionEndpoint;
    }

    public void setSessionEndpoint(String sessionEndpoint) {
        this.sessionEndpoint = sessionEndpoint;
    }
}

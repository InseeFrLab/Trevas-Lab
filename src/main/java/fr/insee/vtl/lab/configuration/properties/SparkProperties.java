package fr.insee.vtl.lab.configuration.properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkProperties {

    @Value("${spark.cluster.master}")
    private String master;

    @Value("${spark.dynamicAllocation.enabled}")
    private String dynamicAllocationEnabled;

    @Value("${spark.dynamicAllocation.min.executors}")
    private String dynamicAllocationMinExecutors;

    @Value("${spark.hadoop.fs.s3a.access.key}")
    private String accessKey;

    @Value("${spark.hadoop.fs.s3a.secret.key}")
    private String secretKey;

    @Value("${spark.hadoop.fs.s3a.connection.ssl.enabled}")
    private String sslEnabled;

    @Value("${spark.hadoop.fs.s3a.session.token}")
    private String sessionToken;

    @Value("${spark.hadoop.fs.s3a.endpoint}")
    private String sessionEndpoint;

    @Value("${spark.driver.memory}")
    private String driverMemory;

    @Value("${spark.executor.memory}")
    private String executorMemory;

    @Value("${spark.rpc.message.maxSize}")
    private Integer rpcMessageMaxSize;

    @Value("${spark.kubernetes.namespace}")
    private String kubernetesNamespace;

    @Value("${spark.kubernetes.executor.request.cores}")
    private String kubernetesExecutorRequestCores;

    @Value("${spark.kubernetes.driver.pod.name}")
    private String kubernetesDriverPodName;

    @Value("${spark.kubernetes.container.image}")
    private String kubernetesContainerImage;

    @Value("${spark.kubernetes.container.pullPolicy}")
    private String kubernetesContainerImagePullPolicy;

    public String getDynamicAllocationEnabled() {
        return dynamicAllocationEnabled;
    }

    public String getDynamicAllocationMinExecutors() {
        return dynamicAllocationMinExecutors;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public Integer getRpcMessageMaxSize() {
        return rpcMessageMaxSize;
    }

    public String getKubernetesNamespace() {
        return kubernetesNamespace;
    }

    public String getKubernetesContainerImage() {
        return kubernetesContainerImage;
    }

    public String getKubernetesContainerImagePullPolicy() {
        return kubernetesContainerImagePullPolicy;
    }

    public String getKubernetesExecutorRequestCores() {
        return kubernetesExecutorRequestCores;
    }

    public String getKubernetesDriverPodName() {
        return kubernetesDriverPodName;
    }

    public String getMaster() {
        return master;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getSslEnabled() {
        return sslEnabled;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public String getSessionEndpoint() {
        return sessionEndpoint;
    }

}

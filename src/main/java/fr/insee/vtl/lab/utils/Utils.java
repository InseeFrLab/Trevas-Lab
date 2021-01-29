package fr.insee.vtl.lab.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.configuration.properties.SparkProperties;
import fr.insee.vtl.spark.SparkDataset;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.script.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class Utils {

    private static final Logger logger = LogManager.getLogger(Utils.class);

    public static ScriptEngine initEngine(Bindings bindings) {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        ScriptContext context = engine.getContext();
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        return engine;
    }

    public static ScriptEngine initEngineWithSpark(Bindings bindings, SparkSession spark) {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        ScriptContext context = engine.getContext();
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        engine.put("$vtl.engine.processing_engine_names", "spark");
        engine.put("$vtl.spark.session", spark);
        return engine;
    }

    public static Bindings getBindings(Bindings input) {
        Bindings output = new SimpleBindings();
        input.forEach((k, v) -> {
            if (!k.startsWith("$")) output.put(k, v);
        });
        return output;
    }

    public static Bindings getBindings(Bindings input, Boolean toLength) {
        if (!toLength) return input;
        Bindings output = new SimpleBindings();
        input.forEach((k, v) -> {
            if (!k.startsWith("$")) {
                var size = ((fr.insee.vtl.model.Dataset) v).getDataPoints().size();
                output.put(k, size);
                logger.info(k + " dataset has size: " + size);
            }
        });
        return output;
    }

    public static void write(Bindings bindings, Bindings toSave,
                             SparkProperties sparkProperties, ObjectMapper objectMapper) {
        toSave.forEach((k, v) -> {
            SparkDataset dataset = (SparkDataset) bindings.get(k);
            Dataset<Row> sparkDataset = dataset.getSparkDataset();
            sparkDataset.write().mode(SaveMode.ErrorIfExists).parquet(v + "/parquet");
            String json = "";
            try {
                json = objectMapper.writeValueAsString(dataset.getDataStructure().values());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint(sparkProperties.getSessionEndpoint())
                            .credentials(sparkProperties.getAccessKey(), sparkProperties.getSecretKey())
                            .build();
            InputStream inputStream = new ByteArrayInputStream(json.getBytes());
            try {
                minioClient.putObject(
                        PutObjectArgs.builder().bucket(sparkProperties.getAccessKey())
                                .object(((String) v).replace("s3a://vtl/", "") + "/structure.json")
                                .stream(inputStream, json.getBytes().length, -1)
                                .build());
            } catch (ErrorResponseException e) {
                e.printStackTrace();
            } catch (InsufficientDataException e) {
                e.printStackTrace();
            } catch (InternalException e) {
                e.printStackTrace();
            } catch (InvalidKeyException e) {
                e.printStackTrace();
            } catch (InvalidResponseException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (XmlParserException e) {
                e.printStackTrace();
            }
        });
    }
}

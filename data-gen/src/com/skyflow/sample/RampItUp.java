package com.skyflow.sample;

import java.net.URI;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class RampItUp {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: java " + RampItUp.class.getName() + " <load-shape> <aws-region> <secret-name> <vault-id> <vault-url>");
            System.err.println("Generation Instructions:");
            System.err.println("  <load-shape>            : Semicolon separated list of rate,mins pairs: ");
            System.err.println("                              generate 'rate' (double) records for 'mins' (int) minutes");
            System.err.println("Vault Instructions:");
            System.err.println("  <aws-region>            : The AWS region where resources are located.");
            System.err.println("  <secret-name>           : The name of the secret in AWS Secrets Manager that stores the vault API key");
            System.err.println("  <vault-id>              : The ID of the vault to be accessed.");
            System.err.println("  <vault-url>             : The URL of the vault service.");
            System.exit(1);
        }
        String loadShape = args[0];
        String awsRegion = args[1];
        String secretName = args[2];
        String vault_id = args[3];
        String vault_url = args[4];

        // Retrieve Skyflow SA credential string from Secrets Manager
        String credentialString = getSecret(awsRegion, secretName);

        PoolingHttpClientConnectionManager connMgr = PoolingHttpClientConnectionManagerBuilder.create().setMaxConnTotal(1).build();
        HttpClientBuilder connBuilder = HttpClients.custom().setConnectionManager(connMgr);

        LoadRunner.run(loadShape, new Runnable() {
            @Override
            public void run() {
                CloseableHttpClient client = connBuilder.build();

                String insertRecordUrl = vault_url + "/v1/vaults/" + vault_id + "/ramp";

                String bodyString = "{\"records\":[{\"fields\":{\"field\":\"val\"}}]}";
                // Create an HTTP request
                ClassicHttpRequest request = ClassicRequestBuilder
                    .post(URI.create(insertRecordUrl))
                    .addHeader("Authorization", "Bearer " + credentialString)
                    .setEntity(bodyString, ContentType.APPLICATION_JSON)
                    .build();

                // Send the request and get the response
                // We NEED the synch call
                try (@SuppressWarnings("deprecation") CloseableHttpResponse response = client.execute(request);) {
                    if (response.getCode() != 200) {
                        String statusLine = response.getReasonPhrase();
                        throw new Exception("http response: " + statusLine);
                    }

                    // Read the response and discard
                    response.getEntity().getContent();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        });
    }

    private static String getSecret(String region, String secretName) {
        SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();

        GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
        return getSecretValueResponse.secretString();
    }
}

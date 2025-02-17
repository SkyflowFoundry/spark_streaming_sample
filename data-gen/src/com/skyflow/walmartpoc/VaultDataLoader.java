package com.skyflow.walmartpoc;

import org.apache.hc.core5.http.ContentType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.skyflow.utils.ReflectionUtils;
import com.skyflow.utils.ReflectionUtils.VaultObjectInfo;

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/* This is NOT a generalizable vault client. It only caches connections for one type of object,
 * and it doesn't "shut down cleanly". See close() mehod.
 */
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;

public class VaultDataLoader<T> implements AutoCloseable {
    private final String vault_id;
    private final String vault_url;
    private final String credentialString;
    private final VaultObjectInfo<T> classInfo;
    private final CloseableHttpClient httpClient;
    private final StatisticDatum apiLatency;
    private final ValueDatum apiError;

    public VaultDataLoader(String vault_id, String vault_url, int num_parallel_reqs, String vaultApiKey, CollectorAndReporter stats, Class<T> objectClass) throws Exception {
        this.vault_id = vault_id;
        this.vault_url = vault_url;
        this.credentialString = vaultApiKey;
        this.classInfo = ReflectionUtils.getVaultObjectInfo(objectClass);
        this.apiLatency = stats.createOrGetUniqueMetric("apiLatency", null, StandardUnit.MILLISECONDS, StatisticDatum.class);
        this.apiError = stats.createOrGetUniqueMetric("apiErrors", null, StandardUnit.COUNT, ValueDatum.class);
        this.httpClient = HttpClients.createDefault();
    }

    @SuppressWarnings("unchecked")
    public String loadObjectIntoVault(T object, boolean ignoreResponse) {
        JSONArray recordsArray = new JSONArray();
        JSONObject record = new JSONObject();
        record.put("table", classInfo.tableName);
        JSONObject fields = ReflectionUtils.jsonObjectForVault(object, classInfo);
        record.put("fields", fields);
        recordsArray.add(record);

        String insertRecordUrl = vault_url + "/v1/vaults/" + vault_id + "/" + classInfo.tableName;
        JSONObject insertReqBody = new JSONObject();
        insertReqBody.put("records", recordsArray);
        insertReqBody.put("tokenization", true);
        insertReqBody.put("upsert", classInfo.upsertColumnName);
        String insertReqBodyString = insertReqBody.toJSONString();

        HttpPost request = new HttpPost(insertRecordUrl);
        request.setHeader("Authorization", "Bearer " + credentialString);
        request.setEntity(new StringEntity(insertReqBodyString, ContentType.APPLICATION_JSON));

        final String requestId = java.util.UUID.randomUUID().toString();
        long startTime = System.currentTimeMillis();

        try (@SuppressWarnings("deprecation") CloseableHttpResponse response = httpClient.execute(request)) {
            long latency = (System.currentTimeMillis() - startTime);
            apiLatency.value(latency);

            if (response.getCode() != 200) {
                String statusLine = response.getReasonPhrase();
                String bodyString = EntityUtils.toString(response.getEntity());
                apiError.increment();
                throw new Exception("NOT-FOR-PRODUCTION: Request: " + requestId + " http response: " + statusLine + "\nResponse Body: " + bodyString + "\nRequest: " + insertReqBody.toJSONString());
            }

            if (ignoreResponse) {
                return "ignored";
            } else {
                String bodyString = EntityUtils.toString(response.getEntity());
                JSONParser jsonParser = new JSONParser();
                JSONObject insertResponse = (JSONObject) jsonParser.parse(bodyString);
                JSONArray responseRecords = (JSONArray) insertResponse.get("records");

                if (responseRecords.size() != 1) {
                    apiError.increment();
                    throw new RuntimeException("NOT-FOR-PRODUCTION: Request: " + requestId + " Skyflow was asked to upsert 1 record and returned " + responseRecords.size() + " results");
                }

                JSONObject responseRecord = (JSONObject) responseRecords.get(0);
                JSONObject extractedFields = (JSONObject) responseRecord.get("tokens");
                String skyflow_id = (String) responseRecord.get("skyflow_id");

                ReflectionUtils.replaceWithValuesFromVault(object, extractedFields, classInfo);
                return skyflow_id;
            }
        } catch (Exception e) {
            apiError.increment();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
	}
}
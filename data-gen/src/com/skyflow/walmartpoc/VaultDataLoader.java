package com.skyflow.walmartpoc;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.skyflow.common.utils.TokenUtils;
import com.skyflow.entities.InsertOptions;
import com.skyflow.entities.ResponseToken;
import com.skyflow.entities.SkyflowConfiguration;
import com.skyflow.entities.TokenProvider;
import com.skyflow.entities.UpsertOption;
import com.skyflow.serviceaccount.util.Token;
import com.skyflow.vault.Skyflow;


public class VaultDataLoader<T extends JsonSerializable> {

    public static class TokenGiver implements TokenProvider {

        private final long TOKEN_EXIPIRATION_GRACE_SEC = 0;

        private String bearerToken = null;
        private long renewAtTime;
        private final String credentialsString;

        public TokenGiver(String credentialsString) {
            this.credentialsString = credentialsString;
        }

        TokenGiver(Config config) throws Exception {
            this(new String(Files.readAllBytes(Paths.get(config.vault.private_key_file))));
        }

        @Override
        public String getBearerToken() throws Exception {
            long currentTime = new Date().getTime() / 1000;
            if(currentTime>this.renewAtTime) {
                ResponseToken response = Token.generateBearerTokenFromCreds(this.credentialsString);
                this.bearerToken = response.getAccessToken();
                this.renewAtTime = (long) TokenUtils.decoded(this.bearerToken).get("exp") - TOKEN_EXIPIRATION_GRACE_SEC;
            }
            return this.bearerToken;
        }
    }

    private final Skyflow skyflow_client;
    @SuppressWarnings("unused")
    private final int batch_size;
    private final String tableName;
    private final InsertOptions insertOptions;

    public VaultDataLoader(String vault_id, String vault_url, int max_rows_in_batch, TokenGiver access_token_generator, String tableName, String upsertColumnName) throws Exception {
        SkyflowConfiguration skyflowConfig = new SkyflowConfiguration(vault_id,
                                                                      vault_url,
                                                                      access_token_generator);
        this.skyflow_client = Skyflow.init(skyflowConfig);
        this.batch_size = max_rows_in_batch;
        this.tableName = tableName;
        UpsertOption[] upsertOptions = null;
        if (upsertColumnName!=null) {
            upsertOptions = new UpsertOption[1];
            upsertOptions[0] = new UpsertOption(tableName, upsertColumnName);
        }
        this.insertOptions = new InsertOptions(
            true, upsertOptions, false
        );
    }

    @SuppressWarnings("unchecked")
    public String loadObjectIntoVault(T object) throws Exception {
        // TBD make more efficient by doing this in a batch

        // construct insert input
        JSONObject records = new JSONObject();
        JSONArray recordsArray = new JSONArray();

        JSONObject record = new JSONObject();
        record.put("table", this.tableName);

        JSONObject fields = object.jsonObjectForVault();

        record.put("fields", fields);
        recordsArray.add(record);
        records.put("records", recordsArray);

        JSONObject insertResponse = this.skyflow_client.insert(records,insertOptions);

        JSONArray responseRecords = (JSONArray) insertResponse.get("records");
        JSONObject firstRecord = (JSONObject) ((JSONArray) responseRecords).get(0);
        JSONObject extractedFields = (JSONObject) firstRecord.get("fields");
        //System.out.println(extractedFields.toJSONString());

        // Extract the values from the JSONObject extractedFields and update the record with the new values
        object.replaceFieldsFromVault(extractedFields);

        return (String) extractedFields.get("skyflow_id");
    }
}
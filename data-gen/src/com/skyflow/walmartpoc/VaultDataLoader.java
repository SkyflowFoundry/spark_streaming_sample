package com.skyflow.walmartpoc;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.skyflow.entities.InsertOptions;
import com.skyflow.entities.SkyflowConfiguration;
import com.skyflow.entities.TokenProvider;
import com.skyflow.entities.UpsertOption;
import com.skyflow.vault.Skyflow;

import com.skyflow.utils.ReflectionUtils;
import com.skyflow.utils.ReflectionUtils.VaultObjectInfo;


public class VaultDataLoader<T> {
    private final Skyflow skyflow_client;
    @SuppressWarnings("unused")
    private final int batch_size;
    private final InsertOptions insertOptions;
    private final VaultObjectInfo<T> classInfo;

    public VaultDataLoader(String vault_id, String vault_url, int max_rows_in_batch, TokenProvider access_token_generator, Class<T> objectClass) throws Exception {

        SkyflowConfiguration skyflowConfig = new SkyflowConfiguration(vault_id,
                                                                      vault_url,
                                                                      access_token_generator);
        this.skyflow_client = Skyflow.init(skyflowConfig);

        this.batch_size = max_rows_in_batch;

        this.classInfo = ReflectionUtils.getVaultObjectInfo(objectClass);

        UpsertOption[] upsertOptions = null;
        if (classInfo.upsertColumnName!=null) {
            upsertOptions = new UpsertOption[1];
            upsertOptions[0] = new UpsertOption(classInfo.tableName, classInfo.upsertColumnName);
        }
        this.insertOptions = new InsertOptions(
            true, upsertOptions, false
        );
    }

    /**
     * Wrapper for @see {@link ReflectionUtils#jsonObjectForVault(Object, VaultObjectInfo)}
     */
    protected JSONObject jsonObjectForVault(T obj) {
        return ReflectionUtils.jsonObjectForVault(obj, classInfo);
    }

    /**
     * Wrapper for @see {@link ReflectionUtils#replaceWithValuesFromVault(Object, JSONObject, VaultObjectInfo, boolean)}
     */
    protected void replaceWithValuesFromVault(T obj, JSONObject vaultResponse) throws Exception {
        ReflectionUtils.replaceWithValuesFromVault(obj, vaultResponse, classInfo);
    }

    @SuppressWarnings("unchecked")
    public String loadObjectIntoVault(T object) throws Exception {
        // TBD make more efficient by doing this in a batch

        // construct insert input
        JSONObject records = new JSONObject();
        JSONArray recordsArray = new JSONArray();

        JSONObject record = new JSONObject();
        record.put("table", classInfo.tableName);

        JSONObject fields = jsonObjectForVault(object);

        record.put("fields", fields);
        recordsArray.add(record);
        records.put("records", recordsArray);

        JSONObject insertResponse = this.skyflow_client.insert(records,insertOptions);

        JSONArray responseRecords = (JSONArray) insertResponse.get("records");
        JSONObject firstRecord = (JSONObject) ((JSONArray) responseRecords).get(0);
        JSONObject extractedFields = (JSONObject) firstRecord.get("fields");
        //System.out.println(extractedFields.toJSONString());

        // Extract the values from the JSONObject extractedFields and update the record with the new values
        replaceWithValuesFromVault(object, insertResponse);

        return (String) extractedFields.get("skyflow_id");
    }
}
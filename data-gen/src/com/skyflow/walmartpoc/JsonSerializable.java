package com.skyflow.walmartpoc;

import org.json.simple.JSONObject;

public interface JsonSerializable {
    String toJSONString();
    JSONObject jsonObjectForVault();
    void replaceFieldsFromVault(JSONObject json);
}
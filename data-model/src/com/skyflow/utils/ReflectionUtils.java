package com.skyflow.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import com.skflow.iface.VaultColumn;
import com.skflow.iface.VaultObject;

public class ReflectionUtils {
    public static class VaultObjectInfo<T> {
        public String tableName;
        public Map<String, Field> vaultedFields;
        public String upsertColumnName;
        // add more as needed
    }

    public static <T> VaultObjectInfo<T> getVaultObjectInfo(Class<T> objectClass) {
        if (!objectClass.isAnnotationPresent(VaultObject.class)) {
            throw new IllegalArgumentException("The class " + objectClass.getName() + " must be annotated with @VaultObject.");
        }

        VaultObjectInfo<T> objectInfo = new VaultObjectInfo<T>();

        VaultObject vaultObjectAnnotation = objectClass.getAnnotation(VaultObject.class);
        objectInfo.tableName = vaultObjectAnnotation.value();

        objectInfo.vaultedFields = new HashMap<>();
        objectInfo.upsertColumnName = null;
        for (Field field : objectClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(VaultColumn.class)) {
                VaultColumn vaultColumn = field.getAnnotation(VaultColumn.class);
                String columnName = vaultColumn.value().isEmpty() ? field.getName().replaceAll("[^a-zA-Z0-9]", "").toLowerCase() : vaultColumn.value();
                objectInfo.vaultedFields.put(columnName, field);

                if (vaultColumn.upsertColumn()) {
                    if (objectInfo.upsertColumnName != null) {
                        throw new IllegalArgumentException("Multiple fields with upsertColumn set in class " + objectClass.getName() + " (Found " + objectInfo.upsertColumnName + " and " + columnName + ")");
                    }
                    objectInfo.upsertColumnName = columnName;
                }
            }
        }

        return objectInfo;
    }

    /**
     * Converts the given object into a JSONObject representation suitable for insertion into the vault.
     * This method maps each field annotated with @VaultColumn in the object to a corresponding key-value pair
     * in the JSONObject. Note that this method does not handle nested objects; all fields must be scalar types.
     *
     * @param obj the object to be converted into a JSONObject
     * @return a JSONObject containing the object's data mapped to vault column names
     * @throws RuntimeException if a field's value cannot be accessed
     */
    @SuppressWarnings("unchecked")
    public static <T> JSONObject jsonObjectForVault(T obj, VaultObjectInfo<T> vaultInfo) {
        JSONObject jsonObject = new JSONObject();
        for (Map.Entry<String, Field> entry : vaultInfo.vaultedFields.entrySet()) {
            String columnName = entry.getKey();
            Field field = entry.getValue();
            field.setAccessible(true);
            try {
                Object value = field.get(obj);
                jsonObject.put(columnName, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to access field value for " + columnName, e);
            }
        }
        return jsonObject;
    }


    /**
     * Replaces the fields of the given object with values from the provided vault response.
     * This method maps each field annotated with @VaultColumn in the object to a corresponding key-value pair
     * in the vault response JSONObject. Note that all fields must be scalar types as nested objects are not supported.
     *
     * @param obj the object whose fields are to be replaced with values from the vault response
     * @param vaultResponse the JSONObject containing the values to replace the object's fields
     * @throws RuntimeException if a field's value cannot be set or if not all fields are replaced when ensureAllReplaced is true
     */
    public static <T> void replaceWithValuesFromVault(T obj, JSONObject vaultResponse, VaultObjectInfo<T> vaultInfo) throws Exception {
        for (Map.Entry<String, Field> entry : vaultInfo.vaultedFields.entrySet()) {
            String columnName = entry.getKey();
            Field field = entry.getValue();
            field.setAccessible(true);
            try {
                if (vaultResponse.containsKey(columnName)) {
                    Object value = vaultResponse.get(columnName);
                    try {
                        field.set(obj, field.getType().cast(value));
                    } catch (Exception e) {
                        // This logs the value. Very helpful for debugging, not to be used in production.
                        throw new Exception("Setting field " + field.getName() + " of type " + field.getType().getName() + " with colname " + columnName + " to (" + value.getClass().getName() + "): " + value, e);
                    }
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to set field value for " + columnName, e);
            }
        }
    }
}

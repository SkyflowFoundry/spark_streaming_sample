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
import com.skyflow.serviceaccount.util.Token;
import com.skyflow.vault.Skyflow;


public class VaultDataLoader {

    static class TokenGiver implements TokenProvider {

        private final long TOKEN_EXIPIRATION_GRACE_SEC = 0;

        private String bearerToken = null;
        private long renewAtTime;
        private final String credentialsString;

        TokenGiver(Config config) throws Exception {
            String credentialsFile = config.vault.private_key_file;
            this.credentialsString = new String(Files.readAllBytes(Paths.get(credentialsFile)));
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

    public VaultDataLoader(Config config) throws Exception {
        TokenGiver access_token_generator = new TokenGiver(config);
        SkyflowConfiguration skyflowConfig = new SkyflowConfiguration(config.vault.vault_id,
                                                                      config.vault.vault_url,
                                                                      access_token_generator);
        this.skyflow_client = Skyflow.init(skyflowConfig);
    }

    @SuppressWarnings("unchecked")
    public String loadCustomerIntoVault(Customer customer) throws Exception {
        // TBD make more efficient by doing this in a batch

        
        // construct insert input
        JSONObject records = new JSONObject();
        JSONArray recordsArray = new JSONArray();

        JSONObject record = new JSONObject();
        record.put("table", "customeraccount");

        JSONObject fields = new JSONObject();
        fields.put("customerid", customer.custID);
        fields.put("firstname", customer.firstName);
        fields.put("lastname", customer.lastName);
        fields.put("email", customer.email);
        fields.put("phonenumber", customer.phoneNumber);
        fields.put("dateofbirth", customer.dateOfBirth);
        fields.put("addressline1", customer.addressLine1);
        fields.put("addressline2", customer.addressLine2);
        fields.put("addressline3", customer.addressLine3);

        record.put("fields", fields);
        recordsArray.add(record);
        records.put("records", recordsArray);

        // Indicates whether or not tokens should be returned for the inserted data. Defaults to 'True'
        InsertOptions insertOptions = new InsertOptions(
                    true, false
                );
        JSONObject insertResponse = this.skyflow_client.insert(records,insertOptions);

        JSONArray responseRecords = (JSONArray) insertResponse.get("records");
        JSONObject firstRecord = (JSONObject) ((JSONArray) responseRecords).get(0);
        JSONObject extractedFields = (JSONObject) firstRecord.get("fields");
        //System.out.println(extractedFields.toJSONString());

        // Extract the values from the JSONObject extractedFields and update the customer record with the new values
        customer.firstName = (String) extractedFields.get("firstname");
        customer.lastName = (String) extractedFields.get("lastname");
        customer.email = (String) extractedFields.get("email");
        customer.phoneNumber = (String) extractedFields.get("phonenumber");
        customer.dateOfBirth = (String) extractedFields.get("dateofbirth");
        customer.addressLine1 = (String) extractedFields.get("addressline1");
        customer.addressLine2 = (String) extractedFields.get("addressline2");
        customer.addressLine3 = (String) extractedFields.get("addressline3");

        return (String) extractedFields.get("skyflow_id");
    }

    @SuppressWarnings("unchecked")
    public String loadPaymentInfoIntoVault(PaymentInfo payment) throws Exception {
        // TBD make more efficient by doing this in a batch

        
        // construct insert input
        JSONObject records = new JSONObject();
        JSONArray recordsArray = new JSONArray();

        JSONObject record = new JSONObject();
        record.put("table", "customerpaymentdetails");

        JSONObject fields = new JSONObject();
        fields.put("paymentid", payment.paymentID);
        fields.put("creditcardnumber", payment.creditCardNumber);
        fields.put("card_cvv", payment.cardCVV);
        fields.put("cardholderfirstname", payment.cardHolderFirstName);
        fields.put("cardholderlastname", payment.cardHolderLastName);
        fields.put("cardholderphonenumber", payment.cardHolderPhoneNumber);
        fields.put("cardholderaddressline1", payment.cardHolderAddressLine1);
        fields.put("cardholderaddressline2", payment.cardHolderAddressLine2);
        fields.put("cardholderaddressline3", payment.cardHolderAddressLine3);
        fields.put("cardholderemail", payment.cardHolderEmail);
        fields.put("cardholderdateofbirth", payment.cardHolderDateOfBirth);

        record.put("fields", fields);
        recordsArray.add(record);
        records.put("records", recordsArray);

        // Indicates whether or not tokens should be returned for the inserted data. Defaults to 'True'
        InsertOptions insertOptions = new InsertOptions(
                    true, false
                );
        JSONObject insertResponse = this.skyflow_client.insert(records,insertOptions);

        JSONArray responseRecords = (JSONArray) insertResponse.get("records");
        JSONObject firstRecord = (JSONObject) ((JSONArray) responseRecords).get(0);
        JSONObject extractedFields = (JSONObject) firstRecord.get("fields");
        //System.out.println(extractedFields.toJSONString());

        // Extract the values from the JSONObject extractedFields and update the customer record with the new values
        payment.creditCardNumber = (String) extractedFields.get("creditcardnumber");
        payment.cardCVV = (String) extractedFields.get("card_cvv");
        payment.cardHolderFirstName = (String) extractedFields.get("cardholderfirstname");
        payment.cardHolderLastName = (String) extractedFields.get("cardholderlastname");
        payment.cardHolderPhoneNumber = (String) extractedFields.get("cardholderphonenumber");
        payment.cardHolderAddressLine1 = (String) extractedFields.get("cardholderaddressline1");
        payment.cardHolderAddressLine2 = (String) extractedFields.get("cardholderaddressline2");
        payment.cardHolderAddressLine3 = (String) extractedFields.get("cardholderaddressline3");
        payment.cardHolderEmail = (String) extractedFields.get("cardholderemail");
        payment.cardHolderDateOfBirth = (String) extractedFields.get("cardholderdateofbirth");

        return (String) extractedFields.get("skyflow_id");
    }

}
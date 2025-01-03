package com.skyflow.walmartpoc;

import java.util.Date;

import com.skyflow.common.utils.TokenUtils;
import com.skyflow.entities.ResponseToken;
import com.skyflow.serviceaccount.util.Token;

public class TokenProvider implements com.skyflow.entities.TokenProvider {

    private final long TOKEN_EXIPIRATION_GRACE_SEC = 0;

    private String bearerToken = null;
    private long renewAtTime;
    private final String credentialsString;

    public TokenProvider(String credentialsString) {
        this.credentialsString = credentialsString;
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

package com.skyflow.sample;

import java.time.Instant;
import java.util.UUID;

public class UUIDt6 {
    public static UUID generateType6UUID() {
        long timeMillis = Instant.now().toEpochMilli();
        
        // Get the time field values
        long timeForUUID = (timeMillis * 10000) + 0x01B21DD213814000L;
        
        long msb = timeForUUID << 32;
        msb = msb | ((timeForUUID & 0xFFFF00000000L) >> 16);
        msb = msb | 0x6000 | ((timeForUUID >> 48) & 0x0FFF);
        
        // Get random node ID
        long lsb = UUID.randomUUID().getLeastSignificantBits();
        
        return new UUID(msb, lsb);
    }
}

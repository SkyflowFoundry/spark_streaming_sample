package com.skyflow.walmartpoc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

public class LoadRunner {
    private static final int FALL_BEHIND_MARGIN_PCT=10;
    private static final Logger logger = LoggerFactory.getLogger(LoadRunner.class);
    
    @SuppressWarnings("restriction")
    public static void terminateOnInterruption() {
        // Register the signal handler for SIGHUP & SIGINT, if possible (needed on Linux machines; not on local Mac)
        try {
            sun.misc.Signal.handle(new sun.misc.Signal("HUP"), signal -> {
                System.out.println("SIGHUP received. Shutting down...");
                // Perform cleanup tasks if necessary
                System.exit(0); // Terminate the application
            });
            sun.misc.Signal.handle(new sun.misc.Signal("INT"), signal -> {
                System.out.println("SIGINT received. Shutting down...");
                // Perform cleanup tasks if necessary
                System.exit(0); // Terminate the application
            });
        } finally {}
        
    }
    public static long run(String loadShape, Runnable operation) {
        if (!loadShape.matches("^(\\d+(\\.\\d+)?,\\d+;)*(\\d+(\\.\\d+)?,\\d+)$")) {
            throw new IllegalArgumentException("Invalid load shape format. Expected format: 'rate,mins;rate,mins;...'");
        }

        long totalIters = 0;
        String[] loadShapeParts = loadShape.split(";");
        for (String part : loadShapeParts) {
            String[] rateAndMins = part.split(",");
            if (rateAndMins.length != 2) {
                throw new RuntimeException("Got invalid loadshape part: " + part);
            }
            try {
                double rate = Double.parseDouble(rateAndMins[0].trim());
                int mins = Integer.parseInt(rateAndMins[1].trim());
                long numIters = (long)(rate*mins*60);
                logger.info("Iterating {} times at {} per second for {} minutes.", numIters, rate, mins);
                run_at_rate(numIters,rate,operation);
                totalIters += numIters;
            } catch (NumberFormatException e) {
                logger.error("Invalid number format in load shape: " + part, e);
            }
        }
        return totalIters;
    }

    public static void run_at_rate(long numIters, double numPerSecond, Runnable operation) {
        RateLimiter throttler = RateLimiter.create(numPerSecond);

        long startTime = System.currentTimeMillis();
        long lastWarningTime = startTime; // ignore rates for the first few seconds
        for (long i = 0; i < numIters; i++) {
            throttler.acquire();

            operation.run();

            // Check if we are falling behind the desired rate limit.
            // Do not check if we just printed a warning less than 30s ago. Also ignore if we are within the first 1000 records.
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastWarningTime >= 30000 && i>=1000) {
                long elapsedTime = currentTime - startTime;
                double expectedTime = (i + 1) / numPerSecond * 1000;
                if (elapsedTime > expectedTime * (100.0 + FALL_BEHIND_MARGIN_PCT)/100.0) {
                    logger.warn("Falling more than {}% behind the desired rate limit. Num: {}. Desired rate: {} records/sec, Current rate: {} records/sec", FALL_BEHIND_MARGIN_PCT, i, numPerSecond, (i + 1) / (elapsedTime / 1000.0));
                    lastWarningTime = currentTime;
                }
            }
        }
        logger.info("Ran for {} minutes",(System.currentTimeMillis() - startTime)/1000.0/60);
    }
    public static int getNumber(String generationLoadShape) {
        if (!generationLoadShape.matches("^(\\d+(\\.\\d+)?,\\d+;)*(\\d+(\\.\\d+)?,\\d+)$")) {
            throw new IllegalArgumentException("Invalid generation load shape format. Expected format: 'rate,mins;rate,mins;...'");
        }

        int totalIterations = 0;
        String[] loadShapeParts = generationLoadShape.split(";");
        for (String part : loadShapeParts) {
            String[] rateAndMins = part.split(",");
            if (rateAndMins.length != 2) {
                throw new RuntimeException("Got invalid loadshape part: " + part);
            }
            try {
                double rate = Double.parseDouble(rateAndMins[0].trim());
                int mins = Integer.parseInt(rateAndMins[1].trim());
                totalIterations += (int)(rate * mins * 60);
            } catch (NumberFormatException e) {
                logger.error("Invalid number format in generation load shape: " + part, e);
            }
        }
        return totalIterations;
    }
}

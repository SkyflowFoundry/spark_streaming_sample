package com.skyflow.walmartpoc;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.async.methods.*;
import org.apache.hc.client5.http.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.support.BasicResponseConsumer;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;

public class SparkBatchProcessor {
    private final CloseableHttpAsyncClient httpClient;
    private final int maxOutstanding;
    private final int batchSize;
    private final Semaphore semaphore;
    private final String apiUrl;

    public SparkBatchProcessor(String apiUrl, int maxOutstanding, int batchSize) {
        this.httpClient = HttpAsyncClients.createDefault();
        this.maxOutstanding = maxOutstanding;
        this.batchSize = batchSize;
        this.semaphore = new Semaphore(maxOutstanding);
        this.apiUrl = apiUrl;
        this.httpClient.start();
    }

    public List<String> processPartition(List<String> payloads) {
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();

        // Process requests in mini-batches
        for (int i = 0; i < payloads.size(); i += batchSize) {
            int end = Math.min(i + batchSize, payloads.size());
            List<String> batch = payloads.subList(i, end);

            semaphore.acquireUninterruptibly();  // Wait if too many outstanding requests
            CompletableFuture<List<String>> future = sendBatchRequest(batch);
            futures.add(future);
        }

        // Wait for all requests to complete and flatten the response
        return futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private CompletableFuture<List<String>> sendBatchRequest(List<String> batch) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        String jsonPayload = toJson(batch);  // Convert batch to JSON
        SimpleHttpRequest request = SimpleHttpRequests.post(apiUrl);
        request.setBody(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));

        long startTime = System.nanoTime();  // Start latency measurement

        httpClient.execute(request, BasicResponseConsumer.create(), new FutureCallback<>() {
            @Override
            public void completed(SimpleHttpResponse response) {
                long latency = (System.nanoTime() - startTime) / 1_000_000;  // Convert to milliseconds
                System.out.println("API call latency: " + latency + " ms");

                semaphore.release();
                future.complete(parseResponse(response.getBodyText()));
            }

            @Override
            public void failed(Exception ex) {
                semaphore.release();
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                semaphore.release();
                future.completeExceptionally(new CancellationException("Request cancelled"));
            }
        });

        return future;
    }

    private String toJson(List<String> batch) {
        return "{\"requests\":" + batch.stream()
                .map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(",", "[", "]")) + "}";
    }

    private List<String> parseResponse(String responseBody) {
        // Assuming JSON response format: {"responses": ["resp1", "resp2", ...]}
        return Arrays.asList(responseBody.replaceAll("[^a-zA-Z0-9,]", "").split(","));
    }

    public void close() throws Exception {
        httpClient.close();
    }

    public static void main(String[] args) throws Exception {
        SparkBatchProcessor processor = new SparkBatchProcessor(
                "https://api.example.com/process", 10, 5);

        List<String> payloads = Arrays.asList(
                "data1", "data2", "data3", "data4", "data5",
                "data6", "data7", "data8", "data9", "data10");

        List<String> results = processor.processPartition(payloads);
        results.forEach(System.out::println);
        processor.close();
    }
}

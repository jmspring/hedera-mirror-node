package com.hedera.mirror.importer.downloader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RequesterPayBucketTest {
    private static final S3AsyncClient anonymousClient = getAnonymousClient();
    private static final S3AsyncClient authClient = getAuthenticatedClient();

    private static final String PUBLIC_BUCKET = "appy1";
    private static final String REQUESTER_PAYS_BUCKET = "appy1-requester-pays";
    private static final String REGION = "us-west-2";

    // Current state
    @Test
    void simpleRequestOnPublicBucketWorks() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(PUBLIC_BUCKET)
                .build();
        assertListResult(anonymousClient.listObjects(listRequest));
    }

    // During transitioning
    @Test
    void requesterPaysRequestOnPublicBucketWorks() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(PUBLIC_BUCKET)
                .requestPayer(RequestPayer.REQUESTER)
                .build();
        assertListResult(authClient.listObjects(listRequest));
    }

    // Desired final state
    @Test
    void requesterPaysRequestOnRequesterPaysBucketWorks() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(REQUESTER_PAYS_BUCKET)
                .requestPayer(RequestPayer.REQUESTER)
                .build();
        assertListResult(authClient.listObjects(listRequest));
    }

    @Test
    void simpleRequestOnRequesterPaysBucketFails() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(REQUESTER_PAYS_BUCKET)
                .build();
        CompletableFuture<ListObjectsResponse> response = anonymousClient.listObjects(listRequest);
        Assertions.assertThrows(ExecutionException.class, () -> {
            response.get();
        }, "software.amazon.awssdk.services.s3.model.S3Exception: Access Denied (Service: S3, Status Code: 403");
    }

    void assertListResult(CompletableFuture<ListObjectsResponse> response) throws Exception {
        List<S3Object> result = response.get().contents();
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("1.jpg", result.get(0).key());
    }

    private static S3AsyncClient getAnonymousClient() {
        return S3AsyncClient.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .region(Region.of(REGION))
                .build();
    }

    private static S3AsyncClient getAuthenticatedClient() {
        String accessKey = "";
        String secretKey = "";
        return S3AsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(REGION))
                .build();
    }
}

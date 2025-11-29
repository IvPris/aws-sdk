package org.example;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The module containing all dependencies required by the {@link Handler}.
 */
public class DependencyFactory {

    private DependencyFactory() {}

    /**
     * @param region the S3 client region
     *
     * @return an instance of S3Client
     */
    public static S3Client s3Client(Region region) {

        return S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .region(region)
                .build();
    }
}

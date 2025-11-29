package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Handler {

    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    private final S3Client clientEU;
    private final S3Client clientUS;
    private final S3Client clientAP;

    private final String bucketEU;
    private final String bucketUS;
    private final String bucketAP;

    private static final String fileName = "file.txt";

    public Handler() {
        clientEU = DependencyFactory.s3Client(Region.EU_WEST_1);
        clientUS = DependencyFactory.s3Client(Region.US_EAST_1);
        clientAP = DependencyFactory.s3Client(Region.AP_SOUTHEAST_1);

        this.bucketEU = "bucket-eu-" + System.currentTimeMillis();
        this.bucketUS = "bucket-us-" + System.currentTimeMillis();
        this.bucketAP = "bucket-ap-" + System.currentTimeMillis();
    }

    public void setup() {

        // Create three S3 buckets in different regions.
        // The buckets are created globally, and must be unique.
        // The region is defined by the S3 client region.
        createBucket(clientEU, bucketEU);
        createBucket(clientUS, bucketUS);
        createBucket(clientAP, bucketAP);

        // List the S3 buckets (region EU_WEST_1).
        // Any client can be used.
        listBuckets(clientEU, Region.EU_WEST_1);
    }

    public void latencyTest() {
        Map<String, Long> uploadLatency = new HashMap<>();
        Map<String, Long> downloadLatency = new HashMap<>();

        try {
            uploadLatency.put(Region.EU_WEST_1.toString(), uploadObject(clientEU, bucketEU));
            uploadLatency.put(Region.US_EAST_1.toString(), uploadObject(clientUS, bucketUS));
            uploadLatency.put(Region.AP_SOUTHEAST_1.toString(), uploadObject(clientAP, bucketAP));
        } catch (URISyntaxException e) {
            logger.error(e.getMessage());
        }

        try {
            downloadLatency.put(Region.EU_WEST_1.toString(), downloadObject(clientEU, bucketEU));
            downloadLatency.put(Region.US_EAST_1.toString(), downloadObject(clientUS, bucketUS));
            downloadLatency.put(Region.AP_SOUTHEAST_1.toString(), downloadObject(clientAP, bucketAP));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        System.out.println("\nUpload latency (ms)");
        uploadLatency.forEach((region, latency) -> System.out.println(region + ": " + latency));

        System.out.println("\nDownload Latency (ms)");
        downloadLatency.forEach((region, latency) -> System.out.println(region + ": " + latency));
    }

    public void tearDown() {
        // Clean-up the S3 buckets
        cleanUp(clientUS, bucketUS);
        cleanUp(clientEU, bucketEU);
        cleanUp(clientAP, bucketAP);

        // Close connection
        closeConnection(clientUS);
        closeConnection(clientEU);
        closeConnection(clientAP);
    }

    private static void createBucket(S3Client s3Client, String bucketName) {

        try {
            s3Client.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .build());
            logger.info("Creating bucket: {}", bucketName);

            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            logger.info("{} is ready.", bucketName);
        } catch (S3Exception e) {

            logger.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void listBuckets(S3Client s3Client, Region region) {
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets();
        List<Bucket> buckets = listBucketsResponse.buckets();

        logger.info("Buckets in region {}:", region);

        for (Bucket bucket : buckets) {
            try {
                // Get the bucket's actual region
                GetBucketLocationResponse locationResponse = s3Client.getBucketLocation(
                        GetBucketLocationRequest.builder()
                                .bucket(bucket.name())
                                .build()
                );

                String bucketRegion = locationResponse.locationConstraintAsString();

                // Buckets in Region us-east-1 have a LocationConstraint of null.
                if (bucketRegion == null) {
                    bucketRegion = "us-east-1";
                }

                if (bucketRegion.equalsIgnoreCase(region.id())) {
                    logger.info(bucket.name());
                }

            } catch (S3Exception e) {
                logger.error("Error checking bucket {}: {}", bucket.name(), e.awsErrorDetails().errorMessage());
            }
        }
    }

    private long uploadObject(S3Client s3Client, String bucket) throws URISyntaxException {

        Instant start = Instant.now();

        logger.info("Uploading object...");

        URL resourceUrl = getClass().getClassLoader().getResource(fileName);
        assert resourceUrl != null;
        Path filePath = Paths.get(resourceUrl.toURI());

        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(fileName)
                        .build(),
                RequestBody.fromFile(filePath));
        logger.info("Upload complete.");

        Instant end = Instant.now();
        return Duration.between(start, end).toMillis();
    }

    private long downloadObject (S3Client client, String bucketName) throws IOException {

        logger.info("Downloading object: {}", fileName);
        Instant start = Instant.now();
        ResponseInputStream<GetObjectResponse> response = client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build());

        // Save the file
        //Files.copy(response, Paths.get(bucketName + fileName), StandardCopyOption.REPLACE_EXISTING);

        Instant end = Instant.now();
        return Duration.between(start, end).toMillis();
    }

    private static void deleteObject(S3Client s3Client, String bucket) {

        logger.info("Deleting object: {}", fileName);
        DeleteObjectRequest deleteObjectRequest =
                DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(fileName)
                .build();

        s3Client.deleteObject(deleteObjectRequest);
        logger.info("Object {} has been deleted.", fileName);
    }

    private static void cleanUp(S3Client s3Client, String bucket) {

        logger.info("Cleaning up...");

        try {
            deleteObject(s3Client, bucket);

            logger.info("Deleting bucket: {}", bucket);
            DeleteBucketRequest deleteBucketRequest =
                    DeleteBucketRequest.builder().bucket(bucket).build();
            s3Client.deleteBucket(deleteBucketRequest);
            logger.info("Bucket {} has been deleted.", bucket);

        } catch (S3Exception e) {
            logger.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        logger.info("Cleanup complete");
    }

    private static void closeConnection(S3Client s3Client) {

        logger.info("Closing the connection to S3");
        s3Client.close();
        logger.info("Connection closed");
    }
}
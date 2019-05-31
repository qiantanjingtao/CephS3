package com.cszt;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import com.cszt.util.CephException;
import com.cszt.util.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Created by zjt on 2019/3/11.
 */
public class CephClient {
    private static Logger log = LoggerFactory.getLogger(CephClient.class);

    private static AccessControlList acl = new AccessControlList();
    private static AmazonS3Client s3Client;
    private static BucketLifecycleConfiguration configuration;
    private static BucketLifecycleConfiguration.Rule rule;

    /**
     * 需且只需要调用一次
     *
     * @throws CephException
     */
    public static void init() throws CephException {
        AWSCredentials credentials = new BasicAWSCredentials(ClientConfig.getAccessKey(), ClientConfig.getSecretKey());
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        rule = new BucketLifecycleConfiguration.Rule()
                .withId("Archive and then delete rule")
                .withFilter(new LifecycleFilter(new LifecycleTagPredicate(new Tag("archive", "true"))))
                .withExpirationInDays(ClientConfig.getExpireDays())
                .withStatus(BucketLifecycleConfiguration.ENABLED);

        configuration = new BucketLifecycleConfiguration()
                .withRules(Arrays.asList(rule));

        s3Client = new AmazonS3Client(credentials, clientConfig);
        s3Client.setEndpoint(ClientConfig.getEndPoint().startsWith("http") ? ClientConfig.getEndPoint() : "http://" + ClientConfig.getEndPoint());
        acl.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
        log.debug("CephClient.init.end.success");
    }

    public static boolean changeAllRule(int days)  throws CephException{
        int endBucketID = ClientConfig.getEndBucketId();
        for(int i=1; i<=endBucketID;i++) {
            Bucket bucket = null;
            String bucketName = "bucket"+String.valueOf(i);
            try {
                configuration = s3Client.getBucketLifecycleConfiguration(bucketName);

                if(configuration == null) {
                    throw new CephException(400, "this bucket has no rule to change");
                }
                s3Client.deleteBucketLifecycleConfiguration(bucketName);
                rule.setExpirationInDays(days);
                configuration = new BucketLifecycleConfiguration()
                        .withRules(Arrays.asList(rule));

                s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
                throw new CephException(500, "fileName or job name or creating date is null");
            }
        }
        return true;
    }

    public static boolean changeRule(String bucketName, int days)  throws CephException{
        try {
            configuration = s3Client.getBucketLifecycleConfiguration(bucketName);
            if(configuration == null) {
                throw new CephException(400, "this bucket has no rule to change");
            }
            s3Client.deleteBucketLifecycleConfiguration(bucketName);
            rule.setExpirationInDays(days);
            configuration = new BucketLifecycleConfiguration()
                    .withRules(Arrays.asList(rule));

            s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
        } catch (AmazonS3Exception e) {
            System.err.println(e.getErrorMessage());
            throw new CephException(500, "fileName or job name or creating date is null");
        }
        return true;
    }

    public static List<Bucket> listBucket() {
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println("hello");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(bucket.getName() + "\t" +
                    sdf.format(bucket.getCreationDate()));
        }
        return buckets;
    }

    public static String createBucket(String bucketName) {
        if (StringUtils.isNullOrEmpty(bucketName)) {
            return "error, Bucket name cannot be null\r\n";
        }

        if (checkBucket(bucketName)) {
            return "error, Bucket is existed\r\n";
        }

        Bucket bucket;
        try {
            bucket = s3Client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
        } catch (AmazonS3Exception e) {
            System.err.println(e.getErrorMessage());
            return "cannot create bucket\r\n";
        }
        if (bucket != null) {
            return "success\r\n";
        } else {
            return "cannot create bucket\r\n";
        }
    }

    // 只能调用一次
    public static Boolean preCreateBucket() {
        int endBucketID = ClientConfig.getEndBucketId();

        for(int i=1; i<=endBucketID;i++) {
            Bucket bucket = null;
            String bucketName = "bucket"+String.valueOf(i);
            try {
                bucket = s3Client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
            }
        }
        log.debug("all bucket has initialized");
        return true;
    }


    public static Boolean checkBucket(String bucketName) {
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket bucket : buckets) {
            if (Objects.equals(bucket.getName(), bucketName)) {
                return true;
            }
        }
        return false;
    }


    public static String deleteBucket(String bucketName) {
        if (!checkBucket(bucketName)) {
            return "Bucket name is null or not existed\r\n";
        }

        if (!bucketIsEmpty(bucketName)) {
            return "bucket contains object(s), cannot be deleted\r\n";
        }

        try {
            s3Client.deleteBucket(bucketName);
            return "success\r\n";
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            return "error\r\n";
        }
    }

    public static Boolean bucketIsEmpty(String bucketName) {
        ListObjectsV2Result result = s3Client.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        if (objects.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }

    public static PutObjectResult putObject(String bucketName, byte[] file, String fileName, String job, String birth) throws CephException {
        PutObjectResult result = null;
        if (StringUtils.isNullOrEmpty(fileName) || StringUtils.isNullOrEmpty(job) || StringUtils.isNullOrEmpty(birth)) {
            throw new CephException(500, "fileName or job name or creating date is null");
        }
        try {
            ObjectMetadata metaData = new ObjectMetadata();
            metaData.setContentLength(file.length);
            s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(file);
            String key = job + "/" + birth + "/" + fileName;
            PutObjectRequest request = new PutObjectRequest(bucketName, key, byteArrayInputStream, metaData).withAccessControlList(acl);
            result = s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            log.error("putObject.AmazonServiceException:", e);
            throw new CephException(500, e.getErrorMessage());
        }
        return result;
    }

    public static PutObjectResult putObject(String bucketName, String Path, String fileName, String job, String birth) throws CephException {
        PutObjectResult result = null;
        if (StringUtils.isNullOrEmpty(fileName) || StringUtils.isNullOrEmpty(job) || StringUtils.isNullOrEmpty(birth)) {
            throw new CephException(500, "fileName or job name or creating date is null");
        }
        try {
            File tempFile = new File(Path + File.separator + fileName);  // order to platform independent
            FileInputStream in = new FileInputStream(tempFile);
            ObjectMetadata metaData = new ObjectMetadata();
            byte[] bytes = IOUtils.toByteArray(in);
            metaData.setContentLength(bytes.length);
            s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            String key = job + "/" + birth + "/" + fileName;
            PutObjectRequest request = new PutObjectRequest(bucketName, key, byteArrayInputStream, metaData).withAccessControlList(acl);
            result = s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            log.error("putObject.AmazonServiceException:", e);
            throw new CephException(500, e.getErrorMessage());
        } catch (IOException e) {
            log.error("putObject.IOException:", e);
            throw new CephException(500, e.getMessage());
        }

        return result;
    }

    public static String getBucketName(String jobName) {
        int hash = jobName.hashCode();
        hash = hash > 0 ? hash : -hash;
        int pos = hash % ClientConfig.getEndBucketId();
        pos += 1;
        return "bucket" + pos;
    }

    public static String getObjectUrl(String fileName, String job, String birth) throws CephException {
        if (StringUtils.isNullOrEmpty(fileName)) {
            throw new CephException(500, "fileName不能为空");
        }
        String bucketName = getBucketName(job);
        log.debug("getObjectUrl.bucketName:" + bucketName);
        return getObjectUrl(bucketName, fileName);
    }

    public static String getObjectUrl(String bucketName, String fileName) {
        if (!checkBucket(bucketName)) {
            return null;
        }

        try {
            if (!s3Client.doesObjectExist(bucketName, fileName)) {
                return null;
            }
            return s3Client.getUrl(bucketName, fileName).toString();
        } catch (Exception e) {
            return null;
        }
    }

    public static Boolean deleteObject(String fileName, String job, String birth) {
        String bucketName = getBucketName(job);
        try {
            if (!s3Client.doesObjectExist(bucketName, fileName)) {
                return false;
            }
            s3Client.deleteObject(bucketName, fileName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    public static Boolean deleteObject(String bucketName, String fileName) {
        if (!checkBucket(bucketName)) {
            return false;
        }
        try {
            if (!s3Client.doesObjectExist(bucketName, fileName)) {
                return false;
            }
            s3Client.deleteObject(bucketName, fileName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static Boolean downloadObject(String path, String fileName, String job) {
        String bucketName = getBucketName(job);
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, fileName);
            s3Client.getObject(request, new File(path + File.separator + fileName));
            return true;
        } catch (Exception e) {
            log.error("downloadObject.error:", e);
            return false;
        }
    }

    public static Boolean downloadObject(String bucketName, String path, String fileName, String job) {
        if (!checkBucket(bucketName)) {
            return false;
        }
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, fileName);
            s3Client.getObject(request, new File(path + File.separator + fileName));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static Boolean moveObject(String sourceBucket, String sourceKey, String destBucket, String destKey) {
        Boolean isDelete = false;
        try {
            if (!s3Client.doesObjectExist(sourceBucket, sourceKey)) {
                return false;
            }
            if (s3Client.doesObjectExist(destBucket, destKey)) {
                return false;
            }
            s3Client.copyObject(sourceBucket, sourceKey, destBucket, destKey);
            isDelete = deleteObject(sourceBucket, sourceKey);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return isDelete;
    }

    public static void listObjectInBucket(String bucketName) {
        ObjectListing object_listing = s3Client.listObjects(bucketName);
        while (true) {
            for (Iterator<?> iterator =
                 object_listing.getObjectSummaries().iterator();
                 iterator.hasNext(); ) {
                S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
                System.out.println(summary);
            }
            // more object_listing to retrieve?
            if (object_listing.isTruncated()) {
                object_listing = s3Client.listNextBatchOfObjects(object_listing);
            } else {
                break;
            }
        }
    }

    public static void deleteMultiObject(String jobName) throws CephException {
        String bucketName = CephClient.getBucketName(jobName);
        ObjectListing objects = null;
        try {
            objects = s3Client.listObjects(bucketName, jobName);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            throw new CephException(500, e.getMessage());
        }

        do {
            for(S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
                try {
                    deleteObject(bucketName, s3ObjectSummary.getKey());
                } catch (AmazonServiceException e) {
                    e.printStackTrace();
                }
            }
        } while (objects.isTruncated());
    }

    public static void deleteMultiObject(String jobName, String birth) throws CephException {
        String bucketName = CephClient.getBucketName(jobName);
        jobName = jobName + "/" + birth;
        ObjectListing objects = null;
        try {
            objects = s3Client.listObjects(bucketName, jobName);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            throw new CephException(500, e.getMessage());
        }
        do {
            for(S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
                try {
                    deleteObject(bucketName, s3ObjectSummary.getKey());
                } catch (AmazonServiceException e) {
                    e.printStackTrace();
                }
            }
        } while (objects.isTruncated());
    }
}



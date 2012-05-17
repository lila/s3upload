package com.amazon.karanb.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Hello world!
 *
 */
public class App 
{

    public static ThreadPoolExecutor createDefaultExecutorService(int n) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private int threadCount = 1;

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("s3-transfer-manager-worker-" + threadCount++);
                return thread;
            }
        };
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(n, threadFactory);
    }
    
    public static void printUsage() {
        System.out.println("app <s3-path> <local-file> <numthreads>");
    }
    
    public static Boolean validateS3Url(String s3url) {
        return s3url.matches("s3://.*");
    }
    
    public static String extractS3Bucket(String s3url) {
        try {
            URI u = new URI(s3url);
            if (u != null && u.getScheme() != null && u.getScheme().equals("s3")) {
                return (u.getAuthority());
            }
        } catch (URISyntaxException e) {
        }
        return null;
    }

    public static String extractS3KeyName(String s3url) {
        try {
            URI u = new URI(s3url);
            if (u != null && u.getScheme() != null && u.getScheme().equals("s3")) {
                return (u.getPath().substring(1));
            }
        } catch (URISyntaxException e) {
            
        }
        return null;
    }
    
    public static Boolean validateFile(String filePath) {
        return true;
    }

    public static void main( String[] args )
    {
        if (args.length < 4) {
            printUsage();
            System.exit(-1);
        }
        String downloadS3Url      = args[0];
        String filePath           = args[1];
        int numThreads            = new Integer(args[2]);
        long uploadPartSize       = new Long(args[3]); 
        
        if (!validateS3Url(downloadS3Url)) {
            printUsage();
            System.exit(-1);
        }
        
        String bucketName         = extractS3Bucket(downloadS3Url);
        String keyName            = extractS3KeyName(downloadS3Url);

        if (!validateFile(filePath)) {
            printUsage();
            System.exit(-1);
        }

        TransferManager tm = null;
        TransferManagerConfiguration tmc = new TransferManagerConfiguration();
        tmc.setMinimumUploadPartSize(uploadPartSize);
        tmc.setMultipartUploadThreshold(5000000);
        
        try {
            InputStream i = App.class.getResourceAsStream("/AwsCredentials.properties");
            tm = new TransferManager(new AmazonS3Client(new PropertiesCredentials(i)), createDefaultExecutorService(numThreads));
            tm.setConfiguration(tmc);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        // For more advanced uploads, you can create a request object
        // and supply additional request parameters (ex: progress listeners,
        // canned ACLs, etc)
        PutObjectRequest request = new PutObjectRequest(
                bucketName, keyName, new File(filePath));


        // You can ask the upload for its progress, or you can
        // add a ProgressListener to your request to receive notifications
        // when bytes are transfered.

        request.setProgressListener(new ProgressListener() {
            long totalbytestransfered=0;
            long starttime = System.nanoTime();
            long numParts = 0;
            public void progressChanged(ProgressEvent event) {
                long b = addBytes(event.getBytesTransfered());
                if (event.getEventCode() == ProgressEvent.COMPLETED_EVENT_CODE) {
                    System.out.println("Transferred bytes: " +
                            b);
                    long now = System.nanoTime();
                    long diff = (now - starttime)/1000000000;
                    System.out.println("Time: " + diff);
                    System.out.println("Bandwidth = " + b/diff + " bytes/sec");
                } else if (event.getEventCode() == ProgressEvent.PART_STARTED_EVENT_CODE) {
                    long p = addPart();
                    System.out.println("part " + p + " started");
                } else if (event.getEventCode() == ProgressEvent.PART_COMPLETED_EVENT_CODE) {
                    System.out.println("part complete");
                    long now = System.nanoTime();
                    long diff = (now - starttime)/1000000000;
                    System.out.println("Bandwidth = " + b/diff + " bytes/sec");

                }
            }
            public synchronized long addBytes(long b) {
                totalbytestransfered += b;
                return totalbytestransfered;
            }
            public synchronized long addPart() {
                numParts++;
                return numParts;
            }
            
        });

        // TransferManager processes all transfers asynchronously,
        // so this call will return immediately.
        Upload upload = tm.upload(request);

        try {
            // You can block and wait for the upload to finish
            try {
                upload.waitForCompletion();
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        } catch (AmazonClientException amazonClientException) {
            System.out.println("Unable to upload file, upload aborted.");
            amazonClientException.printStackTrace();
        }
        System.exit(1);
    }

}


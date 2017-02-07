/**
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.storagetransfer.v1.Storagetransfer;
import com.google.api.services.storagetransfer.v1.StoragetransferScopes;
import com.google.api.services.storagetransfer.v1.model.AwsAccessKey;
import com.google.api.services.storagetransfer.v1.model.AwsS3Data;
import com.google.api.services.storagetransfer.v1.model.Date;
import com.google.api.services.storagetransfer.v1.model.GcsData;
import com.google.api.services.storagetransfer.v1.model.ListOperationsResponse;
import com.google.api.services.storagetransfer.v1.model.Operation;
import com.google.api.services.storagetransfer.v1.model.Schedule;
import com.google.api.services.storagetransfer.v1.model.TransferJob;
import com.google.api.services.storagetransfer.v1.model.TransferSpec;
import com.google.common.base.Preconditions;

/**
 * Example of using transfer api to move data from S3 to GCS.
 * 
 * @author taylorjason
 *
 */
public class S3toAWS {

	private static final String APP_NAME = "storagetransfer-example";
	private Logger log = Logger.getLogger(getClass().getCanonicalName());
	private String projectId;
	private String jobDescription;
	private String awsSourceBucket;
	private String gcsSinkBucket;
	private String awsAccessKeyId;
	private String awsSecretAccessKey;

	public static void main(String[] args) {
		Preconditions.checkArgument(args.length == 6, "Missing all parameters for transfer.");

		S3toAWS transfer = new S3toAWS(Preconditions.checkNotNull(args[0], "Missing project id"),
				Preconditions.checkNotNull(args[1], "Missing job description"),
				Preconditions.checkNotNull(args[2], "Missing aws source bucket"),
				Preconditions.checkNotNull(args[3], "Missing gcs destination bucket"),
				Preconditions.checkNotNull(args[4], "Missing aws access key id"),
				Preconditions.checkNotNull(args[5], "Missing aws secret access key"));
		try {
			transfer.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Entry point, initialize the job and start it. Displays response from
	 * transfer api.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public void start() throws IOException {
		log.info("Starting... " + Arrays.asList(projectId, jobDescription, awsSourceBucket, gcsSinkBucket,
				awsAccessKeyId, awsSecretAccessKey));

		TransferJob request = new TransferJob().setDescription(jobDescription + "-" + System.currentTimeMillis())
				.setProjectId(projectId).setTransferSpec(getTransferSpec()).setSchedule(getSchedule())
				.setStatus("ENABLED");

		log.info(request.toPrettyString());

		Storagetransfer client = createStorageTransferClient();
		// This submit operation is synchronous, the job is asynchronous.
		TransferJob response = client.transferJobs().create(request).execute();
		log.info("Request submitted, waiting for completion...");
		// Poll to check for completion.
		waitForCompletion(response, client);
		log.info("Data transfer has completed!");
	}
	
	private void sleepQuietly(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {}
	}

	private void waitForCompletion(TransferJob r, Storagetransfer client) throws IOException {
		while(!isOperationComplete(r.getProjectId(), r.getName(), client)) {
			sleepQuietly(2000);
			log.info("Waiting to complete... " + System.currentTimeMillis());
		}
	}
	
	/**
	 * HACK
	 * Determines if an operation is in progress by scanning the recent 256 operations.
	 * This is not ideal, ideally we would get just the op for the job.
	 * Due to a bug the filter job_names" : ["jobid1", "jobid2",...] cannot be applied
	 * This is because job id contains a '/' that must be encoded as %2F which the Java API
	 * disallows as per URI rfc.
	 * Also assuming one op for one job which may be incorrect. TBC
	 * @param projectId
	 * @param jobName
	 * @param client
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private boolean isOperationComplete(String projectId, String jobName, Storagetransfer client) throws IOException {
		Storagetransfer.TransferOperations.List request = client.transferOperations().list("transferOperations")
				.setFilter("{\"project_id\":\"" + projectId+ "\"}")
				.setPageSize(256);
		
		ListOperationsResponse response = request.execute();

		boolean completed = false;
		for(Operation op : response.getOperations()) {
			if(op.toPrettyString().contains(jobName) && op.toPrettyString().contains("IN_PROGRESS")) {
				break;
			}
			else if(op.toPrettyString().contains(jobName) && op.toPrettyString().contains("SUCCESS")) {
				completed = true;
			}
		}
		
		return completed;
	}

	private Schedule getSchedule() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, 1);
		return new Schedule()
				.setScheduleStartDate(new Date().setDay(cal.get(Calendar.DAY_OF_MONTH))
						.setMonth(cal.get(Calendar.MONTH) + 1).setYear(cal.get(Calendar.YEAR)))
				.setScheduleEndDate(new Date().setDay(cal.get(Calendar.DAY_OF_MONTH))
						.setMonth(cal.get(Calendar.MONTH) + 1).setYear(cal.get(Calendar.YEAR)));
	}

	private TransferSpec getTransferSpec() {
		return new TransferSpec()
				.setAwsS3DataSource(new AwsS3Data().setBucketName(awsSourceBucket)
						.setAwsAccessKey(new AwsAccessKey().setAccessKeyId(awsAccessKeyId)
								.setSecretAccessKey(awsSecretAccessKey)))
				.setGcsDataSink(new GcsData().setBucketName(gcsSinkBucket));
	}

	/**
	 * Reads the service key credentials from a file on the classpath and
	 * creates a client.
	 * 
	 * @return
	 * @throws IOException
	 */
	private Storagetransfer createStorageTransferClient() throws IOException {
		HttpTransport httpTransport = Utils.getDefaultTransport();
		JsonFactory jsonFactory = Utils.getDefaultJsonFactory();

		GoogleCredential credential = GoogleCredential
				.fromStream(getClass().getClassLoader().getResourceAsStream("service-account.json"));

		if (credential.createScopedRequired()) {
			credential = credential.createScoped(StoragetransferScopes.all());
		}

		HttpRequestInitializer initializer = new RetryHttpInitializerWrapper(credential);
		return new Storagetransfer.Builder(httpTransport, jsonFactory, initializer).setApplicationName(APP_NAME)
				.build();
	}

	public S3toAWS(String projectId, String jobDescription, String awsSourceBucket, String gcsSinkBucket,
			String awsAccessKeyId, String awsSecretAccessKey) {
		this.projectId = projectId;
		this.jobDescription = jobDescription;
		this.awsSourceBucket = awsSourceBucket;
		this.gcsSinkBucket = gcsSinkBucket;
		this.awsAccessKeyId = awsAccessKeyId;
		this.awsSecretAccessKey = awsSecretAccessKey;
	}

}

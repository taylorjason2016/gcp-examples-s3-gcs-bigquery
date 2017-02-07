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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.CsvOptions;
import com.google.api.services.bigquery.model.ExternalDataConfiguration;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;

/**
 * Maps a GCS file into a BigQuery temp table. Then uses the temp table to
 * create a permanent BigQuery table with a concat date appended as a column.
 * 
 * @author taylorjason
 *
 */
public class GCStoTempTable {

	private Logger log = Logger.getLogger(getClass().getCanonicalName());
	private String projectName;
	private String gcsBucketPath;
	private String targetTableName;
	private String dataset;
	private String schema;

	public static void main(String[] args) {
		Preconditions.checkArgument(args.length == 5, "Missing argument(s)");
		String projectName = Preconditions.checkNotNull(args[0], "Missing a project name.");
		String gcsBucketPath = Preconditions.checkNotNull(args[1], "Missing a gcs bucket name.");
		String targetTableName = Preconditions.checkNotNull(args[2], "Missing a target table name.");
		String dataset = Preconditions.checkNotNull(args[3], "Missing a dataset name.");
		String schema = Preconditions.checkNotNull(args[4], "Missing a schema.");
		GCStoTempTable gbq = new GCStoTempTable(projectName, gcsBucketPath, targetTableName, dataset, schema);
		try {
			gbq.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void start() throws IOException {
		log.info(Arrays.asList(projectName, gcsBucketPath, targetTableName, dataset, schema).toString());
		Bigquery client = createClient();
		createTempExternalTable(client);
	}

	private void createTempExternalTable(Bigquery client) throws IOException {
		TableReference ref = new TableReference();
		ref.setProjectId(projectName);
		ref.setDatasetId(dataset);
		ref.setTableId(targetTableName);

		Table content = new Table();
		content.setTableReference(ref);

		content.setExternalDataConfiguration(
				new ExternalDataConfiguration().setCsvOptions(new CsvOptions().setFieldDelimiter(","))
						.setAutodetect(false).setSourceFormat("CSV").setSchema(createSchema()).setSourceUris(Arrays.asList(gcsBucketPath)));

		client.tables().insert(ref.getProjectId(), ref.getDatasetId(), content).execute();
	}

	/**
	 * Builds a {@link TableFieldSchema} from the schema String expected as
	 * <fieldName> <fieldType>,<fieldName> <fieldType> e.g. flightNumber STRING,
	 * airline STRING, flightTime TIMESTAMP
	 * 
	 * @return
	 */
	private TableSchema createSchema() {
		ArrayList<TableFieldSchema> fieldSchema = new ArrayList<TableFieldSchema>();
		Arrays.stream(schema.split(",")).map(fieldDef -> fieldDef.trim().split(" ")).forEach(pair -> {
			fieldSchema.add(new TableFieldSchema().setName(pair[0]).setType(pair[1]));
			log.info("fieldName=" + pair[0] + " type=" + pair[1]);
		});
		return new TableSchema().setFields(fieldSchema);
	}

	/**
	 * Creates a bigquery client using the service account key file on the
	 * classpath.
	 * 
	 * @return
	 * @throws IOException
	 */
	private Bigquery createClient() throws IOException {
		HttpTransport httpTransport = Utils.getDefaultTransport();
		JsonFactory jsonFactory = Utils.getDefaultJsonFactory();

		GoogleCredential credential = GoogleCredential
				.fromStream(getClass().getClassLoader().getResourceAsStream("service-account.json"));

		if (credential.createScopedRequired()) {
			credential = credential.createScoped(Arrays.asList(BigqueryScopes.BIGQUERY));
		}

		return new Bigquery(httpTransport, jsonFactory, credential);
	}

	public GCStoTempTable(String projectName, String gcsBucketPath, String targetTableName, String dataset,
			String schema) {
		this.projectName = projectName;
		this.gcsBucketPath = gcsBucketPath;
		this.targetTableName = targetTableName;
		this.dataset = dataset;
		this.schema = schema;
	}

}

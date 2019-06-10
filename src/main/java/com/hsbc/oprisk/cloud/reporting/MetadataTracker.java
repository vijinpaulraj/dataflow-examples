package com.hsbc.oprisk.cloud.reporting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;

public class MetadataTracker {
	private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();;

	public static void insert(String jobId, String jobType, String sourceType, String sourcePath, String targetPath,
			String startTime, String endTime, String count, String datasetName, String tableName) {

		System.out.println("jobType" + jobType + "sourceType" + sourceType + "sourcePath" + sourcePath
				+ "targetPath" + targetPath + "startTime" + startTime + "endTime" + endTime + "count" + count
				+ "datasetName" + datasetName + "tableName" + tableName + "jobId" + jobId);
		TableId tableId = TableId.of("OPS_RISK", "GCP_METADATA_TRACKER");
		Map<String, Object> rowContent = new HashMap<>();
		
		 rowContent.put("MT_JOB_TYPE", jobType); 		 
		 rowContent.put("MT_JOB_ID", jobId); 
		 rowContent.put("MT_SOURCE_TYPE", sourceType); 
		 rowContent.put("MT_SOURCE_PATH", sourcePath);
		 rowContent.put("MT_TARGET_PATH", targetPath); 
		 rowContent.put("MT_START_TIME", startTime); 
		 rowContent.put("MT_END_TIME", endTime);
		 rowContent.put("MT_TOTAL_COUNT", count); 
		 rowContent.put("MT_TARGET_DATASET",datasetName); 
		 rowContent.put("MT_TARGET_TABLE", tableName);		 

		System.out.println("tableId====>" + tableId);

		System.out.println("rowContent====>" + rowContent);

		InsertAllRequest request = InsertAllRequest.newBuilder(tableId).addRow(rowContent).build();

		InsertAllResponse response = bigquery.insertAll(request);
		if (response.hasErrors()) {
			// If any of the insertions failed, this lets you inspect the errors
			for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
				// inspect row error
			}
		}		
	}
}
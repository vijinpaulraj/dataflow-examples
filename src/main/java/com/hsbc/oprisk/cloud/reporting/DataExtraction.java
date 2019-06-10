package com.hsbc.oprisk.cloud.reporting;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
//import com.google.cloud.bigquery.Job;
import com.google.api.services.dataflow.model.Job;

import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;

/*import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
*/
public class DataExtraction {
	
	private static Logger logger = LoggerFactory.getLogger(DataIngestion.class);

	public static final String FILE_EXTENSION = ".CSV";
	
	public static final String HEADER_TABLE_NAME = "DWS_EXTRACT_HEADER";
	
	public static final String EXTRACT_NAME = "EXTRACT_NAME";
	
	private static String query = "SELECT * FROM ";
	
	public static DataExtractionPipelineOptions options;
	
	private static List<Field> tableFieldSchema;
	
	public static void main(String[] args) throws Exception {
		
	    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataExtractionPipelineOptions.class);
	    
	    options.setRunner(DataflowRunner.class);
	    
	    tableFieldSchema = getTableFieldSchema(options);
	    	    
	    query += options.getBqDataset() + "." + options.getBqSrcTable();
	    
	    Pipeline p = Pipeline.create(options);

	    System.out.println("sql query" + query);
	    
	    String columnNames = "";
	    
	    if (options.getDefaultHeader()) {	    				
			for (Field fieldSchema : tableFieldSchema) {
				columnNames += fieldSchema.getName() + ",";
			}
			
			columnNames = stripTrailingComma(columnNames);			
	    } else {	    	
	    	String headerQuery = "SELECT * FROM " 
					+ options.getBqDataset()
					+ "."
					+ HEADER_TABLE_NAME
					+ " WHERE "
					+ EXTRACT_NAME + "='" + options.getExtractHeaderName() + "' LIMIT 1";			
	    	
	    	logger.info("header query=========>" + headerQuery);
	    	
	    	columnNames = getHeaderColumns(headerQuery);	   
	    }
	    
	    logger.info("column names ===================>" + columnNames);
	
		PCollection<TableRow> rows = p.apply(BigQueryIO.read()
				.fromQuery(query)
				.usingStandardSql()
				.withoutValidation());
		
		logger.info("tableFieldeSchema====before passing on===============>" + columnNames);
	   
	    PCollection<String> records = rows.apply(ParDo.of(new RowToStringConverter(tableFieldSchema))); 
	    
	    
	    System.out.println("target path====>" + options.getGcsTargetPath());
	    
		records.apply(TextIO.write().to(options.getGcsTargetPath())				
				.withHeader(columnNames)
				.withSuffix(FILE_EXTENSION)
				.withoutSharding()
				);
		
		PipelineResult result = p.run();
		
		String jobId = ((DataflowPipelineJob) result).getJobId();	
		
		DataflowClient client = DataflowClient.create(options);
		
		logger.info("jobId======>" + jobId);
		
		switch (result.waitUntilFinish()) {
			case CANCELLED:
				break;
			case DONE:
				
				//System.out.println("The status is done");
				try {
					Job job = client.getJob(jobId);
					MetadataTracker.insert(job.getId().toString(), "DATA EXTRACTION", "CSV", "", "", job.getCreateTime().toString(), job.getCurrentStateTime().toString(), "2134", options.getBqDataset(), options.getBqSrcTable());
					//logger.info("The extraction job finished successfully!");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.info("The extraction job finished successfully!");
			case FAILED:
				break;
			case RUNNING:
				break;
			case STOPPED:
				break;
			case UNKNOWN:
				break;
			case UPDATED:
				break;
			default:
				break;			
		}
	}
	
	public static String getHeaderColumns(String query) throws Exception {
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(query)	      
	            .setUseLegacySql(false)
	            .build();
	    
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    com.google.cloud.bigquery.Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    String columnNames = "";
	    // Wait for the query to complete.
	    queryJob = queryJob.waitFor();

	    // Check for errors
	    if (queryJob == null) {
	      throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	      // You can also look at queryJob.getStatus().getExecutionErrors() for all
	      // errors, not just the latest one.
	      throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }

	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);

	    QueryResult result = response.getResult();

	    // Print all pages of the results.
	    while (result != null) {
	      for (List<FieldValue> row : result.iterateAll()) {
	    	  logger.info("row value ======>" + row);
	    	  //index 1 holds the HEADER value from DWS_EXTRACT_HEADER
	    	  columnNames = row.get(1).getStringValue();	      
	      }

	      result = result.getNextPage();
	    }
	    
	    return columnNames;
	}
	
	
	private static class RowToStringConverter extends DoFn<TableRow, String> {

		private static final long serialVersionUID = 1L;
		
		private List<Field> tableFieldSchema;
		
		public RowToStringConverter(List<Field> schema) {
			this.tableFieldSchema = schema;
		}
		
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			TableRow row = c.element();
			String line = "";			

			logger.info("tableFieldSchema =>" + tableFieldSchema);

			for (Field fieldSchema : tableFieldSchema) {
				line += row.get(fieldSchema.getName()) + ",";
			}

			c.output(stripTrailingComma(line));
		}		
	}
	
	private static List<Field> getTableFieldSchema(DataExtractionPipelineOptions pipelineOptions) {
		Table table = BigQueryOptions.getDefaultInstance().getService().getTable(pipelineOptions.getBqDataset(), pipelineOptions.getBqSrcTable());
		
		Schema schema = table.getDefinition().getSchema();

		return schema.getFields();
	}
	
	public static String stripTrailingComma(String str) {
		return str.substring(0, str.length() - 1);
	}
	
	
}




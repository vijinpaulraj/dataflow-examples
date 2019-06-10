package com.hsbc.oprisk.cloud.reporting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.dataflow.model.Job;
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.api.services.dataflow.model.Job;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
//import com.google.cloud.dataflow.sdk.Pipeline;
//import com.google.cloud.dataflow.sdk.io.BigQueryIO;
//import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
//import com.google.cloud.dataflow.sdk.io.TextIO;
//import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
//import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
//import com.google.cloud.dataflow.sdk.transforms.DoFn;
//import com.google.cloud.dataflow.sdk.transforms.ParDo;
//import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
	
public class DataIngestion {

	private static Logger logger = LoggerFactory.getLogger(DataIngestion.class);

	public static DataIngestionPipelineOptions options;			
	
	//private static boolean isFirstRow = true;
	
	//private static List<String> columnNames = new ArrayList<String>();

	public static void main(String[] args) {

		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataIngestionPipelineOptions.class);
		
		options.setRunner(DataflowRunner.class);

		Pipeline p = Pipeline.create(options);		
		
		//PipelineOptions pOptions = p.getOptions();
		
		//System.out.println("options id" + pOptions.getOptionsId());
		
		Table table = BigQueryOptions.getDefaultInstance().getService().getTable(options.getBqDataset(), options.getBqTargetTable());

		Schema schema = table.getDefinition().getSchema();

		List<Field> tableFieldSchema = schema.getFields();

		List<String> columnNames = new ArrayList<String>();

		logger.info("tableFieldSchema =>" + tableFieldSchema);

		for (Field fieldSchema : tableFieldSchema) {
			columnNames.add(fieldSchema.getName());
		}
		
		logger.info("column names====>"+ columnNames);
		
		logger.info(String.format("The job will be executed on project: %1$s dataset: %2$s table: %3$s",
				options.getProject(), options.getBqDataset(), options.getBqTargetTable()));
		
		p.apply("ReadCSVFromCloudStorage", TextIO.read().from(options.getInputFile()))	
		 .apply(ParDo.of(new StringToRowConverter(columnNames)))
		 .apply(BigQueryIO.writeTableRows()
				.to(options.getProject().trim() + ":" + options.getBqDataset().trim() + "." + options.getBqTargetTable().trim())
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
				.withWriteDisposition(getWriteDisposition (options)));

	
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
					//logger.info("Job Details================>" + job.getId(), job.getName(), job.getCreateTime());
					//logger.info("Job create time =============>" + job.getCreateTime());
					//logger.info("Job object================>" + job);
					MetadataTracker.insert(job.getId().toString(), "DATA INGESTION", "CSV", options.getInputFile(), options.getOutput() == null ? "" : options.getOutput(), job.getCreateTime().toString(), job.getCurrentStateTime().toString(), "45356", options.getBqDataset(), options.getBqTargetTable());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
		//MetadataTracker.insert(id, jobType, sourceType, sourcePath, targetPath, startTime, endTime, count, datasetName, tableName);			
		
		//MetadataTracker.insert(pOptions.getOptionsId(), "DATA INGESTION", "CSV", options.getInputFile(), "", new DateTime(12312), new DateTime(12324), 434343, options.getBqDataset(), options.getBqTargetTable());
	}
	
	private static WriteDisposition getWriteDisposition (DataIngestionPipelineOptions pipelineOptions) {
		WriteDisposition disposition = BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
		if (pipelineOptions.getWriteDisposition() == "TRUNCATE") {
			disposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
		}
		return disposition;
	}
	
	private static class StringToRowConverter extends DoFn<String, TableRow> {

		private static final long serialVersionUID = 1247092043533780997L;
		
		private List <String> columnList;
		
		public StringToRowConverter(List<String> columns) {
			this.columnList = columns;
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			//String[] words = c.element().split(",");
			//String columnValue = "";
			
			String line = c.element();
			CSVParser csvParser = new CSVParser();
			String[] words = csvParser.parseLine(line);
			
			TableRow row = new TableRow();			
			logger.info("column size======>" + columnList.size());
			for (int i = 0; i < columnList.size(); i++) {
				logger.info("column name ===>value" + columnList.get(i) + words[i]);
				/*if (words[i].startsWith("\"") && words[i].endsWith("\"")) {
					columnValue = words[i].replace("\"","");
				}*/
				row.set(columnList.get(i), /*columnValue*/ words[i]);
			}

			c.output(row);
		}
		
	}

}

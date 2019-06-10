package com.hsbc.oprisk.cloud.reporting;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;



public interface DataIngestionPipelineOptions extends DataflowPipelineOptions, GcsOptions {

	@Description("Path of the file to read from")
	String getInputFile();
	void setInputFile(String value);

	@Description("Path of the file to write to")	
	String getOutput();
	void setOutput(String value);

	/*@Description("Name of the project")
	@Validation.Required
	
	@Default.String("oprisk-reporting-on-google-cloud") 
	String getProject();
	void setProject(String value); */

	@Description("Name of the dataset")
	@Validation.Required
	String getBqDataset();
	void setBqDataset(String value);

	@Description("Name of the table")
	@Validation.Required
	String getBqTargetTable();
	void setBqTargetTable(String value);
	
	@Description("Write disposition")
	@Default.String("APPEND")
	String getWriteDisposition();
	void setWriteDisposition(String value);	
}
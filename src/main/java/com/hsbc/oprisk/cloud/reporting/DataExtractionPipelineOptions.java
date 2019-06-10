package com.hsbc.oprisk.cloud.reporting;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

//import com.google.cloud.dataflow.sdk.options.Description;
//import com.google.cloud.dataflow.sdk.options.PipelineOptions;
//import com.google.cloud.dataflow.sdk.options.Validation;

public interface DataExtractionPipelineOptions extends DataflowPipelineOptions, GcsOptions {
	@Description("Name of the source table")
	@Validation.Required
	String getBqSrcTable();
	void setBqSrcTable(String value);
	
	@Description("Name of the dataset")
	@Validation.Required
	String getBqDataset();
	void setBqDataset(String value);
	
	@Description("Target Path")
	@Validation.Required
	String getGcsTargetPath();
	void setGcsTargetPath(String value);
	
	@Description("Temporary location")
	String getTempLocation();
	void setTempLocation(String value);
	
	@Description("Name of the extrct header name")
	String getExtractHeaderName();
	void setExtractHeaderName(String value);
	
	@Description("Is the default header (table header)")
	@Validation.Required
	boolean getDefaultHeader();
	void setDefaultHeader(boolean value);
}

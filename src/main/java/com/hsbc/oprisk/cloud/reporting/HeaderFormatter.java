package com.hsbc.oprisk.cloud.reporting;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class HeaderFormatter extends DoFn<String, String> {
	
	private static final long serialVersionUID = 1L;
	
	String newLine = "\n";
    String line = "EVENT_ID" + "," + "EVENT_DESC" + "," + "EVENT_TYPE" + "," + "EVENT_LOSS_AMT"; 
    String csvHeader = line + newLine;
    StringBuilder csvBody = new StringBuilder().append(csvHeader);

    
	public HeaderFormatter (DataExtractionPipelineOptions options) {
		
	}
	
    @Override
    public void processElement(ProcessContext process) {
    	csvBody.append(process.element()).append(newLine);
    }

    @Override
    public void finishBundle(Context context) throws Exception {
        context.output(csvBody.toString());
    }	
}

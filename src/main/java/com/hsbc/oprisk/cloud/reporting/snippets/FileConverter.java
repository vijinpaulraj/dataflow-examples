package com.hsbc.oprisk.cloud.reporting.snippets;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Scanner;

import org.apache.beam.sdk.io.FileSystems;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

public class FileConverter {

	public static void main(String[] args) throws IOException {
		PipelineOptions options = PipelineOptionsFactory.create();		
		
		Pipeline p = Pipeline.create(options);
		
		ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource("gs://oprisk_inbound_bucket/tmp/test.txt", false));
		try (InputStream stream = Channels.newInputStream(channel)) {
			String myFileInString = getFileString(stream);
			System.out.println(myFileInString);
		}
		
		p.run();

	}
	
	public static String getFileString (InputStream input) {
		if (input == null) {
			return "";
		}
		
		Scanner scnr = new Scanner (input);
		
		scnr.useDelimiter("\\A");
		
		String outputString = scnr.hasNext() ? scnr.next() : "";
		
		scnr.close();
		
		return outputString;
		
	}

}

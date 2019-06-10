package com.hsbc.oprisk.cloud.reporting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class StringToRowConverter extends DoFn<String, TableRow> {

	private static final long serialVersionUID = 1247092043533780997L;
	
	private static Logger logger = LoggerFactory.getLogger(StringToRowConverter.class);
	
	private static boolean isFirstRow = true;
	
	private static String[] columnNames;

	//private List<String> columnNames = new ArrayList<String>();

	/*public StringToRowConverter(DataIngestionPipelineOptions options) {		
		Table table = BigQueryOptions.getDefaultInstance().getService().getTable(options.getDataset(), options.getTable());

		Schema schema = table.getDefinition().getSchema();

		List<Field> tableFieldSchema = schema.getFields();

		List<String> columnNames = new ArrayList<String>();

		//logger.info("tableFieldSchema =>" + tableFieldSchema);

		for (Field fieldSchema : tableFieldSchema) {
			columnNames.add(fieldSchema.getName());
		}
	}*/

	/*public void processElement(ProcessContext c) {
		if (this.columnNames != null) {
			String[] words = c.element().split(",");
			
			TableRow row = new TableRow();

			//logger.info("columnNames =>" + columnNames);
			
			System.out.println("element value =======================>" + c.element());
			
			System.out.println("column names=======>" + this.columnNames);

			for (int i = 0; i < this.columnNames.size(); i++) {
				System.out.println("process Element=================> column names="+ this.columnNames.get(i) + "<====>value: "+ words[i]);
				row.set(this.columnNames.get(i), words[i]);
			}
			
			

			c.output(row);
		}		
	}*/
	public void processElement(ProcessContext c) {
        TableRow row = new TableRow();

        String[] parts = c.element().split(",");
               
        
        if (isFirstRow) {
            columnNames = Arrays.copyOf(parts, parts.length);
            isFirstRow = false;
        } else {
        	//System.out.println("parts.length=========>" + parts.length);
            for (int i = 0; i < parts.length; i++) {
            	//System.out.println("columnNames and Parts" + Arrays.toString(columnNames) + Arrays.toString(parts));
            	if (columnNames[i] != null && parts[i] != null) {
            		row.set(columnNames[i], parts[i]);
            	}                               
            }
            c.output(row);
        }
    }

}
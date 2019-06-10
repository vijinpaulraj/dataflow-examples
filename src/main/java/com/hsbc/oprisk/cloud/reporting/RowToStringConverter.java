package com.hsbc.oprisk.cloud.reporting;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class RowToStringConverter extends DoFn<TableRow, String> {

	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(StringToRowConverter.class);

	private String tableName;
	private String datasetName;

	public RowToStringConverter(DataExtractionPipelineOptions options) {
		this.tableName = options.getBqSrcTable();
		this.datasetName = options.getBqDataset();
	}

	public void processElement(ProcessContext c) {
		TableRow row = c.element();
		String line = "";
		Table table = BigQueryOptions.getDefaultInstance().getService().getTable(this.datasetName, this.tableName);

		Schema schema = table.getDefinition().getSchema();

		List<Field> tableFieldSchema = schema.getFields();

		logger.info("tableFieldSchema =>" + tableFieldSchema);

		for (Field fieldSchema : tableFieldSchema) {
			line += row.get(fieldSchema.getName()) + ",";
		}

		c.output(removeTrailingComma(line));
	}

	public static String removeTrailingComma(String line) {
		return line.replaceAll(", $", "");
	}
}

package com.ksnhr.dataflow_gs_to_bq.converter;

import java.util.Arrays;
import com.ksnhr.dataflow_gs_to_bq.dateformat.DateFormat;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import java.lang.ArrayIndexOutOfBoundsException;


public class BigQueryRowConverter extends DoFn<String,TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String[] split = c.element().split(",");
        // avoid CSV's header
        if (!(Arrays.asList(split).contains("msg_count"))) {
            try {
                TableRow output = new TableRow();
                output.set("message_count", split[0]);
                output.set("time", DateFormat.convert(split[1]) + "-05:00");
                output.set("speed", split[2]);
                output.set("segment_id", split[3]);
                output.set("bus_count", split[4]);
                c.output(output);
            } catch (ArrayIndexOutOfBoundsException e) {
                ;
            }
        }
    }
}

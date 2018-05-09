package com.ksnhr.dataflow_gs_to_bq.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;

public class ChicagoTrafficSchema {
    public static TableSchema create() {
        List<TableFieldSchema> fields;
        fields = new ArrayList<> ();
        fields.add(new TableFieldSchema().setName("time").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("segment_id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("bus_count").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("message_count").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("speed").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }
}

//

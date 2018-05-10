package com.ksnhr.dataflow_gs_to_bq;


import com.google.api.services.bigquery.model.TableSchema;
import com.ksnhr.dataflow_gs_to_bq.converter.BigQueryRowConverter;
import com.ksnhr.dataflow_gs_to_bq.schema.ChicagoTrafficSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;


public class GsToBigQuery {

    public interface GsToBigQueryOptions extends PipelineOptions {
        @Description("Data source file path on Cloud Storage")
        @Default.String("gs://ksnhr_chicago_traffic_historical_data/Chicago_Traffic_Tracker_-_Historical_Congestion_Estimates_by_Segment.csv")
        String getFilePath();
        void setFilePath(String value);

        @Description("Target Big Query table")
        @Default.String("ksnhr-tech:chicago_traffic.historical_data")
        String getBigQueryTable();
        void setBigQueryTable(String value);
    }

    static void gsToBigQuery(GsToBigQueryOptions options) {
        Pipeline p = Pipeline.create(options);
        TableSchema schema = ChicagoTrafficSchema.create();
        p.apply(TextIO.read().from(options.getFilePath()))
                .apply(ParDo.of(new BigQueryRowConverter()))
                .apply(BigQueryIO.writeTableRows()
                        .to(options.getBigQueryTable())
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run();
    }

    public static void main(String[] args) {
        GsToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(GsToBigQueryOptions.class);
        gsToBigQuery(options);
    }
}

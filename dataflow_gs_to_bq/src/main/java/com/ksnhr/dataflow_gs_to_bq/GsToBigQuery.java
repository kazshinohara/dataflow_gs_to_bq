package com.ksnhr.dataflow_gs_to_bq;


import com.google.api.services.bigquery.model.TableSchema;
import com.ksnhr.dataflow_gs_to_bq.converter.BigQueryRowConverter;
import com.ksnhr.dataflow_gs_to_bq.schema.ChicagoTrafficSchema;
import java.util.ResourceBundle;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;



public class GsToBigQuery {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ResourceBundle config = ResourceBundle.getBundle("config");
        GsToBigQuery(config);
    }

    private static void GsToBigQuery(ResourceBundle config) {
        // create option
        DataflowPipelineOptions options = PipelineOptionsFactory.create()
                .as(DataflowPipelineOptions.class);
        options.setProject(config.getString("targetProject"));
        options.setStagingLocation(config.getString("targetStagingLocation"));
        options.setTempLocation(config.getString("targetTempLocation"));
        options.setRunner(DataflowRunner.class);
        options.setStreaming(true);
        options.setJobName(config.getString("targetJobName"));
        // create pipeline & get inputData
        Pipeline p = Pipeline.create(options);
        TableSchema schema = ChicagoTrafficSchema.create();
        p.apply(TextIO.read().from(config.getString("targetFilePath")))
                .apply(ParDo.of(new BigQueryRowConverter()))
                .apply(BigQueryIO.writeTableRows()
                        .to(config.getString("targetBigQueryTable"))
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run();
    }
}

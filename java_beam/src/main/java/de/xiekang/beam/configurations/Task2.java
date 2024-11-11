package de.xiekang.beam.configurations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.logging.Logger;

public class Task2 {
    static final Logger LOG = Logger.getLogger(Task2.class.getName());

    public interface MyOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("src/main/resources/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("src/main/resources/output.txt")
        String getOutput();

        void setOutput(String value);
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.info(prefix + ": " + c.element());
        }
    }

    static void readLines(MyOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> output = pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(Filter.by(line -> !line.isEmpty()));
        output.apply("Log", ParDo.of(new LogOutput<String>()));
        pipeline.run().waitUntilFinish();
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        readLines(options);
    }
}

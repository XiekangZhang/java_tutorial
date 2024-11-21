package de.xiekang.beam.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * it supports min, max as well.
 * **/

public class Task4 {
    private final static Logger LOGGER = LogManager.getLogger(Task4.class.getName());

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // Create input PCollection
        PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        PCollection<KV<String, Double>> mapPCollection = pipeline.apply(Create.of(
                KV.of("1", 1.0),
                KV.of("2", 2.0),
                KV.of("1", 1.5),
                KV.of("3", 3.1),
                KV.of("2", 2.1)));

        input.apply(Mean.globally()).apply("Log", ParDo.of(new LogOutput<>("processing mean")));
        mapPCollection.apply(Mean.perKey()).apply("Log per key", ParDo.of(new LogOutput<>("processing mean on key")));

        pipeline.run().waitUntilFinish();

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
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info(prefix + ": {}", c.element());
        }
    }
}

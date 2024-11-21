package de.xiekang.beam.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class Task5 {
    private final static Logger LOGGER = LogManager.getLogger(Task5.class.getName());

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        public LogOutput() {
            this.prefix = "Processing element";
        }

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info(prefix + ": {}", c.element());
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> input = pipeline.apply(Create.of("Hello", "World", "Apache", "Beam"));
        input.apply(WithKeys.of(new SerializableFunction<String, Integer>() {
            public Integer apply(String word) {
                return word.length();
            }
        })).apply("Log", ParDo.of(new LogOutput<>("withKey functions")));

        input.apply(WithKeys.of("my keys")).apply("Log", ParDo.of(new LogOutput<>("withkey specific key")));

        pipeline.run().waitUntilFinish();
    }
}

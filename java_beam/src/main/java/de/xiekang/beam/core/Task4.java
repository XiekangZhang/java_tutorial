package de.xiekang.beam.core;

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

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Task4 {
    private final static Logger LOGGER = LogManager.getLogger(Task4.class.getName());

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        public LogOutput() {
            this.prefix = "Processing Element";
        }

        @ProcessElement
        public void processElement(@Element T element) {
            LOGGER.info("{}: {}", prefix, element);
        }
    }

    static PCollection<KV<String, Iterable<String>>> applyTransform(PCollection<String> input) {
        return input
                .apply(MapElements.into(kvs(strings(), strings())).via(word -> KV.of(word.substring(0, 1), word)))
                .apply(GroupByKey.create());
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> input = pipeline.apply("Create Input",
                Create.of("apple", "ball", "car", "bear", "cheetah", "ant"));
        PCollection<KV<String, Iterable<String>>> output = applyTransform(input);
        output.apply("Log Output", ParDo.of(new LogOutput<>()));

        pipeline.run().waitUntilFinish();
    }
}

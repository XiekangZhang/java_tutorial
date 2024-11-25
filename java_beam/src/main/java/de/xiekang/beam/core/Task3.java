package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Arrays;

public class Task3 {
    final private static Logger LOGGER = LogManager.getLogger(Task3.class.getName());

    static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(FlatMapElements.into(TypeDescriptors.strings()).
                via(sentence -> Arrays.asList(sentence.split(" "))));
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        public LogOutput() {
            this.prefix = "Processing Element";
        }

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(@Element T element) {
            LOGGER.info("{}: {}", prefix, element);
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> input = pipeline.apply("create string",
                Create.of("Apache Beam", "Unified Batch and Streaming"));
        PCollection<String> output = applyTransform(input);
        output.apply("Log out", ParDo.of(new LogOutput<>()));

        pipeline.run().waitUntilFinish();
    }
}

package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class Task12 {
    private static final Logger LOGGER = LogManager.getLogger(Task12.class.getName());

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
            LOGGER.info("{}: {}", this.prefix, element);
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> wordsStartingWithA = pipeline.apply("words starting with A",
                Create.of("apple", "ant", "arrow"));
        PCollection<String> wordsStartingWithB = pipeline.apply("words starting with B",
                Create.of("ball", "book", "bow"));

        PCollection<String> output = PCollectionList.of(wordsStartingWithA).and(wordsStartingWithB)
                .apply(Flatten.pCollections());
        output.apply("Log out", ParDo.of(new LogOutput<>()));

        pipeline.run().waitUntilFinish();
    }
}

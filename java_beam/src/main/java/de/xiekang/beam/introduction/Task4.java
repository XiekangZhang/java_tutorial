package de.xiekang.beam.introduction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Arrays;

public class Task4 {
    private static final Logger LOGGER = LogManager.getLogger(Task4.class.getName());

    public static class LogStrings extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info("Processing element: {}", c.element());
            c.output(c.element());
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // Read text file content line by line, which is not empty.
        PCollection<String> input = pipeline
                .apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
                .apply(Filter.by(line -> !line.isEmpty()));

        // Output first 10 elements to the console
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);

        PCollection<String> sampleLines = input.apply(sample)
                .apply(Flatten.iterables())
                .apply("Log lines", ParDo.of(new LogStrings()));

        // Read text file and split into words
        PCollection<String> words = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via(line -> Arrays.asList(line.split("[^\\\\p{L}]+"))))
                .apply(Filter.by(word -> !word.isEmpty()));

        PCollection<String> sampleWords = words.apply(sample).apply(Flatten.iterables())
                .apply("Log words", ParDo.of(new LogStrings()));

        // Write PCollection to text file
        sampleWords.apply(TextIO.write().to("sample-words"));
        pipeline.run().waitUntilFinish();
    }
}

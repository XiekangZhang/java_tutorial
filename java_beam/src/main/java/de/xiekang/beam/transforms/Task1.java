package de.xiekang.beam.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Arrays;

public class Task1 {
    private static final Logger LOGGER = LogManager.getLogger(Task1.class.getName());

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        String str = "To be, or not to be: that is the question:Whether 'tis nobler in the mind to suffer " +
                "The slings and arrows of outrageous fortune,Or to take arms against a sea of troubles," +
                "And by opposing end them. To die: to sleep";

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> read = pipeline.apply("Read", Create.of(Arrays.asList(str.split(" "))));
        PCollection<String> filterStartingWithA = read.apply("Starting with a", Filter.by(s -> s.startsWith("a")));
        PCollection<String> filterWordsLength = filterStartingWithA.apply("Words length", Filter.by(s -> s.length() > 3));

        filterWordsLength.apply("Log output", ParDo.of(new LogOutput()));
        pipeline.run().waitUntilFinish();
    }

    private static class LogOutput extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            LOGGER.info("Element: {}", context.element());
            context.output(context.element());
        }
    }
}

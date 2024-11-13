package de.xiekang.beam.introduction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;


public class Task3 {
    private static final Logger log = LogManager.getLogger(Task3.class.getName());

    public static class LogStrings extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            log.info("Processing word: {}", c.element());
            c.output(c.element());
        }
    }

    public static class LogIntegers extends DoFn<Integer, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            log.info("Processing number: {}", c.element());
            c.output(c.element());
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> words = pipeline.apply(
                Create.of("To", "be", "or", "not", "to", "be", "that", "is", "the", "question")
        );
        PCollection<Integer> numbers = pipeline.apply(
                Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        );

        words.apply("Log words", ParDo.of(new LogStrings()));
        numbers.apply("Log numbers", ParDo.of(new LogIntegers()));

        pipeline.run().waitUntilFinish();
    }
}

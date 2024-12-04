package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Kinglear
 * - leave the words beginning with "i"
 * - additional output --> separate uppercase and lowercase
 * - side-input on lowercase --> check matching words in both PCollections
 */

public class Challenge3 {
    private static final Logger LOGGER = LogManager.getLogger(Challenge3.class.getName());

    static class Logoutput<T> extends DoFn<T, T> {
        private String prefix;

        public Logoutput(String prefix) {
            this.prefix = prefix;
        }

        public Logoutput() {
            this.prefix = "Processing Element";
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOGGER.info("{}: {}", this.prefix, c.element());
        }
    }

    // Step 1: Read input from file
    static PCollection<String> readInputFromFile(Pipeline pipeline, String filePath) {
        return pipeline.apply("Read Input", TextIO.read().from(filePath));
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> input = readInputFromFile(pipeline, Challenge3.class.getResource("/kinglear.txt").getPath());
        input.apply("Log out", ParDo.of(new Logoutput<>()));

        pipeline.run().waitUntilFinish();
    }
}

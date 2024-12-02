package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Challenge: "kinglear" will be divided into words and filtered.
 * - Divide words into 3 portions. --> contains capital letters, begin with a capital letter, all lowercase letters
 * - Count each element
 * - Translate the 1st, 2nd elements of the array to lowercase
 * - combine the resulting collections and group them by key.
 */

public class Challenge1 {
    private static final Logger LOGGER = LogManager.getLogger(Challenge1.class.getName());

    private static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        public LogOutput() {
            this.prefix = "Processing Element";
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            LOGGER.info("{}: {}", this.prefix, context.element());
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        PCollection<String> input = readInputs(pipeline, Challenge1.class.getResource("/kinglear.txt").getPath());
        input.apply("Log out", ParDo.of(new LogOutput<>("Get Inputs")));

        pipeline.run().waitUntilFinish();
    }

    // Step 1 read input
    static PCollection<String> readInputs(Pipeline pipeline, String filePath) {
        return pipeline.apply("Read Files", TextIO.read().from(filePath));
    }

    // Step 2 divide into portions
    static PCollectionTuple divideInputs(PCollection<String> input, TupleTag<String>... tags) {
        if (tags.length > 0) {
            LOGGER.info("The inputs will be then divided into {} parts", tags.length);
        } else {
            throw new RuntimeException("no TupleTag could be found");
        }
        return input.apply(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String element, MultiOutputReceiver outputs) {
                if (element.startsWith())
            }
        });
    }


}

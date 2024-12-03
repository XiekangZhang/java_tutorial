package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Arrays;
import java.util.List;

/**
 * Sum up by username using Combine, each player's point must be combined
 */

public class Challenge2 {
    private static final Logger LOGGER = LogManager.getLogger(Challenge2.class.getName());
    private static final String REGEX_FOR_CSV = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";


    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        public LogOutput() {
            this.prefix = "Processing Element";
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOGGER.info(prefix + ": " + c.element());
        }
    }

    // Step 1 read from file
    static PCollection<String> readFromFile(Pipeline pipeline, String filePath) {
        return pipeline.apply("read from files", TextIO.read().from(filePath));
    }

    // Step 2 parse input to get username and score
    static PCollection<KV<String, Long>> parseUsernameAndScore(PCollection<String> input) {
        return input.apply("Parse username and score", ParDo.of(new DoFn<String, KV<String, Long>>() {
            @ProcessElement
            public void processElement(@Element String element, OutputReceiver<KV<String, Long>> out) {
                List<String> parts = Arrays.asList(element.split(REGEX_FOR_CSV));
                try {
                    String username = parts.get(1);
                    Long score = Long.parseLong(parts.get(2));
                    out.output(KV.of(username, score));
                } catch (Exception e) {
                    LOGGER.error("Failed to parse score: {}", e.getMessage());
                }
            }
        }));
    }

    // Step 3 combine by username and sum up the score
    static class SumScores extends Combine.BinaryCombineFn<Long> {
        @Override
        public Long apply(Long a, Long b) {
            return a + b;
        }
    }

    static PCollection<KV<String, Long>> combineByName(PCollection<KV<String, Long>> input) {
        return input.apply(Combine.perKey(new SumScores()));
    }


    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // Read file
        PCollection<String> input = readFromFile(pipeline,
                Challenge2.class.getResource("/gaming_data.csv").getPath());

        //PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        //PCollection<String> sampleOutput = input.apply(sample).apply(Flatten.iterables());

        PCollection<KV<String, Long>> parsed = combineByName(parseUsernameAndScore(input));
        parsed.apply(ParDo.of(new LogOutput<>("Parsed & Combined")));
        pipeline.run().waitUntilFinish();
    }
}

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

import java.io.Serializable;
import java.util.*;

/**
 * Challenge 4:
 * - kinglear.txt --> group the words so that the key is the first letter and the value is an array of words
 */

public class Challenge4 {
    private static final Logger LOGGER = LogManager.getLogger(Challenge3.class.getName());
    private static final String ONLYWORDS = "[^a-zA-Z]+";

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
        return pipeline.apply("Read Input", TextIO.read().from(filePath))
                .apply("get words", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String line, OutputReceiver<String> out) {
                        for (String word : line.split(ONLYWORDS)) {
                            if (!word.isEmpty()) {
                                out.output(word);
                            }
                        }
                    }
                }));
    }

    // Step 2: Group the words by the first letter
    static PCollection<KV<String, String>> groupWordByFirstLetter(PCollection<String> input) {
        return input.apply("group by first letter", WithKeys.of(new SerializableFunction<String, String>() {
            @Override
            public String apply(String input) {
                return input.substring(0, 1);
            }
        }));
    }

    // Step 3: merge the words with the same first letter
    static class MergeWordsCombineFn extends Combine.CombineFn<String, MergeWordsCombineFn.Accumulate,
            Map<String, List<String>>> {
        static class Accumulate implements Serializable {
            Map<String, List<String>> result = new HashMap<>();
        }

        @Override
        public Accumulate createAccumulator() {
            return new Accumulate();
        }

        @Override
        public Accumulate addInput(Accumulate mutableAccumulator, String input) {
            mutableAccumulator.result.computeIfAbsent(input.substring(0, 1), k -> new ArrayList<>()).add(input);
            return mutableAccumulator;
        }

        @Override
        public Accumulate mergeAccumulators(Iterable<Accumulate> accumulators) {
            Accumulate merged = createAccumulator();
            for (Accumulate acc: accumulators) {
                merged.result.putAll(acc.result);
            }
            return merged;
        }

        @Override
        public Map<String, List<String>> extractOutput(Accumulate accumulator) {
            return accumulator.result;
        }
    }

    public static void main(String[] args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> words = readInputFromFile(pipeline, Challenge4.class.getResource("/kinglear.txt").getPath());
        PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        PCollection<String> sampleWords = words.apply(sample).apply(Flatten.iterables());
        PCollection<KV<String, String>> groupedWords = groupWordByFirstLetter(sampleWords);

        // Option 1
        groupedWords.apply("group by key", GroupByKey.create()).apply("Log out", ParDo.of(new Logoutput<>("Grouped Words - Easy way")));

        // Option 2
        PCollection<Map<String, List<String>>> mergedWords = sampleWords.apply("Merge Words", Combine.globally(new MergeWordsCombineFn()));
        mergedWords.apply("Log out", ParDo.of(new Logoutput<>("Grouped words - CombineFn way")));
        pipeline.run().waitUntilFinish();
    }
}

package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
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
        TupleTag<String> allCaps = new TupleTag<>() {
        };
        TupleTag<String> allLower = new TupleTag<>() {
        };
        TupleTag<String> camelCase = new TupleTag<>() {
        };

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        PCollection<String> input = readInputs(pipeline, Challenge1.class.getResource("/kinglear.txt").getPath());
        //input.apply("Log out", ParDo.of(new LogOutput<>("Get Inputs")));

        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        PCollection<String> sampleOutput = input.apply(sample).apply(Flatten.iterables());

        PCollectionTuple outputs = divideInputs(sampleOutput, allCaps, allLower, camelCase);

        PCollection<String> allCapsToLower = lowerCase(outputs.get(allCaps));
        PCollection<String> allSmall = outputs.get(allLower);
        PCollection<String> camelCaseToLower = lowerCase(outputs.get(camelCase));

        //allCapsToLower.apply("Log all caps", ParDo.of(new LogOutput<>("All Caps to lower")));
        //allSmall.apply("Log all lower", ParDo.of(new LogOutput<>("All Lower")));
        //camelCaseToLower.apply("Log camelCase", ParDo.of(new LogOutput<>("camelCase to lower")));

        PCollection<KV<String, String>> combined = combineWithKey(allCapsToLower, allSmall, camelCaseToLower);
        //combined.apply("Log combined", ParDo.of(new LogOutput<>("PCollection")));

        // Step 5 Count each element
        PCollection<KV<String, Long>> counted = combined.apply("Count each element", Count.perKey());
        counted.apply("Log counted", ParDo.of(new LogOutput<>("Counted")));

        pipeline.run().waitUntilFinish();
    }

    // Step 1 read input
    static PCollection<String> readInputs(Pipeline pipeline, String filePath) {
        return pipeline.apply("Read Files", TextIO.read().from(filePath))
                .apply("Split into words", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String element, OutputReceiver<String> out) {
                        for (String word : element.split("[^a-zA-Z]+")) {
                            if (!word.isEmpty()) {
                                out.output(word);
                            }
                        }
                    }
                }));
    }

    // Step 2 divide into portions
    static PCollectionTuple divideInputs(PCollection<String> input, TupleTag<String>... tags) {
        if (tags.length > 0) {
            LOGGER.info("The inputs will be then divided into {} parts", tags.length);
        } else {
            throw new RuntimeException("no TupleTag could be found");
        }
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String element, MultiOutputReceiver outputs) {
                if (element.chars().allMatch(Character::isUpperCase)) {
                    outputs.get(tags[0]).output(element);
                } else if (element.chars().allMatch(Character::isLowerCase)) {
                    outputs.get(tags[1]).output(element);
                } else {
                    outputs.get(tags[2]).output(element);
                }
            }
        }).withOutputTags(tags[0], TupleTagList.of(tags[1]).and(tags[2])));
    }

    // Step 3 lower case
    static PCollection<String> lowerCase(PCollection<String> input) {
        return input.apply("lower case", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String element, OutputReceiver<String> output) {
                output.output(element.toLowerCase());
            }
        }));
    }

    // Step 4 Combine with key
    static PCollection<KV<String, String>> combineWithKey(PCollection<String>... inputs) {
        return PCollectionList.of(inputs[0]).and(inputs[1]).and(inputs[2])
                .apply("Combine with key", Flatten.pCollections())
                .apply("Group by key", WithKeys.of((String word) -> word).withKeyType(TypeDescriptors.strings()));
    }
}

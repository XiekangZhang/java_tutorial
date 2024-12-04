package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Kinglear
 * - leave the words beginning with "i"
 * - additional output --> separate uppercase and lowercase
 * - side-input on lowercase --> check matching words in both PCollections
 */

public class Challenge3 {
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

    // Step 2: leave the words beginning with "i"
    static PCollection<String> filterOutWordsBeginningWithI(PCollection<String> input) {
        return input.apply("Filter Words Beginning with 'i'", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String element, OutputReceiver<String> out) {
                if (!element.toLowerCase().startsWith("i")) out.output(element);
            }
        }));
    }

    // Step 3: separate uppercase and lowercase
    static PCollectionTuple separateUppercaseAndLowercase(PCollection<String> input,
                                                          TupleTag<String> uppercaseTag,
                                                          TupleTag<String> lowercaseTag) {
        List<String> upperCaseWords = new ArrayList<>();
        List<String> lowerCaseWords = new ArrayList<>();
        return input.apply("Separate Uppercase and Lowercase", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String element, MultiOutputReceiver multiOutput) {
                if (element.equals(element.toUpperCase())) {
                    multiOutput.get(uppercaseTag).output(element);
                } else {
                    multiOutput.get(lowercaseTag).output(element);
                }
            }
        }).withOutputTags(uppercaseTag, TupleTagList.of(lowercaseTag)));
    }

    // Step 4: side input on lowercase --> check matching words in both PCollections
    static PCollection<KV<String, Boolean>> checkMatchingWords(PCollection<String> upperCase,
                                                               PCollectionView<List<String>> lowercaseView) {
        return upperCase.apply("Check Matching Words", ParDo.of(new DoFn<String, KV<String, Boolean>>() {
            @ProcessElement
            public void processElement(@Element String element, OutputReceiver<KV<String, Boolean>> kvOutputReceiver,
                                       ProcessContext processContext) {
                List<String> lowercase = processContext.sideInput(lowercaseView).stream().distinct().collect(Collectors.toList());
                kvOutputReceiver.output(KV.of(element, lowercase.contains(element.toLowerCase())));
            }
        }).withSideInputs(lowercaseView));
    }


    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        TupleTag<String> uppercaseTag = new TupleTag<>() {
        };
        TupleTag<String> lowercaseTag = new TupleTag<>() {
        };

        PCollection<String> input = readInputFromFile(pipeline, Challenge3.class.getResource("/kinglear.txt").getPath());

        //PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        //PCollection<String> output = input.apply(sample).apply(Flatten.iterables());

        PCollection<String> filteredOutput = filterOutWordsBeginningWithI(input);
        PCollectionTuple separatedOutput = separateUppercaseAndLowercase(filteredOutput, uppercaseTag, lowercaseTag);

        PCollectionView<List<String>> lowercaseView = separatedOutput.get(lowercaseTag).apply(View.asList());
        PCollection<KV<String, Boolean>> matchingWords = checkMatchingWords(separatedOutput.get(uppercaseTag)
                        .apply(Deduplicate.<String>values()), lowercaseView);

        matchingWords.apply("Log Matching Words", ParDo.of(new Logoutput<>("Matching Words")));

        pipeline.run().waitUntilFinish();
    }
}

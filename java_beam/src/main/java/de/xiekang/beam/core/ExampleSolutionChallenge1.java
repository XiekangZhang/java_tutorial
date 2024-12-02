package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Arrays;

public class ExampleSolutionChallenge1 {
    private static final Logger LOGGER = LogManager.getLogger(ExampleSolutionChallenge1.class.getName());

    static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info(prefix + ": {}", c.element());
        }
    }

    static PCollection<KV<String, Long>> mergePCollections(PCollection<KV<String, Long>> input1,
                                                           PCollection<KV<String, Long>> input2,
                                                           PCollection<KV<String, Long>> input3) {
        return PCollectionList.of(input1).and(input2).and(input3).apply(Flatten.pCollections());
    }

    static PCollection<KV<String, Long>> countPerElement(PCollection<String> input) {
        return input.apply(Count.perElement());
    }

    static PCollection<KV<String, Long>> convertPCollectionToLowerCase(PCollection<KV<String, Long>> input) {
        return input.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                .via(word -> KV.of(word.getKey().toLowerCase(), word.getValue())));
    }

    static PCollectionList<String> partitionPCollectionByCase(PCollection<String> input) {
        return input.apply(Partition.of(3, (word, numPartitions) -> {
            if (word.equals(word.toUpperCase())) {
                return 0;
            } else if (Character.isUpperCase(word.charAt(0))) {
                return 1;
            } else {
                return 2;
            }
        }));
    }


    static PCollection<KV<String, Iterable<Long>>> groupByKey(PCollection<KV<String, Long>> input) {
        return input.apply(GroupByKey.create());
    }

    public static void main(String[] args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply(TextIO.read().from(ExampleSolutionChallenge1.class.getResource("/kinglear.txt").getPath()))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()));

        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);

        PCollection<String> limitedPCollection = input.apply(sample).apply(Flatten.iterables());

        PCollectionList<String> pCollectionList = partitionPCollectionByCase(limitedPCollection);

        PCollection<KV<String, Long>> allLetterUpperCase = countPerElement(pCollectionList.get(0));
        PCollection<KV<String, Long>> firstLetterUpperCase = countPerElement(pCollectionList.get(1));
        PCollection<KV<String, Long>> allLetterLowerCase = countPerElement(pCollectionList.get(2));

        PCollection<KV<String, Long>> newFirstPartPCollection = convertPCollectionToLowerCase(allLetterUpperCase);
        PCollection<KV<String, Long>> newSecondPartPCollection = convertPCollectionToLowerCase(firstLetterUpperCase);

        PCollection<KV<String, Long>> flattenPCollection = mergePCollections(newFirstPartPCollection, newSecondPartPCollection, allLetterLowerCase);

        PCollection<KV<String, Iterable<Long>>> groupPCollection = groupByKey(flattenPCollection);

        groupPCollection
                .apply("Log words", ParDo.of(new LogOutput<>()));


        pipeline.run();
    }
}

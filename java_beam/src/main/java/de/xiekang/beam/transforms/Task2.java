package de.xiekang.beam.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class Task2 {
    private static final Logger LOGGER = LogManager.getLogger(Task2.class.getName());

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> numberInput = pipeline.apply("Create Numbers",
                Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        PCollection<KV<String, Integer>> numberInputWithKeys = pipeline.apply("Create Key Value Pair",
                Create.of(KV.of("1", 1), KV.of("2", 2), KV.of("3", 3), KV.of("4", 4), KV.of("5", 5), KV.of("5", 5)));

        // Count
        PCollection<Long> allCounts = numberInput.apply("All Count", Count.globally());
        allCounts.apply("Log All Count", ParDo.of(new LogOutput()));

        // Count per key
        PCollection<KV<String, Long>> countPerKey = numberInputWithKeys.apply("Count Per Key", Count.perKey());
        countPerKey.apply("Log Count Per Key", ParDo.of(new LogOutput()));

        // Count per Element
        PCollection<KV<KV<String, Integer>, Long>> countPerElement = numberInputWithKeys.apply("Count Per Element", Count.perElement());
        countPerElement.apply("Log Count Per Element", ParDo.of(new LogOutput()));

        pipeline.run().waitUntilFinish();
    }

    private static class LogOutput extends DoFn<Object, String> {
        @ProcessElement
        public void processElement(ProcessContext processContext) {
            LOGGER.info("Processing element: " + processContext.element());
            processContext.output("Number: " + processContext.element());
        }
    }
}

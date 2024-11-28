package de.xiekang.beam.core;

// Task: create own sum int function use at least two ways

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Task8 {
    private final static Logger LOGGER = LogManager.getLogger(Task8.class.getName());

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<Integer> input = pipeline.apply("create input",
                Create.of(10, 20, 30, 40, 50));

        // option 1
        input.apply("sum1", Sum.integersGlobally()).apply("log out", ParDo.of(new LogOutput<>("easy way")));

        // option 2
        input.apply("sum2", Combine.globally(new SumIntFn())).apply("log out", ParDo.of(new LogOutput<>("combine way")));

        pipeline.run().waitUntilFinish();
    }

    static class SumIntFn implements SerializableFunction<Iterable<Integer>, Integer> {
        @Override
        public Integer apply(Iterable<Integer> input) {
            return StreamSupport.stream(input.spliterator(), false).collect(Collectors.toList()).stream().mapToInt(e -> e).sum();
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {
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
}

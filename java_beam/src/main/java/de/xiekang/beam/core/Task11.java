package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.integers;

public class Task11 {
    private static final Logger LOGGER = LogManager.getLogger(Task11.class.getName());

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        public LogOutput() {
            this.prefix = "Processing Element";
        }

        @ProcessElement
        public void processElement(@Element T element) {
            LOGGER.info("{}: {}", this.prefix, element);
        }
    }

    static class ExtractAndMultiplyNumbers extends PTransform<PCollection<String>, PCollection<Integer>> {
        @Override
        public PCollection<Integer> expand(PCollection<String> input) {
            return input.apply(
                    "Extract value", ParDo.of(new DoFn<String, Integer>() {
                        @ProcessElement
                        public void processElement(@Element String input, OutputReceiver<Integer> output) {
                            Arrays.stream(input.split(",")).forEach(
                                    value -> output.output(Integer.valueOf(value))
                            );
                        }
                    })
            ).apply("Multiply 10", MapElements.into(integers()).via(e -> e * 10));
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> input = pipeline.apply("create input", Create.of("1,2,3,4,5", "6,7,8,9,10"));
        input.apply("Extract and Multiply", new ExtractAndMultiplyNumbers()).apply(ParDo.of(new LogOutput<>()));

        pipeline.run().waitUntilFinish();
    }
}

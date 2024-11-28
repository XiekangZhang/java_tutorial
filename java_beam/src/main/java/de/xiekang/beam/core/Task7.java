package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;

public class Task7 {
    private static final Logger LOGGER = LogManager.getLogger(Task7.class.getName());

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
            LOGGER.info("{}: {}", this.prefix, c.element());
        }
    }

    static class Multiply<T> extends DoFn<Integer, Integer> {
        private int multi;

        public Multiply(int multi) {
            this.multi = multi;
        }

        @ProcessElement
        public void processElement(@Element Integer input, OutputReceiver<Integer> out) {
            out.output(input * this.multi);
        }
    }

    static PCollection<Integer> multiplyBy15(PCollection<Integer> input) {
        return input.apply("multiple by 15", MapElements.into(integers()).via(num -> num * 15));
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<Integer> input = pipeline.apply("create input", Create.of(1, 2, 3, 4, 5));

        input.apply("multiply by 5", ParDo.of(new Multiply<>(5)))
                .apply("multiply by 5", ParDo.of(new LogOutput<>(" * 5 = ")));

        input.apply("multiply by 10", ParDo.of(new Multiply<>(10)))
                .apply("multiply by 10", ParDo.of(new LogOutput<>(" * 10 = ")));

        PCollection<Integer> multiplyBy15 = multiplyBy15(input);
        multiplyBy15.apply("multiply by 15", ParDo.of(new LogOutput<>(" * 15 = ")));

        pipeline.run().waitUntilFinish();
    }
}

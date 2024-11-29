package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.math.BigInteger;

public class Task10 {
    private static final Logger LOGGER = LogManager.getLogger(Task10.class.getName());

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

    //  as binary operations
    static class SumInBinaryCombineFn extends Combine.BinaryCombineFn<Integer> {
        @Override
        public Integer apply(Integer left, Integer right) {
            return left + right;
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<KV<String, Integer>> input = pipeline.apply(Create.of(
                KV.of("Player 1", 15),
                KV.of("Player 2", 10),
                KV.of("Player 1", 100),
                KV.of("Player 3", 25),
                KV.of("Player 2", 75)
        ));
        input.apply(Combine.perKey(new SumInBinaryCombineFn())).apply("Log out", ParDo.of(new LogOutput<>("Combine with key")));

        PCollection<Integer> input2 = pipeline.apply(Create.of(
                10, 20, 30, 40, 50
        ));

        input2.apply("Combine globally", Combine.globally(new SumInBinaryCombineFn())).apply("Log out",
                ParDo.of(new LogOutput<>("Combine without key")));

        pipeline.run().waitUntilFinish();
    }
}

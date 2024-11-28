package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.Serializable;
import java.util.Objects;

public class Task9 {
    private static final Logger LOGGER = LogManager.getLogger(Task9.class.getName());

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
            LOGGER.info("{}: {}", prefix, c.element());
        }
    }

    static class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
        class Accum implements Serializable {
            int sum = 0;
            int count = 0;

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                Accum accum = (Accum) obj;
                return sum == accum.sum && count == accum.count;
            }

            @Override
            public int hashCode() {
                return Objects.hash(sum, count);
            }
        }

        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(Accum mutableAccumulator, Integer input) {
            mutableAccumulator.sum += input;
            mutableAccumulator.count++;
            return mutableAccumulator;
        }

        @Override
        public Accum mergeAccumulators(@UnknownKeyFor @NonNull @Initialized Iterable<Accum> accumulators) {
            Accum merged = createAccumulator();
            for (Accum accum : accumulators) {
                merged.sum += accum.sum;
                merged.count += accum.count;
            }
            return merged;
        }

        @Override
        public Double extractOutput(Accum accumulator) {
            return accumulator.sum / (double) accumulator.count;
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> input = pipeline.apply("crate input", Create.of(10, 20, 50, 70, 90));
        PCollection<Double> average = input.apply("calculate average", Combine.globally(new AverageFn()));
        average.apply("log output", ParDo.of(new LogOutput<>("average with combine")));

        PCollection<Double> averageEasy = input.apply("calculate average easy", Mean.globally());
        averageEasy.apply("log output easy", ParDo.of(new LogOutput<>("average with mean")));

        pipeline.run().waitUntilFinish();
    }
}

package de.xiekang.beam.core;

// Two way to find whether the number is bigger than 100 or not

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;

public class Task13 {
    private static final Logger LOGGER = LogManager.getLogger(Task13.class.getName());

    static class Logout<T> extends DoFn<T, T> {
        private String prefix;

        public Logout(String prefix) {
            this.prefix = prefix;
        }

        public Logout() {
            this.prefix = "Processing Element";
        }

        @ProcessElement
        public void processElement(@Element T element) {
            LOGGER.info("{}: {}", this.prefix, element);
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        TupleTag<Integer> numSmaller = new TupleTag<>() {
        };
        TupleTag<Integer> numBigger = new TupleTag<>() {
        };

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        PCollection<Integer> input = pipeline.apply("create input", Create.of(
                1, 4, 100, 103, 4352, 123, 5, 90, 3942, 565
        ));

        PCollectionTuple output = input.apply("MultiOutputReceiver", ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer input, MultiOutputReceiver outputReceiver) {
                if (input > 100) {
                    outputReceiver.get(numBigger).output(input);
                }
                if (input <= 100) {
                    outputReceiver.get(numSmaller).output(input);
                }
            }
        }).withOutputTags(numBigger, TupleTagList.of(numSmaller)));

        output.get(numBigger).apply(" > 100", ParDo.of(new Logout<>("old way > 100")));
        output.get(numSmaller).apply(" <= 100", ParDo.of(new Logout<>("old way <= 100")));

        PCollectionList<Integer> partition = applyTransform(input);
        partition.get(0).apply(" > 100", ParDo.of(new Logout<>("partition > 100")));
        partition.get(1).apply(" <= 100", ParDo.of(new Logout<>("partition <= 100")));

        pipeline.run().waitUntilFinish();
    }

    static PCollectionList<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(Partition.of(
                2,
                (PartitionFn<Integer>) (number, numPartitions) -> {
                    if (number > 100) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
        ));
    }
}

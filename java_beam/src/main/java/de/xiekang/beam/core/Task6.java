package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;


public class Task6 {
    private static final Logger LOGGER = LogManager.getLogger(Task6.class.getName());

    static PCollectionTuple applyTransform(PCollection<Integer> input,
                                           TupleTag<Integer> numBelow100Tag,
                                           TupleTag<Integer> numAbove100Tag) {
        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer element, MultiOutputReceiver out) {
                if (element <= 100) {
                    out.get(numBelow100Tag).output(element);
                } else {
                    out.get(numAbove100Tag).output(element);
                }
            }
        }).withOutputTags(numBelow100Tag, TupleTagList.of(numAbove100Tag)));
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> input = pipeline.apply("create list of number",
                Create.of(10, 50, 120, 20, 200, 0));

        TupleTag<Integer> numBelow100 = new TupleTag<Integer>() {
        };
        TupleTag<Integer> numAbove100 = new TupleTag<Integer>() {
        };

        PCollectionTuple output = applyTransform(input, numBelow100, numAbove100);

        output.get(numBelow100).apply("Log numBelow100", ParDo.of(new LogOutput<>("num <= 100")));
        output.get(numAbove100).apply("Log numAbove100", ParDo.of(new LogOutput<>("num > 100")));

        pipeline.run().waitUntilFinish();
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
        public void processElement(ProcessContext c) {
            LOGGER.info("{}: {}", prefix, c.element());
        }
    }
}



package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class Task1 {
    private final static Logger LOGGER = LogManager.getLogger(Task1.class.getName());

    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer number, OutputReceiver<Integer> out) {
                out.output(number * 10);
            }
        }));
    }

    static PCollection<String> splitSentences(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String input, OutputReceiver<String> out) {
                String[] words = input.split(" ");
                for (String word : words) {
                    out.output(word);
                }
            }
        }));
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
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info(this.prefix + ": {}", c.element());
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5));
        PCollection<Integer> output = applyTransform(input);
        output.apply("Log", ParDo.of(new LogOutput<>()));

        PCollection<String> inputString = pipeline.apply(Create.of("Hello Beam", "It is awesome"));
        PCollection<String> outputString = splitSentences(inputString);
        outputString.apply("Log", ParDo.of(new LogOutput<>("String ")));

        pipeline.run().waitUntilFinish();
    }
}

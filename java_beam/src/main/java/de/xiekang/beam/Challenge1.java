package de.xiekang.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class Challenge1 {
    private final static Logger LOGGER = LogManager.getLogger(Challenge1.class.getName());

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        String filePath = Challenge1.class.getResource("/sample1000.csv").getPath();

        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<Double> readFiles = pipeline.apply("read file", TextIO.read().from(filePath))
                .apply("extract price", ParDo.of(new ExtractTaxiRideCostFn()));

        //final PTransform<PCollection<Double>, PCollection<Iterable<Double>>> sample = Sample.fixedSizeGlobally(10);
        //readFiles.apply(sample)
        //        .apply(Flatten.iterables())
        //        .apply("log output", ParDo.of(new LogOutput<>("existing columns: ")));

        PCollection<Double> rideAmountsSmallerThan15 = readFiles.apply("filter 15", Filter.lessThan(15.0));
        //rideAmountsSmallerThan15.apply("log output", ParDo.of(new LogOutput<>("amounts smaller than 15: ")));
        rideAmountsSmallerThan15.apply("# of orders smaller than 15", Count.globally())
                .apply("log output", ParDo.of(new LogOutput<>("# of orders smaller than 15: ")));
        rideAmountsSmallerThan15.apply("sum of total amount smaller than 15", Sum.doublesGlobally())
                .apply("key value", WithKeys.of(new SerializableFunction<Double, String>() {
                    public String apply(Double input) {
                        return "lower";
                    }
                }))
                .apply("log output", ParDo.of(new LogOutput<>("sum of total amount smaller than 15: ")));

        PCollection<Double> rideAmountsLargerThan15 = readFiles.apply("filter 15", Filter.greaterThanEq(15.0));
        //rideAmountsLargerThan15.apply("log output", ParDo.of(new LogOutput<>("amounts larger than 15: ")));
        rideAmountsLargerThan15.apply("# of orders larger than 15", Count.globally())
                .apply("log output", ParDo.of(new LogOutput<>("# of orders larger than 15: ")));
        rideAmountsLargerThan15.apply("sum of total amount larger than 15", Sum.doublesGlobally())
                .apply("key value", WithKeys.of(new SerializableFunction<Double, String>() {
                    public String apply(Double input) {
                        return "above";
                    }
                }))
                .apply("log output", ParDo.of(new LogOutput<>("sum of total amount larger than 15: ")));

        pipeline.run().waitUntilFinish();

    }

    static class ExtractTaxiRideCostFn extends DoFn<String, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");
            if (fields.length == 17) {
                try {
                    Double totalAmount = Double.parseDouble(fields[16]);
                    c.output(totalAmount);
                } catch (NumberFormatException e) {
                    LOGGER.warn("Error parsing total amount: {}", e.getMessage());
                }
            } else {
                LOGGER.warn("Invalid number of fields: {}", fields.length);
            }
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        public LogOutput() {
            this.prefix = "Processing element";
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info(this.prefix + ": {}", c.element());
        }
    }
}

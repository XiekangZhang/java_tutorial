package de.xiekang.beam.introduction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

public class Task5 {
    private static final Logger LOGGER = LogManager.getLogger(Task5.class.getName());

    private static Double tryParseTaxiRideCost(String[] inputItems) {
        try {
            return Double.parseDouble(tryParseString(inputItems, 16));
        } catch (NumberFormatException | NullPointerException e) {
            return 0.0;
        }
    }

    private static String tryParseString(String[] inputItems, int index) {
        return inputItems.length > index ? inputItems[index] : null;
    }

    static class ExtractTaxiRideCostFn extends DoFn<String, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            Double totalAmount = tryParseTaxiRideCost(items);
            c.output(totalAmount);
        }
    }

    public static class LogDouble extends DoFn<Double, Double> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LOGGER.info("Total Amount: {}", c.element());
            c.output(c.element());
        }
    }

    public static void main(String[] args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply("Input",
                TextIO.read().from("gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv"));
        PCollection<Double> rideTotalAmounts = input.apply(ParDo.of(new ExtractTaxiRideCostFn()));

        final PTransform<PCollection<Double>, PCollection<Iterable<Double>>> sample = Sample.fixedSizeGlobally(10);

        PCollection<Double> rideAmounts = rideTotalAmounts.apply(sample)
                .apply(Flatten.iterables())
                .apply("Log amounts", ParDo.of(new LogDouble()));

        pipeline.run().waitUntilFinish();
    }
}

package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Task5 {
    private final static Logger LOGGER = LogManager.getLogger(Task5.class.getName());

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
            LOGGER.info("{}: {}", prefix, element);
        }
    }

    static class WordsAlphabet {
        private String alphabet;
        private String fruit;
        private String country;

        public WordsAlphabet(String alphabet, String fruit, String country) {
            this.alphabet = alphabet;
            this.fruit = fruit;
            this.country = country;
        }

        @Override
        public String toString() {
            return "WordsAlphabet{" +
                    "alphabet='" + alphabet + '\'' +
                    ", fruit='" + fruit + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }
    }

    public static PCollection<String> applyTransform(PCollection<String> fruits, PCollection<String> countries) {
        TupleTag<String> fruitsTag = new TupleTag<>();
        TupleTag<String> countriesTag = new TupleTag<>();

        MapElements<String, KV<String, String>> mapToAlphabetKv = MapElements.into(kvs(strings(), strings()))
                .via((word -> KV.of(word.substring(0, 1), word)));

        PCollection<KV<String, String>> fruitsPColl = fruits.apply("MapFruitsToAlphabetKv", mapToAlphabetKv);
        PCollection<KV<String, String>> countriesPColl = countries.apply("MapCountriesToAlphabetKv", mapToAlphabetKv);

        return KeyedPCollectionTuple.of(fruitsTag, fruitsPColl)
                .and(countriesTag, countriesPColl)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<String> out) {
                        String alphabet = element.getKey();
                        CoGbkResult coGbkResult = element.getValue();
                        String fruit = coGbkResult.getOnly(fruitsTag);
                        String country = coGbkResult.getOnly(countriesTag);

                        out.output(new WordsAlphabet(alphabet, fruit, country).toString());
                    }
                }));
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> fruits = pipeline.apply("fruits",
                Create.of("apple", "banana", "cherry"));

        PCollection<String> countries = pipeline.apply("countries",
                Create.of("australia", "brazil", "canada"));

        PCollection<String> output = applyTransform(fruits, countries);
        output.apply("LogOutput", ParDo.of(new LogOutput<>()));
        pipeline.run().waitUntilFinish();
    }
}

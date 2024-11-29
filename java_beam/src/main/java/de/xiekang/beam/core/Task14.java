package de.xiekang.beam.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class Task14 {
    private static final Logger LOGGER = LogManager.getLogger(Task14.class.getName());

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info(prefix + ": {}", c.element());
        }
    }

    static class Person implements Serializable {
        private String name;
        private String city;
        private String country;

        public Person(String name, String city, String country) {
            this.name = name;
            this.city = city;
            this.country = country;
        }

        public Person(String name, String city) {
            this.name = name;
            this.city = city;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            Person person = (Person) obj;
            return Objects.equals(name, person.name)
                    && Objects.equals(city, person.city)
                    && Objects.equals(country, person.country);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, city, country);
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", city='" + city + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }
    }

    static PCollectionView<Map<String, String>> createView(PCollection<KV<String, String>> citiesToCountries) {
        return citiesToCountries.apply(View.asMap());
    }

    static PCollection<Person> applyTransform(
            PCollection<Person> persons, PCollectionView<Map<String, String>> citiesToCountriesView) {
        return persons.apply(ParDo.of(new DoFn<Person, Person>() {
            @ProcessElement
            public void process(@Element Person person, OutputReceiver<Person> out, ProcessContext processContext) {
                Map<String, String> citiesToCountries = processContext.sideInput(citiesToCountriesView);
                String city = person.getCity();
                String country = citiesToCountries.get(city);

                out.output(new Person(person.getName(), city, country));
            }
        }).withSideInputs(citiesToCountriesView));
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<KV<String, String>> citiesToCountries =
                pipeline.apply("Cities and Countries",
                        Create.of(
                                KV.of("Beijing", "China"),
                                KV.of("London", "United Kingdom"),
                                KV.of("San Francisco", "United States"),
                                KV.of("Singapore", "Singapore"),
                                KV.of("Sydney", "Australia")
                        ));

        PCollectionView<Map<String, String>> citiesToCountriesView =
                createView(citiesToCountries);

        PCollection<Person> persons =
                pipeline.apply("Persons",
                        Create.of(
                                new Person("Henry", "Singapore"),
                                new Person("Jane", "San Francisco"),
                                new Person("Lee", "Beijing"),
                                new Person("John", "Sydney"),
                                new Person("Alfred", "London")
                        ));

        // The applyTransform() converts [persons] and [citiesToCountriesView] to [output]
        PCollection<Person> output = applyTransform(persons, citiesToCountriesView);

        output.apply("Log", ParDo.of(new LogOutput<Person>()));

        pipeline.run();
    }
}

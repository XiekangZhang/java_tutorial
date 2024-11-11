package de.xiekang.beam.configurations;

import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger log = LoggerFactory.getLogger(Task.class.getName());

    public interface MyOptions extends PipelineOptions {
        // Default value if [--output] equal null
    }
}

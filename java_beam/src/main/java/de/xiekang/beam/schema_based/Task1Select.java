package de.xiekang.beam.schema_based;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;


public class Task1Select {
    private static final Logger LOGGER = LogManager.getLogger(Task1Select.class.getName());

    public static PCollection<User> getProgressPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(
                TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv")
        );
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserProgressFn()));
    }

    static class ExtractUserProgressFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<User> userOutputReceiver) {
            String[] items = element.split(",");
            userOutputReceiver.output(
                    new User(
                            items[0],
                            items[1],
                            new Game(
                                    items[0],
                                    items[2],
                                    items[3],
                                    items[4]
                            )
                    )
            );
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        public LogOutput() {
            this.prefix = "Processing element";
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOGGER.info("{}: {}", prefix, c.element());
        }
    }

    public static void main(String... args) {
        Configurator.setRootLevel(Level.INFO);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);

        Schema shortInfoSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("gameId")
                .build();

        Schema gameSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("score")
                .addStringField("gameId")
                .addStringField("date")
                .build();

        Schema dataSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .addRowField("game", gameSchema)
                .build();

        PCollection<User> input = getProgressPCollection(pipeline)
                .setSchema(dataSchema,
                        TypeDescriptor.of(User.class),
                        row -> {
                            User user = row;
                            Game game = user.game;
                            Row gameRow = Row.withSchema(gameSchema)
                                    .addValues(game.userId, game.score, game.gameId, game.date)
                                    .build();
                            return Row.withSchema(dataSchema)
                                    .addValues(user.userId, user.userName, gameRow)
                                    .build();
                        },
                        row -> {
                            String userId = row.getValue("userId");
                            String userName = row.getValue("userName");
                            Row game = row.getValue("game");
                            String gameId = game.getValue("gameId");
                            String score = game.getValue("score");
                            String date = game.getValue("date");
                            return new User(userId, userName, new Game(userId, score, gameId, date));
                        }
                );

        // Select [userId] and [userName]
        PCollection<Row> shortInfo = input.apply(Select.<User>fieldNames("userId", "userName")
                        .withOutputSchema(shortInfoSchema))
                .apply("User short info", ParDo.of(new LogOutput<>("Short Info")));

        // Select user [game]
        PCollection<Row> game = input.apply(Select.fieldNames("game.*"))
                .apply("User game", ParDo.of(new LogOutput<>("Game")));

        // Flattened row, select all fields
        PCollection<Row> flattened = input.apply(Select.flattenedSchema())
                .apply("User flatten row", ParDo.of(new LogOutput<>("Flattened")));

        pipeline.run().waitUntilFinish();
    }

    // Java POJO
    @DefaultSchema(JavaFieldSchema.class)
    public static class Game {
        public String userId;
        public String score;
        public String gameId;
        public String date;

        @SchemaCreate
        public Game(String userId, String score, String gameId, String date) {
            this.userId = userId;
            this.score = score;
            this.gameId = gameId;
            this.date = date;
        }

        public String toString() {
            return "Game{" +
                    "userId='" + userId + '\'' +
                    ", score='" + score + '\'' +
                    ", gameId='" + gameId + '\'' +
                    ", date='" + date + '\'' +
                    '}';
        }
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class User {
        public String userId;
        public String userName;
        public Game game;

        @SchemaCreate
        public User(String userId, String userName, Game game) {
            this.userId = userId;
            this.userName = userName;
            this.game = game;
        }

        public String toString() {
            return "User{" +
                    "userId='" + userId + '\'' +
                    ", userName='" + userName + '\'' +
                    ", game=" + game +
                    '}';
        }
    }
}

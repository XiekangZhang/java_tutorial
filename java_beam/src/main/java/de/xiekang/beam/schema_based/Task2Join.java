package de.xiekang.beam.schema_based;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class Task2Join {
    private static final Logger LOGGER = Logger.getLogger(Task2Join.class.getName());

    public static PCollection<User> getUserPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserFn())).setCoder(UserCoder.of());
    }

    public static PCollection<Game> getGamePCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserProgressFn())).setCoder(GameCoder.of());
    }

    public static class ExtractUserFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<User> userOutputReceiver) {
            String[] parts = element.split(",");
            userOutputReceiver.output(new User(parts[0], parts[1]));

        }
    }

    public static class ExtractUserProgressFn extends DoFn<String, Game> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            String[] items = context.element().split(",");
            context.output(new Game(items[0], Integer.parseInt(items[2]), items[3], items[4]));
        }
    }

    public static class UserCoder extends Coder<User> {
        private static final UserCoder INSTANCE = new UserCoder();

        public static UserCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(User value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
            String line = value.userId + "," + value.userName;
            outStream.write(line.getBytes());
        }

        @Override
        public User decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] params = serializedDTOs.split(",");
            return new User(params[0], params[1]);
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
        }
    }

    public static class GameCoder extends Coder<Game> {
        private static final GameCoder INSTANCE = new GameCoder();

        public static GameCoder of() {
            return INSTANCE;
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {

        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void encode(Game value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
            String line = String.format("%s,%d,%s,%s", value.userId, value.score, value.gameId, value.date);
            outStream.write(line.getBytes());
        }

        @Override
        public Game decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] params = serializedDTOs.split(",");
            return new Game(params[0], Integer.parseInt(params[2]), params[3], params[4]);
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Schema definitions
        Schema userSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .build();

        Schema gameSchema = Schema.builder()
                .addStringField("userId")
                .addInt32Field("score")
                .addStringField("gameId")
                .addStringField("date")
                .build();

        PCollection<User> userInfo = getUserPCollection(pipeline)
                .setSchema(userSchema, TypeDescriptor.of(User.class),
                        user -> Row.withSchema(userSchema)
                                .addValues(user.userId, user.userName)
                                .build(),
                        input -> new User(input.getString("userId"), input.getString("userName")));

        PCollection<Game> gameInfo = getGamePCollection(pipeline)
                .setSchema(gameSchema, TypeDescriptor.of(Game.class),
                        game -> Row.withSchema(gameSchema)
                                .addValue(game.userId)
                                .addValue(game.score)
                                .addValue(game.gameId)
                                .addValue(game.date)
                                .build(),
                        input -> new Game(input.getString("userId"), input.getInt32("score"), input.getString("gameId"), input.getString("date")));

        PCollection<Row> pCollection = userInfo.apply(Join.<User, Game>innerJoin(gameInfo).using("userId"));
        pCollection.apply("User flatten row", ParDo.of(new LogOutPut<>("Flattened")));
        pipeline.run().waitUntilFinish();
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Game {
        public String userId;
        public Integer score;
        public String gameId;
        public String date;

        @SchemaCreate
        public Game(String userId, Integer score, String gameId, String date) {
            this.userId = userId;
            this.score = score;
            this.gameId = gameId;
            this.date = date;
        }

        public String toString() {
            return "Game{" +
                    "userId='" + userId + '\'' +
                    ", score=" + score +
                    ", gameId='" + gameId + '\'' +
                    ", date='" + date + '\'' +
                    '}';
        }
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class User {
        public String userId;
        public String userName;

        @SchemaCreate
        public User(String userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }

        public String toString() {
            return "User{" +
                    "userId='" + userId + '\'' +
                    ", userName='" + userName + '\'' +
                    '}';
        }
    }

    public static class LogOutPut<T> extends DoFn<T, T> {
        private final String prefix;

        public LogOutPut(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(@Element T element) {
            LOGGER.info(prefix + " " + element.toString());
        }
    }
}

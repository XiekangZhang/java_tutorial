import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        JsonSchema jsonSchema = factory.getSchema(Main.class.getResourceAsStream("/schema_vertrag.json"));

        File file = new File(Objects.requireNonNull(Main.class.getResource("/testcases/vertrag")).getFile());

        for (File f : Objects.requireNonNull(file.listFiles())) {
            JsonNode jsonNode = mapper.readTree(new FileInputStream(f));
            Set<ValidationMessage> validationResult = jsonSchema.validate(jsonNode);
            System.out.println("##########################################################################");
            if (validationResult.isEmpty()) {
                System.out.println(f.getName() + " is valid");
            } else {
                System.out.println(f.getName() + " is invalid");
                validationResult.forEach(vm -> System.out.println(vm.getMessage()));
            }
        }

    }
}

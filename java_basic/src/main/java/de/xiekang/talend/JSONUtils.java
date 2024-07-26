package de.xiekang.talend;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.util.*;

public class JSONUtils {

    public static String convertJSONToString(String JSON, String JSONPath) {
        DocumentContext documentContext = JsonPath.parse(JSON);
        LinkedHashMap<String, Object> value = documentContext.read(JSONPath);
        String newValue = "";
        try {
            newValue = new ObjectMapper().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return documentContext.set(JSONPath, newValue).jsonString();
    }

    public static String convertStringToJSON(String JSONString) {
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(JSONString);
            JSONString = jsonNode.asText();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return JSONString;
    }

    public static String convertListOfMaptoJSON(List<Map<String, Object>> JSON) {
        String result = "";
        try {
            result = new ObjectMapper().writeValueAsString(JSON);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static List<Map<String, Object>> convertStringtoListOfMap(String jsonString) {
        List<Map<String, Object>> jsonStructure = new ArrayList<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            jsonStructure = objectMapper.readValue(jsonString, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonStructure;
    }

    // JSON Filter + Sorted
    public static List<String> getAllSortedJSON(List<Map<String, Object>> JSON, String keyToSort, String filterPath, String sortPath) {
        List<String> result = new ArrayList<>();
        List<String> valuesToSort = JSONFilter(JSON, keyToSort, filterPath);
        List<String> sortedValues = new ArrayList<>(valuesToSort);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            // Sort Values
            sortedValues.sort(Comparator.reverseOrder());
            for (String s : sortedValues) {
                int index = valuesToSort.indexOf(s);
                result.add(objectMapper.writeValueAsString(JsonPath.read(JSON, String.format(sortPath, index))));
                valuesToSort.set(index, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static List<String> getAllSortedJSON(List<Map<String, Object>> JSON, String keyToSort, String filterPath) {
        return getAllSortedJSON(JSON, keyToSort, filterPath, "$[%s]");
    }

    public static List<String> getAllSortedJSON(List<Map<String, Object>> JSON, String keyToSort) {
        return getAllSortedJSON(JSON, keyToSort, "$[%s]['%s']", "$[%s]");
    }

    public static String getSortedJSON(List<Map<String, Object>> JSON, String keyToSort, int index) {
        return getAllSortedJSON(JSON, keyToSort).get(index);
    }

    public static String getSortedJSON(List<Map<String, Object>> JSON, String keyToSort, String filterPath, int index) {
        return getAllSortedJSON(JSON, keyToSort, filterPath).get(index);
    }

    public static String getSortedJSON(List<Map<String, Object>> JSON, String keyToSort, String filterPath,
                                       String sortPath, int index) {
        return getAllSortedJSON(JSON, keyToSort, filterPath, sortPath).get(index);
    }

    private static List<String> JSONFilter(List<Map<String, Object>> JSON, String keyToSort, String path) {
        List<String> valuesToSort = new ArrayList<>();
        for (int i = 0; i < JSON.size(); i++) {
            valuesToSort.add(JsonPath.read(JSON, String.format(path, i, keyToSort)));
        }
        return valuesToSort;
    }

    public static List<Map<String, Object>> JSONFilter(String JSONString, String filter) {
        return JsonPath.read(JSONString, filter);
    }

    public static Map<String, Object> convertToFlattenJSON(String JSONString) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonStructure = new HashMap<>();
        try {
            JsonNode jsonNode = objectMapper.readTree(JSONString);
            addContents("", jsonNode, jsonStructure);
            System.out.println(jsonStructure);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return jsonStructure;
    }

    private static void addContents(String path, JsonNode jsonNode, Map<String, Object> jsonStructure) {
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> entryIterator = objectNode.fields();
            String pathPrefix = path.isEmpty() ? "" : path + ".";
            while (entryIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = entryIterator.next();
                addContents(pathPrefix + entry.getKey(), entry.getValue(), jsonStructure);
            }
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                addContents(path + "[" + i + "]", arrayNode.get(i), jsonStructure);
            }
        } else if (jsonNode.isValueNode()) {
            ValueNode valueNode = (ValueNode) jsonNode;
            jsonStructure.put(path, valueNode.asText());
        }
    }

}

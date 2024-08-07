package de.xiekang.talend;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class JSONUtils {
    public static Logger LOGGER = LogManager.getLogger(JSONUtils.class.getName());

    /**
     * Convert sub-JSON to string marked with double quotes
     * @param JSON input JSON like string
     * @param JSONPath if only sub-JSON will be converted to string with double quotes
     * @return sub-JSON with double quotes
     */
    public static String convertJSONToString(String JSON, String JSONPath) {
        DocumentContext documentContext = JsonPath.parse(JSON);
        LinkedHashMap<String, Object> value = documentContext.read(JSONPath);
        String newValue = "";
        try {
            newValue = new ObjectMapper().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage());
        }
        return documentContext.set(JSONPath, newValue).jsonString();
    }

    /**
     * Convert JSON with double quotes back to JSON (attribute: value) alike structure
     * @param JSONString JSON with double quotes
     * @return (attribute: value) alike structure
     */
    public static String convertStringToJSON(String JSONString) {
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(JSONString);
            JSONString = jsonNode.asText();
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage());
        }
        return JSONString;
    }

    /**
     * Convert Map back to JSON (attribute: value) like structure
     * @param JSONMap Map of JSON
     * @return JSONString
     */
    public static String convertMapToJSON(Map<String, Object> JSONMap) {
        String result = "";
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            result = objectMapper.writeValueAsString(JSONMap);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return result;
    }

    /**
     * Convert JSONString to Java Map Object
     * @param JSONString a string formed for JSON
     * @return Map Object
     */
    public static Map<String, Object> convertJSONStringToMap(String JSONString) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> map = new LinkedHashMap<>();
        try {
            map = objectMapper.readValue(JSONString, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return map;
    }

    @Deprecated(since = "0.2")
    public static String convertListOfMaptoJSON(List<Map<String, Object>> JSON) {
        String result = "";
        try {
            result = new ObjectMapper().writeValueAsString(JSON);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Deprecated(since = "0.2")
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

    // todo: rethink
    public static Map<String, Object> convertToFlattenJSON(String JSONString) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonStructure = new HashMap<>();
        try {
            JsonNode jsonNode = objectMapper.readTree(JSONString);
            addContents("", jsonNode, jsonStructure);
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

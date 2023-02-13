package profile2schema;

import java.util.Map;

public record ArrowFieldDef(String name, Map<String, Object> properties, Map<String, String> metadata) {}

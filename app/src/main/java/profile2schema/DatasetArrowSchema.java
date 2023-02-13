package profile2schema;

import java.util.List;

public record DatasetArrowSchema(String name, List<ArrowFieldDef> fields) {}

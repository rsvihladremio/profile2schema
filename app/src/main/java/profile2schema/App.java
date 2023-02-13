/**
 * Copyright (c) 2023 Ryan SVIHLA
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package profile2schema;

import static java.util.Map.entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    name = "profile2schema",
    mixinStandardHelpOptions = true,
    version = "0.1.0",
    description = "outputs the a generic json schema from a dremio profile")
public class App implements Callable<Integer> {

  @Parameters(
      index = "0",
      description = "The profile.json or archive with a profile.json to use for schema analysis")
  private File file;

  @Option(
      names = {"-o", "--output-dir"},
      description =
          "output directory for the schema files. if empty output is just sent to standard out")
  private String outputDir;

  private static final ArrowSchemaReader reader = new ArrowSchemaReader();
  private static final Logger logger = Logger.getLogger(App.class.getName());

  @Override
  public Integer call() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ProfileJSON profileJSON = objectMapper.readValue(file, ProfileJSON.class);
    final Consumer<DatasetArrowSchema> output;
    if ("".equals(outputDir)) {
      output =
          (s) -> {
            try {
              System.out.println(objectMapper.writeValueAsString(s));
            } catch (JsonProcessingException e) {
              logger.log(
                  Level.SEVERE,
                  e,
                  () -> "unable to write to standard out for datasetpath: " + s.name());
            }
          };
    } else {
      output =
          (s) -> {
            final String newFile = String.join(".", s.name(), "json");
            final Path outputPath = Paths.get(outputDir, newFile);
            try (OutputStream stream = Files.newOutputStream(outputPath)) {
              objectMapper.writeValue(stream, s);
            } catch (IOException e) {
              logger.log(Level.SEVERE, e, () -> "unable to write to file: " + outputPath);
            }
          };
    }
    for (final DatasetProfile datasetProfile : profileJSON.getDatasetProfile()) {
      final Schema arrowFields = reader.getArrowSchema(datasetProfile);
      final List<ArrowFieldDef> fields = new ArrayList<>();
      for (final Field field : arrowFields.getFields()) {
        final String fieldName = field.getName();
        final Map<String, Object> props = arrowTypeToMap(field);
        final Map<String, String> metadata = field.getMetadata();
        final ArrowFieldDef arrowFieldDef = new ArrowFieldDef(fieldName, props, metadata);
        fields.add(arrowFieldDef);
      }
      final DatasetArrowSchema schema =
          new DatasetArrowSchema(datasetProfile.getDatasetPath(), fields);
      output.accept(schema);
    }
    return 0;
  }

  private Map<String, Object> arrowTypeToMap(final Field field) {
    final FieldType fieldType = field.getFieldType();
    final ArrowType arrowType = fieldType.getType();
    final ArrowTypeID arrowTypeID = arrowType.getTypeID();
    switch (arrowTypeID) {
      case Null -> {}
      case Struct -> {
        List<Map<String, Object>> children = field.getChildren().stream().map(this::arrowTypeToMap).toList();
        return Map.ofEntries(entry("class", "STRUCT"),
                entry("nestedTypes", children)
        );
      }
      case List -> {
        List<Map<String, Object>> children = field.getChildren().stream().map(this::arrowTypeToMap).toList();
        return Map.ofEntries(entry("class", "LIST"),
                entry("nestedTypes", children)
        );
      }
      case LargeList -> {
        List<Map<String, Object>> children = field.getChildren().stream().map(this::arrowTypeToMap).toList();
        return Map.ofEntries(entry("class", "LARGE_LIST"),
                entry("nestedTypes", children)
        );
      }
      case FixedSizeList -> {
        List<Map<String, Object>> children = field.getChildren().stream().map(this::arrowTypeToMap).toList();
        return Map.ofEntries(entry("class", "FIXED_SIZE_LIST"),
                entry("nestedTypes", children)
        );
      }
      case Union -> {
        final ArrowType.Union typeDef = (ArrowType.Union) arrowType;
        final UnionMode mode = typeDef.getMode();
        final int[] unionTypeIds = typeDef.getTypeIds();
        List<Map<String, Object>> children = field.getChildren().stream().map(this::arrowTypeToMap).toList();
        return Map.ofEntries(entry("class", "MAP"),
                entry("unionMode",  mode),
                entry("unionTypeIds", unionTypeIds),
                entry("nestedTypes", children)
        );
      }
      case Map -> {
        final ArrowType.Map typeDef = (ArrowType.Map) arrowType;
        boolean areKeySorted = typeDef.getKeysSorted();
        List<ArrowFieldDef> children = field.getChildren().stream().map(x->{
          for (Field childField :x.getChildren()){
            new ArrowFieldDef(childField.getName(), arrowTypeToMap(childField), x.getMetadata());
          }
        }).toList();
        return Map.ofEntries(entry("class", "MAP"),
                entry("keysSorted",  areKeySorted),
                entry("nestedTypes", children)
        );
      }
      case Int -> {
        final ArrowType.Int typeDef = (ArrowType.Int) arrowType;
        final int bitWidth = typeDef.getBitWidth();
        final boolean isSigned = typeDef.getIsSigned();
        return Map.ofEntries(
            entry("class", "INT"), entry("signed", isSigned), entry("bitWidth", bitWidth));
      }
      case FloatingPoint -> {
        final ArrowType.FloatingPoint typeDef = (ArrowType.FloatingPoint) arrowType;
        final FloatingPointPrecision precision = typeDef.getPrecision();
        final String className;
        switch (precision) {
          case HALF, SINGLE -> // we don't really support HALF so we are cheating and using float
          className = "FLOAT";
          case DOUBLE -> className = "DOUBLE";
          default -> throw new InvalidParameterException(
              String.format(
                  "the type of precision %s for field %s is not possible and we do not know how to"
                      + " handle it. Exiting",
                  precision.name(), fieldName));
        }
        return Map.ofEntries(entry("class", className));
      }
      case Utf8 -> {
        return Map.ofEntries(entry("class", "UTF8"));
      }
      case LargeUtf8 -> {
        field.get
        return Map.ofEntries(entry("class", "LARGE_UTF8"));
      }
      case Binary -> {
        return Map.ofEntries(entry("class", "BINARY"));
      }
      case LargeBinary -> {
        return Map.ofEntries(entry("class", "LARGE_BINARY"));
      }
      case FixedSizeBinary -> {
        final ArrowType.FixedSizeBinary typeDef = (ArrowType.FixedSizeBinary) arrowType;
        int byteWidth = typeDef.getByteWidth();
        return Map.ofEntries(
                entry("class", "FIXED_SIZE_BINARY"),
                entry("byteWidth", byteWidth));
      }
      case Bool -> {
        return Map.ofEntries(entry("class", "BOOL"));
      }
      case Decimal -> {
        final ArrowType.Decimal typeDef = (ArrowType.Decimal) arrowType;
        int precision = typeDef.getPrecision();
        int scale = typeDef.getScale();
        return Map.ofEntries(entry("class", "DECIMAL"), entry("precision", precision), entry("scale", scale));
      }
      case Date -> {
        final ArrowType.Date typeDef = (ArrowType.Date) arrowType;
        DateUnit unit = typeDef.getUnit();
        return Map.ofEntries(
                entry("class", "DATE"),
                entry("dateUnit", unit)
                );
      }
      case Time -> {
        final ArrowType.Time typeDef = (ArrowType.Time) arrowType;
        int bitWidth = typeDef.getBitWidth();
        TimeUnit unit = typeDef.getUnit();
        return Map.ofEntries(
                entry("class", "TIME"),
                entry("timeUnit", unit),
                entry("bitWidth", bitWidth)
        );
      }
      case Timestamp -> {
        final ArrowType.Timestamp typeDef = (ArrowType.Timestamp) arrowType;
        String timezone = typeDef.getTimezone();
        TimeUnit unit = typeDef.getUnit();
        return Map.ofEntries(
                entry("class", "TIMESTAMP"),
                entry("timeUnit", unit),
                entry("timeZone", timezone)
        );
      }
      case Interval -> {
        final ArrowType.Interval typeDef = (ArrowType.Interval) arrowType;
        IntervalUnit unit = typeDef.getUnit();
        return Map.ofEntries(
                entry("class", "INTERVAL"),
                entry("intervalUnit", unit)
        );
      }
      case Duration -> {
        final ArrowType.Duration typeDef = (ArrowType.Duration) arrowType;
        TimeUnit unit = typeDef.getUnit();
        return Map.ofEntries(
                entry("class", "DURATION"),
                entry("timeUnit", unit)
        );
      }
      case NONE -> {
        return Map.ofEntries(entry("class", "NONE"));
      }
      default -> {
        throw new RuntimeException(
            String.format(
                "impossible!!!!!!! critical error do not know how to read type %s", arrowTypeID));
      }
    }
  }

  // this example implements Callable, so parsing, error handling and handling user
  //
  // requests for usage help or version help can be done with one line of code.
  public static void main(final String... args) {
    final int exitCode = new CommandLine(new App()).execute(args);
    System.exit(exitCode);
  }
}

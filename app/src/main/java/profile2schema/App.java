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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import com.fasterxml.jackson.databind.SerializationFeature;
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
          "output dir for json schema")
  private File outputDir;

  private static final ArrowSchemaReader reader = new ArrowSchemaReader();
  private static final Logger logger = Logger.getLogger(App.class.getName());

  @Override
  public Integer call() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    ProfileJSON profileJSON = null;
    if(file.getName().endsWith(".zip")){
        try(FileInputStream fileInputStream = new FileInputStream(file)){
            try(ZipInputStream zipFile = new ZipInputStream(fileInputStream)){
                ZipEntry entry;
                while((entry = zipFile.getNextEntry())!=null){
                    if (entry.isDirectory()){
                        continue;
                    }
                    if(entry.getName().contains("profile")){
                        profileJSON = objectMapper.readValue(zipFile, ProfileJSON.class);
                        break;
                    }
                }
            }
            if (profileJSON == null){
                throw new InvalidParameterException("zip file " + file + " does not contain a profile and so cannot be parsed");
            }
        }
    } else {
          profileJSON = objectMapper.readValue(file, ProfileJSON.class);
    }
    final BiConsumer<Schema, String> output;
    if (outputDir == null) {
      output =
          (s,n ) -> {
            try {
              System.out.println(String.format("\"%s\":%s",n, objectMapper.writeValueAsString(s)));
            } catch (JsonProcessingException e) {
              logger.log(
                  Level.SEVERE,
                  e,
                  () -> "unable to write to standard out for schema");
            }
          };
    } else {
        if (!outputDir.exists()){
            boolean created = outputDir.mkdirs();
            if (!created){
                throw new IOException("unable to create directory at " + outputDir.getAbsolutePath());
            }
        }
      output =
          (s, n) -> {
              Path path = Paths.get(outputDir.toString(), String.join(".", n.replaceAll("[^a-zA-Z0-9\\.\\-]", "_"), "json"));
              try {
                  objectMapper.writeValue(path.toFile(), s);
              } catch (IOException e) {
                  logger.log(
                          Level.SEVERE,
                          e,
                          () -> "unable to write schema to file " + path);
              }
          };

    }
    for (final DatasetProfile datasetProfile : profileJSON.getDatasetProfile()) {
      if (datasetProfile.getBatchSchema() != null) {
        final Schema arrowFields = reader.getArrowSchema(datasetProfile);
        output.accept(arrowFields, datasetProfile.getDatasetPath());
      }
    }
    return 0;
  }


  // this example implements Callable, so parsing, error handling and handling user
  //
  // requests for usage help or version help can be done with one line of code.
  public static void main(final String... args) {
    final int exitCode = new CommandLine(new App()).execute(args);
    System.exit(exitCode);
  }
}

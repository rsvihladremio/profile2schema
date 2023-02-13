package profile2schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ArrowSchemaReader {
  public org.apache.arrow.vector.types.pojo.Schema getArrowSchema(final DatasetProfile dp) {
    final Base64.Decoder d = Base64.getDecoder();
    // assuming UTF-8
    final byte[] schemaBytes = dp.getBatchSchema().getBytes(StandardCharsets.UTF_8);
    // assume it is base64 and decode it
    final byte[] base64Decoded = d.decode(schemaBytes);
    // drop it in a byte buffer and get schema object
    final org.apache.arrow.flatbuf.Schema schema = org.apache.arrow.flatbuf.Schema.getRootAsSchema(ByteBuffer.wrap(base64Decoded));
    // conver to a vector schema, I am not sure why we do this, but we do it in the
    // reproduction
    // tool as it was
    final org.apache.arrow.vector.types.pojo.Schema s =
        org.apache.arrow.vector.types.pojo.Schema.convertSchema(schema);
    return s;
  }
}

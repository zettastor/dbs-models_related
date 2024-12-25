

package py.archive.segment;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import py.membership.SegmentMembership;

public class JsonMembershipSerialize extends JsonSerializer<SegmentMembership> {
  @Override
  public void serialize(SegmentMembership value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeString(value.serializeToObjectMapperContent());
  }
}

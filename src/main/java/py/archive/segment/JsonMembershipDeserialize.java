

package py.archive.segment;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import py.membership.SegmentMembership;

public class JsonMembershipDeserialize extends JsonDeserializer<SegmentMembership> {
  @Override
  public SegmentMembership deserialize(JsonParser jp, DeserializationContext ctxt)
      throws IOException {
    return SegmentMembership.deserializeFromObjectMapperContent(jp.getText());
  }
}

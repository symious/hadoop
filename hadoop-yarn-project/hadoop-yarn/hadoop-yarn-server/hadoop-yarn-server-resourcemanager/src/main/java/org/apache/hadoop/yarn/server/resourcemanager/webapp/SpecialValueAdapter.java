package org.apache.hadoop.yarn.server.resourcemanager.webapp;
import javax.xml.bind.annotation.adapters.XmlAdapter;

public class SpecialValueAdapter extends XmlAdapter<Float, Float> {
  @Override
  public Float unmarshal(Float value) throws Exception {
    return value;
  }

  @Override
  public Float marshal(Float value) throws Exception {
    if (value == null || value.isNaN() || value.isInfinite()) {
      return 0.0f;
    } else {
      return value;
    }
  }
}

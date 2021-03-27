package me.lyh.parquet.tensorflow;

import org.apache.parquet.Preconditions;
import org.tensorflow.proto.example.Example;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ExampleScanner {
  private long total = 0L;
  private final String name;
  private final Set<String> fields = new LinkedHashSet<>();
  private final Map<String, Long> nonZeroCounts = new HashMap<>();
  private final Map<String, Schema.Type> types = new HashMap<>();
  private final Map<String, Integer> maxCounts = new HashMap<>();

  public ExampleScanner(String name) {
    this.name = name;
  }

  public ExampleScanner scan(Example example) {
    total++;

    example.getFeatures().getFeatureMap().forEach((name, feature) -> {
      fields.add(name);

      Schema.Type newType = null;
      int count = -1;
      switch (feature.getKindCase()) {
      case BYTES_LIST:
        newType = Schema.Type.BYTES;
        count = feature.getBytesList().getValueCount();
        break;
      case FLOAT_LIST:
        newType = Schema.Type.FLOAT;
        count = feature.getFloatList().getValueCount();
        break;
      case INT64_LIST:
        newType = Schema.Type.INT64;
        count = feature.getInt64List().getValueCount();
        break;
      case KIND_NOT_SET:
        count = 0;
        break;
      }
      Schema.Type type = types.get(name);
      if (type != null && newType != null) {
        Preconditions.checkArgument(type == newType, "Incompatible types for field %s: %s != %s", name, type, newType);
      }
      if (newType != null) {
        types.put(name, newType);
      }
      if (count != -1) {
        final int c = count;
        if (count >= 1) {
          nonZeroCounts.compute(name, (k, v) -> v == null ? 1 : v + 1);
        }
        maxCounts.compute(name, (k, v) -> v == null ? c : Math.max(v, c));
      }
    });
    return this;
  }

  public Schema getSchema() {
    Schema.Builder builder = Schema.newBuilder();
    for (String name : fields) {
      long nonZeroCount = nonZeroCounts.getOrDefault(name, 0L);

      Schema.Type type = types.get(name);
      Preconditions.checkNotNull(type, String.format("Field type for %s", name));

      int min = nonZeroCount < total ? 0 : 1;
      int max = maxCounts.get(name);

      if (min == 1 && max == 1) {
        builder = builder.required(name, type);
      } else if (min == 0 && max == 1) {
        builder = builder.optional(name, type);
      } else if (max > 1) {
        builder = builder.repeated(name, type);
      }
    }
    return builder.named(name);
  }
}

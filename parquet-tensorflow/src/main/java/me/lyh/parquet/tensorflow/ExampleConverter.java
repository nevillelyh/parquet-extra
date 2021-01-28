package me.lyh.parquet.tensorflow;

import com.google.protobuf.ByteString;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.tensorflow.proto.example.*;

import java.util.List;

class ExampleConverter extends GroupConverter {
  private final String name;
  private final String[] names;
  private final FeatureConverter[] converters;
  private final Features.Builder builder = Features.newBuilder();

  public ExampleConverter(Schema schema) {
    name = schema.getName();
    List<Schema.Field> fields = schema.getFields();
    names = new String[fields.size()];
    converters = new FeatureConverter[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      names[i] = fields.get(i).getName();
      converters[i] = fields.get(i).newConverter();
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    builder.clear();
  }

  @Override
  public void end() {
    for (int i = 0; i < names.length; i++) {
      try {
        Feature feature = converters[i].get();
        if (feature != null) {
          builder.putFeature(names[i], feature);
        }
      } catch (IllegalStateException e) {
        String msg = String.format("Failed to decode %s#%s: %s", name, names[i], e.getMessage());
        throw new ParquetDecodingException(msg, e);
      }
    }
  }

  public Example get() {
    Example example = Example.newBuilder().setFeatures(builder.build()).build();
    builder.clear();
    return example;
  }

  abstract static class FeatureConverter extends PrimitiveConverter {
    abstract public Feature get();
  }

  static class Int64Converter extends FeatureConverter {
    private final Schema.Repetition repetition;
    private final Int64List.Builder builder = Int64List.newBuilder();

    Int64Converter(Schema.Repetition repetition) {
      this.repetition = repetition;
    }

    @Override
    public void addLong(long value) {
      builder.addValue(value);
    }

    @Override
    public Feature get() {
      int n = builder.getValueCount();
      repetition.checkSize(n);
      Feature feature = n == 0 ? null : Feature.newBuilder().setInt64List(builder).build();
      builder.clear();
      return feature;
    }
  }

  static class FloatConverter extends FeatureConverter {
    private final Schema.Repetition repetition;
    private final FloatList.Builder builder = FloatList.newBuilder();

    FloatConverter(Schema.Repetition repetition) {
      this.repetition = repetition;
    }

    @Override
    public void addFloat(float value) {
      builder.addValue(value);
    }

    @Override
    public Feature get() {
      int n = builder.getValueCount();
      repetition.checkSize(n);
      Feature feature = n == 0 ? null : Feature.newBuilder().setFloatList(builder).build();
      builder.clear();
      return feature;
    }
  }

  static class BytesConverter extends FeatureConverter {
    private final Schema.Repetition repetition;
    private final BytesList.Builder builder = BytesList.newBuilder();

    BytesConverter(Schema.Repetition repetition) {
      this.repetition = repetition;
    }

    @Override
    public void addBinary(Binary value) {
      builder.addValue(ByteString.copyFrom(value.getBytes()));
    }

    @Override
    public Feature get() {
      int n = builder.getValueCount();
      repetition.checkSize(n);
      Feature feature = n == 0 ? null : Feature.newBuilder().setBytesList(builder).build();
      builder.clear();
      return feature;
    }
  }
}

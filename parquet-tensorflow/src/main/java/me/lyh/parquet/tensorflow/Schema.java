package me.lyh.parquet.tensorflow;

import com.google.protobuf.ByteString;
import me.lyh.parquet.tensorflow.ExampleConverter.FeatureConverter;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.*;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import shaded.parquet.com.fasterxml.jackson.core.JsonProcessingException;
import shaded.parquet.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

public class Schema {
  public enum Type {
    INT64(PrimitiveType.PrimitiveTypeName.INT64) {
      @Override
      void write(String name, int index, Repetition repetition, RecordConsumer recordConsumer,
                        Feature feature) {
        List<Long> xs = feature.getInt64List().getValueList();
        repetition.checkSize(xs.size());
        if (xs.size() > 0) {
          recordConsumer.startField(name, index);
          xs.forEach(recordConsumer::addLong);
          recordConsumer.endField(name, index);
        }
      }

      @Override
      FeatureConverter newConverter(Repetition repetition) {
        return new ExampleConverter.Int64Converter(repetition);
      }
    },
    FLOAT(PrimitiveType.PrimitiveTypeName.FLOAT) {
      @Override
      void write(String name, int index, Repetition repetition, RecordConsumer recordConsumer,
                 Feature feature) {
        List<Float> xs = feature.getFloatList().getValueList();
        repetition.checkSize(xs.size());
        if (xs.size() > 0) {
          recordConsumer.startField(name, index);
          xs.forEach(recordConsumer::addFloat);
          recordConsumer.endField(name, index);
        }
      }

      @Override
      FeatureConverter newConverter(Repetition repetition) {
        return new ExampleConverter.FloatConverter(repetition);
      }
    },
    BYTES(PrimitiveType.PrimitiveTypeName.BINARY) {
      @Override
      void write(String name, int index, Repetition repetition, RecordConsumer recordConsumer,
                 Feature feature) {
        List<ByteString> xs = feature.getBytesList().getValueList();
        repetition.checkSize(xs.size());
        if (xs.size() > 0) {
          recordConsumer.startField(name, index);
          xs.stream()
              .map(b -> Binary.fromConstantByteArray(b.toByteArray()))
              .forEach(recordConsumer::addBinary);
          recordConsumer.endField(name, index);
        }
      }

      @Override
      FeatureConverter newConverter(Repetition repetition) {
        return new ExampleConverter.BytesConverter(repetition);
      }
    };

    private final PrimitiveType.PrimitiveTypeName parquet;
    Type(PrimitiveType.PrimitiveTypeName parquet) {
      this.parquet = parquet;
    }

    public static Type fromParquet(PrimitiveType.PrimitiveTypeName parquet) {
      switch (parquet) {
        case INT64: return INT64;
        case FLOAT: return FLOAT;
        case BINARY: return BYTES;
      }
      throw new IllegalArgumentException("Unsupported primitive type: " + parquet);
    }

    abstract void write(String name, int index, Repetition repetition,
                        RecordConsumer recordConsumer, Feature feature);
    abstract FeatureConverter newConverter(Repetition repetition);
  }

  public enum Repetition {
    REQUIRED(org.apache.parquet.schema.Type.Repetition.REQUIRED) {
      @Override
      void checkSize(int count) {
        Preconditions.checkState(count == 1, "Required field size != 1: " + count);
      }
    },
    OPTIONAL(org.apache.parquet.schema.Type.Repetition.OPTIONAL) {
      @Override
      void checkSize(int count) {
        Preconditions.checkState(count <= 1, "Required field size > 1: " + count);
      }
    },
    REPEATED(org.apache.parquet.schema.Type.Repetition.REPEATED) {
      @Override
      void checkSize(int count) {}
    };

    private final org.apache.parquet.schema.Type.Repetition parquet;
    Repetition(org.apache.parquet.schema.Type.Repetition parquet) {
      this.parquet = parquet;
    }

    public static Repetition fromParquet(org.apache.parquet.schema.Type.Repetition parquet) {
      switch (parquet) {
        case REQUIRED: return REQUIRED;
        case OPTIONAL: return OPTIONAL;
        case REPEATED: return REPEATED;
      }
      throw new IllegalStateException("This should never happen");
    }
    abstract void checkSize(int count);

  }

  public static class Field {
    private String name;
    private Type type;
    private Repetition repetition;

    private Field() {}

    private Field(String name, Type type, Repetition repetition) {
      this.name = name;
      this.type = type;
      this.repetition = repetition;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    public Repetition getRepetition() {
      return repetition;
    }

    public PrimitiveType toParquet() {
      Types.PrimitiveBuilder<PrimitiveType> builder =
          Types.primitive(type.parquet, repetition.parquet);
      return type.parquet == PrimitiveType.PrimitiveTypeName.INT64
          ? builder.as(LogicalTypeAnnotation.intType(64, true)).named(name)
          : builder.named(name);
    }

    public static Field fromParquet(org.apache.parquet.schema.Type parquet) {
      Preconditions.checkArgument(parquet.isPrimitive(), "Only primitive fields are supported");
      return new Field(
          parquet.getName(),
          Type.fromParquet(parquet.asPrimitiveType().getPrimitiveTypeName()),
          Repetition.fromParquet(parquet.getRepetition()));
    }

    public void write(int index, RecordConsumer recordConsumer, Features features) {
      Feature feature = features.getFeatureOrDefault(name, Feature.getDefaultInstance());
      type.write(name, index, repetition, recordConsumer, feature);
    }

    public FeatureConverter newConverter() {
      return type.newConverter(repetition);
    }
  }

  ////////////////////////////////////////

  public static class Builder {
    private final Set<String> names;
    private final List<Field> fields;

    private Builder() {
      names = new HashSet<>();
      fields = new ArrayList<>();
    }

    public Builder required(String name, Type type) {
      return addField(name, type, Repetition.REQUIRED);
    }

    public Builder optional(String name, Type type) {
      return addField(name, type, Repetition.OPTIONAL);
    }

    public Builder repeated(String name, Type type) {
      return addField(name, type, Repetition.REPEATED);
    }

    public Schema named(String name) {
      return new Schema(name, fields);
    }

    private Builder addField(String name, Type type, Repetition repetition) {
      Preconditions.checkArgument(!names.contains(name), "Duplicate field name %s", name);
      names.add(name);
      fields.add(new Field(name, type, repetition));
      return this;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  ////////////////////////////////////////

  private String name;
  private List<Field> fields;

  private Schema() {}

  private Schema(String name, List<Field> fields) {
    this.name = name;
    this.fields = fields;
  }

  public String getName() {
    return name;
  }

  public List<Field> getFields() {
    return fields;
  }

  ////////////////////////////////////////

  public MessageType toParquet() {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (Field field : fields) {
      builder.addField(field.toParquet());
    }
    return builder.named(name);
  }

  public static Schema fromParquet(MessageType schema) {
    Builder builder = Schema.newBuilder();
    for (org.apache.parquet.schema.Type parquet : schema.getFields()) {
      Field field = Field.fromParquet(parquet);
      builder.addField(field.name, field.type, field.repetition);
    }
    return builder.named(schema.getName());
  }

  ////////////////////////////////////////

  private static final ObjectMapper mapper = new ObjectMapper();

  public String toJson() {
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Schema fromJson(String json) throws IOException {
    return mapper.readValue(json, Schema.class);
  }

  ////////////////////////////////////////

  @Override
  public String toString() {
    return toParquet().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Schema schema = (Schema) o;
    return this.toParquet().equals(schema.toParquet());
  }

  @Override
  public int hashCode() {
    return this.toParquet().hashCode();
  }
}

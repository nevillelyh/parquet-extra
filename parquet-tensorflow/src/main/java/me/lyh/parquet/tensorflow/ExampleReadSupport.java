package me.lyh.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.tensorflow.proto.example.Example;

import java.util.*;
import java.util.stream.Collectors;

public class ExampleReadSupport extends ReadSupport<Example> {
  private Schema schema;
  private Set<String> fields;

  public ExampleReadSupport() {
  }

  public ExampleReadSupport(Schema schema) {
    this.schema = schema;
  }

  public ExampleReadSupport(Collection<String> fields) {
    this.fields = new HashSet<>(fields);
  }

  @Override
  public ReadContext init(InitContext context) {
    MessageType messageType;
    if (schema != null) {
      messageType = schema.toParquet();
    } else if (fields != null) {
      messageType = projectFileSchema(context, fields);
    } else {
      String schemaString = context.getConfiguration().get(ExampleParquetInputFormat.SCHEMA_KEY);
      String fieldsString = context.getConfiguration().get(ExampleParquetInputFormat.FIELDS_KEY);
      if (schemaString != null) {
        messageType = MessageTypeParser.parseMessageType(schemaString);
      } else if (fieldsString != null) {
        fields = Arrays.stream(fieldsString.split(",")).collect(Collectors.toSet());
        messageType = projectFileSchema(context, fields);
      } else {
        messageType = context.getFileSchema();
      }
    }

    return new ReadContext(messageType, Collections.emptyMap());
  }

  @Override
  public RecordMaterializer<Example> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema, ReadContext readContext) {
    return new RecordMaterializer<Example>() {
      private ExampleConverter exampleConverter = new ExampleConverter(
          Schema.fromParquet(readContext.getRequestedSchema()));

      @Override
      public Example getCurrentRecord() {
        return exampleConverter.get();
      }

      @Override
      public GroupConverter getRootConverter() {
        return exampleConverter;
      }
    };
  }

  private static MessageType projectFileSchema(InitContext context, Set<String> fields) {
    MessageType fileSchema = context.getFileSchema();
    Set<String> unmatched = new TreeSet<>(fields);

    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (Type field : fileSchema.getFields()) {
      if (unmatched.contains(field.getName())) {
        builder.addField(field);
        unmatched.remove(field.getName());
      }
    }

    Preconditions.checkState(unmatched.isEmpty(), "Invalid fields: " + unmatched);
    return builder.named(fileSchema.getName());
  }
}

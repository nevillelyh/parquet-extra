package me.lyh.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.tensorflow.example.Example;
import org.tensorflow.example.Features;

import java.util.Collections;

public class ExampleWriteSupport extends WriteSupport<Example> {
  private Schema schema;
  private RecordConsumer recordConsumer;

  public ExampleWriteSupport() {}

  public ExampleWriteSupport(Schema schema) {
    this.schema = schema;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    MessageType messageType;
    if (schema == null) {
      String schemaString = configuration.get(ExampleParquetOutputFormat.SCHEMA_KEY);
      messageType = MessageTypeParser.parseMessageType(schemaString);
      schema = Schema.fromParquet(messageType);
    } else {
      messageType = schema.toParquet();
    }
    return new WriteContext(messageType, Collections.emptyMap());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(Example record) {
    recordConsumer.startMessage();
    int i = 0;
    Features features = record.getFeatures();
    for (Schema.Field field : schema.getFields()) {
      field.write(i, recordConsumer, features);
      i++;
    }
    recordConsumer.endMessage();
  }
}

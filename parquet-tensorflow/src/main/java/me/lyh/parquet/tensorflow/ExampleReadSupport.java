package me.lyh.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.tensorflow.example.Example;

import java.util.Collections;
import java.util.Map;

public class ExampleReadSupport extends ReadSupport<Example> {
  private Schema schema;

  public ExampleReadSupport() {}

  public ExampleReadSupport(Schema schema) {
    this.schema = schema;
  }

  @Override
  public ReadContext init(InitContext context) {
    MessageType messageType;
    if (schema == null) {
      String schemaString = context.getConfiguration().get(ExampleParquetInputFormat.SCHEMA_KEY);
      messageType = MessageTypeParser.parseMessageType(schemaString);
      schema = Schema.fromParquet(messageType);
    } else {
      messageType = schema.toParquet();
    }
    return new ReadContext(messageType, Collections.emptyMap());
  }

  @Override
  public RecordMaterializer<Example> prepareForRead(Configuration configuration,
                                                    Map<String, String> keyValueMetaData,
                                                    MessageType fileSchema,
                                                    ReadContext readContext) {
    return new RecordMaterializer<Example>() {
      private ExampleConverter exampleConverter =
          new ExampleConverter(Schema.fromParquet(readContext.getRequestedSchema()));

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
}

package me.lyh.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.tensorflow.proto.example.Example;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ExampleParquetReader {
  private ExampleParquetReader() {
  }

  public static Builder builder(Path path) {
    return new Builder(path);
  }

  public static Builder builder(InputFile file) {
    return new Builder(file);
  }

  public static class Builder extends ParquetReader.Builder<Example> {
    private Schema schema;
    private Set<String> fields;

    protected Builder(Path path) {
      super(path);
    }

    protected Builder(InputFile file) {
      super(file);
    }

    public Builder withSchema(Schema schema) {
      Preconditions.checkState(fields == null, "Only one of [schema, fields] can be set");
      this.schema = schema;
      return this;
    }

    public Builder withFields(Collection<String> fields) {
      Preconditions.checkState(schema == null, "Only one of [schema, fields] can be set");
      this.fields = new HashSet<>(fields);
      return this;
    }

    @Override
    protected ReadSupport<Example> getReadSupport() {
      if (schema != null) {
        return new ExampleReadSupport(schema);
      } else if (fields != null) {
        return new ExampleReadSupport(fields);
      } else {
        return new ExampleReadSupport();
      }
    }
  }

  public static Schema getSchema(Path path, Configuration conf) throws IOException {
    return getSchema(HadoopInputFile.fromPath(path, conf));
  }

  public static Schema getSchema(InputFile file) throws IOException {
    return Schema.fromParquet(ParquetFileReader.open(file).getFileMetaData().getSchema());
  }
}

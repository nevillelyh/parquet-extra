package me.lyh.parquet.beam;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class BeamOutputFile implements OutputFile {

  private OutputStream outputStream;

  public static BeamOutputFile of(WritableByteChannel channel) {
    return new BeamOutputFile(Channels.newOutputStream(channel));
  }

  public static BeamOutputFile of(ResourceId resourceId) throws IOException {
    return of(FileSystems.create(resourceId, MimeTypes.BINARY));
  }

  public static BeamOutputFile of(String path) throws IOException {
    return of(FileSystems.matchNewResource(path, false));
  }

  private BeamOutputFile(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return new BeamOutputStream(outputStream);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return new BeamOutputStream(outputStream);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  private static class BeamOutputStream extends PositionOutputStream {
    private long position = 0;
    private OutputStream outputStream;

    private BeamOutputStream(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public long getPos() throws IOException {
      return position;
    }

    @Override
    public void write(int b) throws IOException {
      position++;
      outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
      position += len;
    }

    @Override
    public void flush() throws IOException {
      outputStream.flush();
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
    }
  }
}

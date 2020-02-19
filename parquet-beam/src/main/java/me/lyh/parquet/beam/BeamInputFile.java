package me.lyh.parquet.beam;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

public class BeamInputFile implements InputFile {

  private final SeekableByteChannel channel;

  public static BeamInputFile of(SeekableByteChannel channel) {
    return new BeamInputFile(channel);
  }

  public static BeamInputFile of(ResourceId resourceId) throws IOException {
    return of((SeekableByteChannel) FileSystems.open(resourceId));
  }

  public static BeamInputFile of(String path) throws IOException {
    return of(FileSystems.matchSingleFileSpec(path).resourceId());
  }

  private BeamInputFile(SeekableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public long getLength() throws IOException {
    return channel.size();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new BeamInputStream(channel);
  }

  private static class BeamInputStream extends DelegatingSeekableInputStream {
    private final SeekableByteChannel channel;

    private BeamInputStream(SeekableByteChannel channel) {
      super(Channels.newInputStream(channel));
      this.channel = channel;
    }

    @Override
    public long getPos() throws IOException {
      return channel.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
      channel.position(newPos);
    }
  }
}

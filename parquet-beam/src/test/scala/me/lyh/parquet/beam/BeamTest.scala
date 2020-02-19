package me.lyh.parquet.beam

import java.nio.file.Files

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BeamTest extends AnyFlatSpec with Matchers {
  private val tmpDir = Files.createTempDirectory("parquet-beam-")
  tmpDir.de
}

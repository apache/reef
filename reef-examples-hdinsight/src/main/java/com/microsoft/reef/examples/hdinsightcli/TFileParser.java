package com.microsoft.reef.examples.hdinsightcli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.logging.Logger;

/**
 * Parse TFile's content to key value pair
 */
public final class TFileParser {
  private static final Logger LOG = Logger.getLogger(TFileParser.class.getName());
  private final FileSystem fileSystem;
  private final Configuration configuration;

  public TFileParser(final Configuration conf, final FileSystem fs) {
    this.configuration = conf;
    this.fileSystem = fs;
  }

  public void parseOneFile(final Path inputPath, final Writer outputWriter) throws IOException {
    try (
        final FSDataInputStream fsdis = fileSystem.open(inputPath);
        final TFile.Reader reader = new TFile.Reader(fsdis, fileSystem.getFileStatus(inputPath).getLen(), configuration);
        final TFile.Reader.Scanner scanner = reader.createScanner()
    ) {
      int count = 0;
      while (!scanner.atEnd()) {
        if (count != 3) {
          //skip VERSION, APPLICATION_ACL, and APPLICATION_OWNER
          scanner.advance();
          count++;
          continue;
        }

        final TFile.Reader.Scanner.Entry entry = scanner.entry();

        try (
            final DataInputStream disKey = entry.getKeyStream();
            final DataInputStream disValue = entry.getValueStream();
        ) {
          outputWriter.write("Container: ");
          outputWriter.write(disKey.readUTF());
          outputWriter.write("\r\n");

          while (disValue.available() > 0) {
            String strFileName = disValue.readUTF();
            String strLength = disValue.readUTF();
            outputWriter.write("=====================================================");
            outputWriter.write("\r\n");
            outputWriter.write("File Name: " + strFileName);
            outputWriter.write("\r\n");
            outputWriter.write("File Length: " + strLength);
            outputWriter.write("\r\n");
            outputWriter.write("-----------------------------------------------------");
            outputWriter.write("\r\n");

            final byte[] buf = new byte[65535];
            int lenRemaining = Integer.parseInt(strLength);
            while (lenRemaining > 0) {
              int len = disValue.read(buf, 0, lenRemaining > 65535 ? 65535 : lenRemaining);
              if (len > 0) {
                outputWriter.write(new String(buf, 0, len, "UTF-8"));
                lenRemaining -= len;
              } else {
                break;
              }
            }
            outputWriter.write("\r\n");
          }
        }
        break;
      } //end of while(!scanner.atEnd())
    }
  }
}

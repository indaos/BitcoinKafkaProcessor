package app;

import java.io.FilenameFilter;
import java.util.Arrays;
import java.io.File;
import java.util.LinkedList;
import java.util.stream.Stream;

public class BitcoinsDataFiles {

  private String path;
  private LinkedList<String> files;

  public BitcoinsDataFiles(String path) {
    this.path = path;
  }

  public BitcoinsDataFiles loadFiles() {
    String[] list = new File(path).list(
        (dir, name) -> name.startsWith("blk") && name.endsWith("dat"));
    assert list != null;
    files = new LinkedList<>(Arrays.asList(list));
    return this;
  }

  public String getNextFile() {
    return files.pollFirst();
  }

  public Stream<String> stream() {
    return files.stream();
  }

}

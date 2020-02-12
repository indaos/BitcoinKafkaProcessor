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
        this.path=path;
    }

    public BitcoinsDataFiles  loadFiles() {
        String[] list=new File(path).list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("blk") && name.endsWith("dat");
            }
        });
        Arrays.sort(list);
        files=new LinkedList<>(Arrays.asList(list));
        return this;
    }

    public String getNextFile() {
        return files.pollFirst();
    }

    public Stream<String> stream() {
        return files.stream();
    }

}
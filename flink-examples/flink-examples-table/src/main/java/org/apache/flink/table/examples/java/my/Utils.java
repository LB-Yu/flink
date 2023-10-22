package org.apache.flink.table.examples.java.my;

import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class Utils {

    /** Creates a temporary file with the contents and returns the absolute path. */
    public static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile(UUID.randomUUID().toString(), ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }
}

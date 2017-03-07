package org.github.mitallast.taskflow.common;

import org.apache.logging.log4j.core.util.StringBuilderWriter;

import java.io.*;

public class IOUtils {

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
    private static final int EOF = -1;

    public static int copy(Reader input, Writer output) throws IOException {
        long count = copyLarge(input, output);
        if (count > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) count;
    }

    public static long copyLarge(Reader input, Writer output) throws IOException {
        return copyLarge(input, output, new char[DEFAULT_BUFFER_SIZE]);
    }

    public static long copyLarge(Reader input, Writer output, char[] buffer) throws IOException {
        long count = 0;
        int n;
        while (EOF != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    public static String toString(InputStream input) throws IOException {
        return toString(new InputStreamReader(input));
    }

    public static String toString(InputStream input, String charsetName) throws IOException {
        return toString(new InputStreamReader(input, charsetName));
    }

    public static String toString(Reader input) throws IOException {
        final StringBuilderWriter sw = new StringBuilderWriter();
        copy(input, sw);
        return sw.toString();
    }

    public static void write(File file, String content) throws IOException {
        try (
            FileOutputStream output = new FileOutputStream(file);
            OutputStreamWriter writer = new OutputStreamWriter(output)
        ) {
            writer.write(content);
        }
    }
}

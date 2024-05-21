import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SplitLogs {

    public static void main(String[] args) {

        String folder = "C:\\Program Files\\PostgreSQL\\15\\data\\log";

        try (Stream<Path> paths = Files.walk(Paths.get(folder))) {
            paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".csv"))
                    .forEach(SplitLogs::splitFile); // replace this with your processing logic
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class OpenFile implements AutoCloseable {
        OutputStream outputStream;
        String fileName;
        BufferedWriter bufferedWriter;

        @Override
        public void close() throws Exception {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }

        }
    }

    private static void splitFile(Path path) {
        final String outputFolder = "C:\\db2logs";
        HashMap<String, OpenFile> openFiles = new HashMap<>();
        OpenFile lastOpenFile = null;

        try (InputStream inputStream = Files.newInputStream(path)) {
            try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] split = line.split(",");
                    String schema = split.length >= 3 ? split[2] : null;
                    if (schema == null || schema.isEmpty()) {
                        schema = "postgres";
                    }
                    if (schema.length() > 2 && schema.charAt(0) == '"' && schema.charAt(schema.length() - 1) == '"') {
                        schema = schema.substring(1, schema.length() - 1);
                    }
                    if (!schema.startsWith("schema") && !schema.equals("postgres")) {
                        if (lastOpenFile == null) {
                            schema = "postgres";
                        } else {
                            lastOpenFile.bufferedWriter.write(line);
                            lastOpenFile.bufferedWriter.newLine();
                            continue;
                        }
                    }
                    OpenFile openFile = openFiles.get(schema);
                    lastOpenFile = openFile;
                    if (openFile == null) {
                        openFile = new OpenFile();
                        openFile.fileName = outputFolder + "\\" + path.getName(path.getNameCount() - 1).toString().replace("postgresql", schema.equals("postgres") ? "postgresql" : schema);
                        openFile.outputStream = new FileOutputStream(openFile.fileName);
                        openFile.bufferedWriter = new BufferedWriter(new OutputStreamWriter(openFile.outputStream));
                        openFiles.put(schema, openFile);
                    }
                    openFile.bufferedWriter.write(line);
                    openFile.bufferedWriter.newLine();
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            for (OpenFile openFile : openFiles.values()) {
                try {
                    openFile.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }

    }
}

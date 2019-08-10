package in.nimbo.common.utility;

import in.nimbo.common.exception.CompressionException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtility {
    private CompressionUtility() {
    }

    public static byte[] compress(String value) {
        byte[] dataToCompress = value.getBytes(StandardCharsets.ISO_8859_1);
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(dataToCompress.length);
             GZIPOutputStream zipStream = new GZIPOutputStream(byteStream)) {
            zipStream.write(dataToCompress);
            zipStream.finish();
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new CompressionException(e);
        }
    }

    public static String decompress(byte[] bytes) {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes));
             Scanner scanner = new Scanner(gzipInputStream)) {
            StringBuilder stringBuilder = new StringBuilder();
            while (scanner.hasNextLine()) {
                stringBuilder.append(scanner.nextLine()).append('\n');
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            throw new CompressionException(e);
        }
    }
}

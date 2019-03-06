package project.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class ReaderTest {

    public static void main(String[] args) {
        int from, to;
        Scanner scanner = new Scanner(System.in);
        System.out.print("from_line:");
        from = Integer.parseInt(scanner.nextLine());
        System.out.print("to_line:");
        to = Integer.parseInt(scanner.nextLine());

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("/home/mark/Desktop/CC-MAIN-20190224023850-20190224045850-00324.warc"));
            String line = reader.readLine();
            int i = 0;
            while (i >= from && i <= to && line != null) {
                System.out.println(line);
                line = reader.readLine();
                i++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

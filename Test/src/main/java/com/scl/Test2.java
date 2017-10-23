package com.scl;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

public class Test2 {
    private Integer sum;

    /*
    *   把G:\BD1702\bd1702maven\Test\src下的所有文件读取出来
    *   放到一个文件中
    *
    * */
    @Test
    public void test() throws IOException {
        String path = "G:\\BD1702\\bd1702maven\\Hadoop\\src";
        String outputPath = "G:\\BD1702\\bd1702maven\\Hadoop\\src\\a.txt";
        Files.createFile(Paths.get(outputPath));
        Files.walk(Paths.get(path)).forEach(a-> {
            if(a.toFile().isFile()) {
                try {
                    Files.write(Paths.get(outputPath),Files.readAllBytes(a), StandardOpenOption.APPEND);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void test_1() throws IOException {
        Files.lines(Paths.get("G:\\BD1702\\bd1702maven\\Hadoop\\src\\a.txt"))
                .flatMap(s-> Stream.of(s.split("[.]")))
                .filter(a->a.length()>0)
                .map(String::toUpperCase)
                .distinct()
                .sorted()
                .forEach(System.out::println);
    }

    @Test
    public void tess_2() {
        int i = Integer.parseInt("+999");
        int x = Integer.parseInt("-999");

        System.out.println(i+":"+x);
    }

    @Test
    public void test_3(){
        sum = 0;
        for (int i = 0; i < 10; i++) {
            sum+=i;
        }
        System.out.println(sum);
    }
}

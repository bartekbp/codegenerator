package org.bartekbp;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.hash.BloomFilter;
import com.google.common.io.Files;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class Main {
    private static Path outputDir = Paths.get("output");

    public static void main(String... args) throws IOException, InterruptedException {
        int numbersToGenerate = Integer.parseInt(args[0]);
        int codeLength = 10;
        char[] alphabet = "abcdefghijklmnoprstuwz".toCharArray();
        int cpuCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService =
                new ThreadPoolExecutor(cpuCount, cpuCount, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        Stopwatch stopWatch = Stopwatch.createStarted();
        for(int i = 0; i < alphabet.length; i++) {
            int j = i;

            executorService.submit(() -> {
                int codesToGenerate = numbersToGenerate / alphabet.length;
                codesToGenerate += ((codesToGenerate * alphabet.length + j) < numbersToGenerate) ? 1 : 0;
                try {
                    generateCodes(codesToGenerate, codeLength - 1, Character.toString(alphabet[j]), alphabet);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);

        stopWatch.stop();
        System.out.println("Elapsed: " + stopWatch.elapsed(TimeUnit.MILLISECONDS) + "[ms]");
    }

    private static void generateCodes(int numbersToGenerate, int codeLength, String prefix, char[] alphabet) throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int batchSize = 10000;
        int maxLetter = alphabet.length;
        BloomFilter<String> codeFilter = BloomFilter.create((item, sink) -> {
            sink.putString(item, Charsets.UTF_8);
        }, numbersToGenerate, 0.03);

        File outputFile = outputDir.resolve(Paths.get(String.format("codes_%s.txt", prefix))).toFile();
        outputDir.toFile().mkdir();
        try(BufferedWriter out = Files.newWriter(outputFile, Charsets.UTF_8)) {
            for (int i = 0; i < numbersToGenerate; i += batchSize) {
                List<String> codes = new ArrayList<>(batchSize);
                for (int j = 0; j < Math.min(batchSize, numbersToGenerate - i); j++) {
                    boolean itemAdded = false;
                    while(!itemAdded) {
                        StringBuilder builder = new StringBuilder();
                        for (int letterIndex = 0; letterIndex < codeLength; letterIndex++) {
                            builder.append(alphabet[random.nextInt(maxLetter)]);
                        }

                        String code = builder.toString();
                        if (codeFilter.put(code)) {
                            codes.add(prefix + code);
                            itemAdded = true;
                        }
                    }
                }

                out.write(codes.stream().collect(Collectors.joining("\n")));
                out.write('\n');
            }
        }
    }
}

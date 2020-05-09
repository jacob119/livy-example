package org.livy.example;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import java.io.File;
import java.net.URI;

public class PiApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PiJob <livy url> <slices>");
            System.exit(-1);
        }

        LivyClient client = new LivyClientBuilder()
                .setURI(new URI(args[0]))
                .build();

        try {
            System.out.println("Uploading livy-example jar to the SparkContext...");
            for (String s : System.getProperty("java.class.path").split(File.pathSeparator)) {
                if (new File(s).getName().startsWith("livy-examples")) {
                    client.uploadJar(new File(s)).get();
                    break;
                }
            }

            final int slices = Integer.parseInt(args[1]);
            double pi = client.submit(new PiJob(slices)).get();

            System.out.println("Pi is roughly " + pi);
        } finally {
            client.stop(true);
        }
    }
}

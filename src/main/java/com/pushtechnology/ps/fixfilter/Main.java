package com.pushtechnology.ps.fixfilter;

import com.pushtechnology.diffusion.client.features.Topics;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {

    private final Config config;

    private DiffusionWrapper diffusion;

    private final BlockingQueue<UpdateItem> updateQueue = new ArrayBlockingQueue<>(100);

    public Main(String configFilename) {
        config = new Config(configFilename);
    }

    public Config getConfig() {
        return config;
    }

    public void run() {

        System.out.println("*********************");
        System.out.println("* FIXFilter started *");
        System.out.println("*********************");

        boolean running = true;

        // At shutdown, stop all rules which also gracefully closes all sessions.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (diffusion != null) {
                diffusion.close();
            }
        }));

        diffusion = new DiffusionWrapper(config);
        diffusion.connect();

        if (!diffusion.isConnected()) {
            System.err.println("Couldn't make a connection to Diffusion, aborting");
            System.exit(1);
        }

        // Start thread to process updates
        new Thread(() -> {
            UpdateItem item;
            while (running) {
                try {
                    item = updateQueue.take();
                    diffusion.updateTopic(item);
                }
                catch(InterruptedException ignore) {}
            }
        }).start();

        final String topicSelector = "?" + getConfig().getTopicSource() + "//";

        diffusion.getSession().feature(Topics.class)
                .addStream(topicSelector,
                        String.class,
                        new FIXStream(getConfig().getTopicSource(),
                                getConfig().getFieldSeparator(),
                                getConfig().getPairSeparator(),
                                getConfig().getFIXFields(),
                                updateQueue));
        diffusion.getSession().feature(Topics.class)
                .subscribe(topicSelector);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: " + Main.class.getName() + " <config_file> ");
            System.exit(1);
        }

        Main app = new Main(args[0]);
        app.run();
    }
}

package co.f4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: java co.f4.Main [consumer|producer] <topic> <group>");
        }

        if (args[0].toLowerCase().equals("producer")) {
            logger.debug("Running producer...");
            Producer.main(args);
        } else if (args[0].toLowerCase().equals("consumer")) {
            logger.debug("Running consumer...");
            Consumer.main(args);
        } else {
            throw new IllegalArgumentException("Unknown argument " + args[0]);
        }
    }
}
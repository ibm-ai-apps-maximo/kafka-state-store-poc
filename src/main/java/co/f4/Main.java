package co.f4;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: java co.f4.Main [consumer|producer] <topic> <group>");
        }

        if (args[0].toLowerCase().equals("producer")) {
            System.out.println("Running producer...");
            Producer.main(args);
        } else if (args[0].toLowerCase().equals("consumer")) {
            System.out.println("Running consumer...");
            Consumer.main(args);
        } else {
            throw new IllegalArgumentException("Unknown argument " + args[0]);
        }
    }
}
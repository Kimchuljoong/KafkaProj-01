package com.practice.kafka.producer;

import com.github.javafaker.Faker;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class FileUtilAppend {

    private static final List<String> pizzaNames = List.of("Potato Pizza", "Cheese Pizza",
            "Cheese Garlic Pizza", "Super Supreme", "Peperoni");

    private static final List<String> pizzaShop = List.of("A001", "B001", "C001",
            "D001", "E001", "F001", "G001", "H001", "I001", "J001", "K001", "L001", "M001", "N001",
            "O001", "P001", "Q001");

    private static int orderSeq = 5000;
    public FileUtilAppend() {}

    private String getRandomValueFromList(List<String> list, Random random) {
        int size = list.size();
        int index = random.nextInt(size);

        return list.get(index);
    }

    public HashMap<String, String> produce_msg(Faker faker, Random random, int id) {

        String shopId = getRandomValueFromList(pizzaShop, random);
        String pizzaName = getRandomValueFromList(pizzaNames, random);

        String ordId = "ord"+id;
        String customerName = faker.name().fullName();
        String phoneNumber = faker.phoneNumber().phoneNumber();
        String address = faker.address().streetAddress();
        LocalDateTime now = LocalDateTime.now();
        String message = String.format("%s, %s, %s, %s, %s, %s, %s"
                , ordId, shopId, pizzaName, customerName, phoneNumber, address
                , now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN)));

        HashMap<String, String> messageMap = new HashMap<>();
        messageMap.put("key", shopId);
        messageMap.put("message", message);

        return messageMap;
    }

    public void writeMessage(String filePath, Faker faker, Random random) {
        try {
            File file = new File(filePath);
            if(!file.exists()) {
                file.createNewFile();
            }

            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            PrintWriter printWriter = new PrintWriter(bufferedWriter);

            for(int i=0; i < 50; i++) {
                HashMap<String, String> message = produce_msg(faker, random, orderSeq++);
                printWriter.println(message.get("key")+"," + message.get("message"));
            }
            printWriter.close();

        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        FileUtilAppend fileUtilAppend = new FileUtilAppend();

        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        String filePath = "/Users/kimchj/Documents/practice/KafkaProj-01/practice/src/main/resources/pizza_append.txt";

        for(int i=0; i<1000; i++) {

            fileUtilAppend.writeMessage(filePath, faker, random);
            System.out.println("###### iteration:"+i+" file write is done");
            try {

                Thread.sleep(500);
            }catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
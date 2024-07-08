package org.iitj.mongodb;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MongoDBCRUD1 {

    private static MongoDatabase database;

    public static void main(String[] args) {
        // Connect to MongoDB Atlas cluster
        String uri = "mongodb+srv://g23ai1042:Yo9Fowj3g3SOgMDQ@cluster0.iftgoz0.mongodb.net/";  // Replace with your MongoDB Atlas connection string
        MongoClient mongoClient = MongoClients.create(uri);
        database = mongoClient.getDatabase("customerdb_iitj");  //Replace with your database name

        System.out.println("Connection successful");
        
        // Load data into collections
        //load("C:\\IITJ\\BDM\\Data\\Assig-6\\data\\customer.tbl", "customers");
        //load("C:\\IITJ\\BDM\\Data\\Assig-6\\data\\order.tbl", "orders");
        //loadNest("C:\\IITJ\\BDM\\Data\\Assig-6\\data\\customer.tbl", "C:\\IITJ\\BDM\\Data\\Assig-6\\data\\order.tbl", "custorders");

        // Example usage of queries
        System.out.println(query1(1000));
        /*
        System.out.println(query2(32));
        System.out.println(query2Nest(32));
        System.out.println(query3());
        System.out.println(query3Nest());
        System.out.println(toString(query4()));
        System.out.println(toString(query4Nest()));
        */
    }

    // Method to load data from file into a MongoDB collection
    public static void load(String filePath, String collectionName) {
        MongoCollection<Document> collection =
database.getCollection(collectionName);
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split("\\|");
                Document doc = new Document();
                for (int i = 0; i < fields.length; i++) {
                    doc.append("field" + i, fields[i]);
                }
                collection.insertOne(doc);
                System.out.println("Inserted document: " + doc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to load nested data into a MongoDB collection
    public static void loadNest(String customerFilePath, String
orderFilePath, String collectionName) {
        MongoCollection<Document> collection =
database.getCollection(collectionName);
        try (BufferedReader customerBr = new BufferedReader(new
FileReader(customerFilePath));
             BufferedReader orderBr = new BufferedReader(new
FileReader(orderFilePath))) {

            String customerLine;
            while ((customerLine = customerBr.readLine()) != null) {
                String[] customerFields = customerLine.split("\\|");
                Document customerDoc = new Document();
                for (int i = 0; i < customerFields.length; i++) {
                    customerDoc.append("field" + i, customerFields[i]);
                }

                String orderLine;
                while ((orderLine = orderBr.readLine()) != null) {
                    String[] orderFields = orderLine.split("\\|");
                    if (orderFields[1].equals(customerFields[0])) {
                        Document orderDoc = new Document();
                        for (int i = 0; i < orderFields.length; i++) {
                            orderDoc.append("field" + i, orderFields[i]);
                        }
                        customerDoc.append("orders", orderDoc);
                    }
                }
                collection.insertOne(customerDoc);
                System.out.println("Inserted nested document: " + customerDoc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to query customer name by customer ID
    public static String query1(int custkey) {
        System.out.println("\nExecuting query 1:");
        MongoCollection<Document> col = database.getCollection("customers");
        Document result = col.find(new Document("field0", custkey)).first();
        return result != null ? result.getString("field1") : null;
    }

    // Method to query order date by order ID
    public static String query2(int orderId) {
        System.out.println("\nExecuting query 2:");
        MongoCollection<Document> col = database.getCollection("orders");
        Document result = col.find(new Document("field0", orderId)).first();
        return result != null ? result.getString("field3") : null;
    }

    // Method to query order date by order ID using nested collection
    public static String query2Nest(int orderId) {
        System.out.println("\nExecuting query 2 nested:");
        MongoCollection<Document> col = database.getCollection("custorders");
        Document result = col.find(new Document("orders.field0",
orderId)).first();
        return result != null ? result.getEmbedded(Arrays.asList("orders", "field3"), String.class) : null;
    }

    // Method to count total number of orders
    public static long query3() {
        System.out.println("\nExecuting query 3:");
        MongoCollection<Document> col = database.getCollection("orders");
        return col.countDocuments();
    }

    // Method to count total number of orders using nested collection
    public static long query3Nest() {
        System.out.println("\nExecuting query 3 nested:");
        MongoCollection<Document> col = database.getCollection("custorders");
        long count = 0;
        for (Document doc : col.find()) {
            count += ((List<?>) doc.get("orders")).size();
        }
        return count;
    }

    // Method to get top 5 customers based on total order amount
    public static MongoCursor<Document> query4() {
        System.out.println("\nExecuting query 4:");
        MongoCollection<Document> customers =
database.getCollection("customers");
        MongoCollection<Document> orders = database.getCollection("orders");

        // Implement the aggregation logic here
        return null;
    }

    // Method to get top 5 customers based on total order amount using nested collection
    public static MongoCursor<Document> query4Nest() {
        System.out.println("\nExecuting query 4 nested:");
        MongoCollection<Document> col = database.getCollection("custorders");

        // Implement the aggregation logic here
        return null;
    }

    // Helper method to print MongoCursor results
    public static String toString(MongoCursor<Document> cursor) {
        StringBuilder buf = new StringBuilder();
        int count = 0;
        buf.append("Rows:\n");
        if (cursor != null) {
            while (cursor.hasNext()) {
                Document obj = cursor.next();
                buf.append(obj.toJson());
                buf.append("\n");
                count++;
            }
            cursor.close();
        }
        buf.append("Number of rows: " + count);
        return buf.toString();
    }
}
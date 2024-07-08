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
import java.util.ArrayList;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Sorts.*;

public class MongoDBCRUD {

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
        System.out.println(query1("1000"));
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
                if (collectionName.equals("customers")) {
                    doc.append("c_custkey", fields[0])
                        .append("c_name", fields[1])
                        .append("c_address", fields[2])
                        .append("c_nationkey", fields[3])
                        .append("c_phone", fields[4])
                        .append("c_acctbal", fields[5])
                        .append("c_mktsegment", fields[6])
                        .append("c_comment", fields[7]);
                } else if (collectionName.equals("orders")) {
                    doc.append("o_orderkey", fields[0])
                        .append("o_custkey", fields[1])
                        .append("o_orderstatus", fields[2])
                        .append("o_totalprice", fields[3])
                        .append("o_orderdate", fields[4])
                        .append("o_orderpriority", fields[5])
                        .append("o_clerk", fields[6])
                        .append("o_shippriority", fields[7])
                        .append("o_comment", fields[8]);
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
                Document customerDoc = new Document()
                        .append("c_custkey", customerFields[0])
                        .append("c_name", customerFields[1])
                        .append("c_address", customerFields[2])
                        .append("c_nationkey", customerFields[3])
                        .append("c_phone", customerFields[4])
                        .append("c_acctbal", customerFields[5])
                        .append("c_mktsegment", customerFields[6])
                        .append("c_comment", customerFields[7]);

                List<Document> orders = new ArrayList<>();
                String orderLine;
                while ((orderLine = orderBr.readLine()) != null) {
                    String[] orderFields = orderLine.split("\\|");
                    if (orderFields[1].equals(customerFields[0])) {
                        Document orderDoc = new Document()
                                .append("o_orderkey", orderFields[0])
                                .append("o_custkey", orderFields[1])
                                .append("o_orderstatus", orderFields[2])
                                .append("o_totalprice", orderFields[3])
                                .append("o_orderdate", orderFields[4])
                                .append("o_orderpriority", orderFields[5])
                                .append("o_clerk", orderFields[6])
                                .append("o_shippriority", orderFields[7])
                                .append("o_comment", orderFields[8]);
                        orders.add(orderDoc);
                    }
                }
                customerDoc.append("orders", orders);
                collection.insertOne(customerDoc);
                System.out.println("Inserted nested document: " + customerDoc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to query customer name by customer ID
    public static String query1(String custkey) {
        System.out.println("\nExecuting query 1:");
        MongoCollection<Document> col = database.getCollection("customers");
        Document result = col.find(new Document("c_custkey", custkey)).first();
        return result != null ? result.getString("c_name") : null;
    }

    // Method to query order date by order ID
    public static String query2(int orderId) {
        System.out.println("\nExecuting query 2:");
        MongoCollection<Document> col = database.getCollection("orders");
        Document result = col.find(new Document("o_orderkey", orderId)).first();
        return result != null ? result.getString("o_orderdate") : null;
    }

    // Method to query order date by order ID using nested collection
    public static String query2Nest(int orderId) {
        System.out.println("\nExecuting query 2 nested:");
        MongoCollection<Document> col = database.getCollection("custorders");
        Document result = col.find(new Document("orders.o_orderkey",
orderId)).first();
        return result != null ?
result.getEmbedded(Arrays.asList("orders", "o_orderdate"),
String.class) : null;
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

        MongoCursor<Document> cursor = orders.aggregate(Arrays.asList(
                group("$o_custkey", sum("totalAmount", "$o_totalprice")),
                lookup("customers", "_id", "c_custkey", "customerDetails"),
                sort(descending("totalAmount")),
                limit(5)
        )).iterator();

        return cursor;
    }

    // Method to get top 5 customers based on total order amount using nested collection
    public static MongoCursor<Document> query4Nest() {
        System.out.println("\nExecuting query 4 nested:");
        MongoCollection<Document> col = database.getCollection("custorders");

        MongoCursor<Document> cursor = col.aggregate(Arrays.asList(
                unwind("$orders"),
                group("$c_custkey", sum("totalAmount", "$orders.o_totalprice")),
                lookup("custorders", "_id", "c_custkey", "customerDetails"),
                sort(descending("totalAmount")),
                limit(5)
        )).iterator();

        return cursor;
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


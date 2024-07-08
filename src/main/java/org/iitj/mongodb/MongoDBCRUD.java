package org.iitj.mongodb;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Sorts.descending;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDBCRUD {

	private static MongoDatabase database;

	public static void main(String[] args) {
		// Connect to MongoDB Atlas cluster
		String uri = "mongodb+srv://g23ai1042:Yo9Fowj3g3SOgMDQ@cluster0.iftgoz0.mongodb.net/"; // Replace with your
																								// MongoDB Atlas
																								// connection string
		MongoClient mongoClient = MongoClients.create(uri);
		database = mongoClient.getDatabase("customerdb_iitj"); // Replace with your database name

		System.out.println("Connection successful");

		// Load data into collections
		load("C:\\IITJ\\BDM\\Data\\Assig-6\\data\\customer.tbl", "customers");
		load("C:\\IITJ\\BDM\\Data\\Assig-6\\data\\order.tbl", "orders");
		loadNest("C:\\IITJ\\BDM\\Data\\Assig-6\\data\\customer.tbl", "C:\\IITJ\\BDM\\Data\\Assig-6\\data\\order.tbl",
				"custorders");

		query1("1000");
		query2("32");
		query2Nest("32");
		query3();
		query3Nest();
		query4();
		query4Nest();

	}

	// Method to load data from file into a MongoDB collection
	public static void load(String filePath, String collectionName) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");
				Document doc = new Document();
				if (collectionName.equals("customers")) {
					doc.append("c_custkey", fields[0]).append("c_name", fields[1]).append("c_address", fields[2])
							.append("c_nationkey", fields[3]).append("c_phone", fields[4])
							.append("c_acctbal", fields[5]).append("c_mktsegment", fields[6])
							.append("c_comment", fields[7]);
				} else if (collectionName.equals("orders")) {
					doc.append("o_orderkey", fields[0]).append("o_custkey", fields[1])
							.append("o_orderstatus", fields[2]).append("o_totalprice", fields[3])
							.append("o_orderdate", fields[4]).append("o_orderpriority", fields[5])
							.append("o_clerk", fields[6]).append("o_shippriority", fields[7])
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
	public static void loadNest(String customerFilePath, String orderFilePath, String collectionName) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		try (BufferedReader customerBr = new BufferedReader(new FileReader(customerFilePath))) {

			String customerLine;
			while ((customerLine = customerBr.readLine()) != null) {
				String[] customerFields = customerLine.split("\\|");
				Document customerDoc = new Document().append("c_custkey", customerFields[0])
						.append("c_name", customerFields[1]).append("c_address", customerFields[2])
						.append("c_nationkey", customerFields[3]).append("c_phone", customerFields[4])
						.append("c_acctbal", customerFields[5]).append("c_mktsegment", customerFields[6])
						.append("c_comment", customerFields[7]);

				List<Document> orders = new ArrayList<>();
				String orderLine;

				try (BufferedReader orderBr = new BufferedReader(new FileReader(orderFilePath))) {
					while ((orderLine = orderBr.readLine()) != null) {
						String[] orderFields = orderLine.split("\\|");
						if (orderFields[1].equals(customerFields[0])) {
							Document orderDoc = new Document().append("o_orderkey", orderFields[0])
									.append("o_custkey", orderFields[1]).append("o_orderstatus", orderFields[2])
									.append("o_totalprice", orderFields[3]).append("o_orderdate", orderFields[4])
									.append("o_orderpriority", orderFields[5]).append("o_clerk", orderFields[6])
									.append("o_shippriority", orderFields[7]).append("o_comment", orderFields[8]);
							orders.add(orderDoc);
						}
					}
					customerDoc.append("orders", orders);
					collection.insertOne(customerDoc);
					System.out.println("Inserted nested document: " + customerDoc);
				} catch (IOException e) {
					e.printStackTrace();
				}
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
		String custName = result != null ? result.getString("c_name") : null;
		System.out.println("Customer Name: " + custName);
		return custName;
	}

	// Method to query order date by order ID
	public static String query2(String orderId) {
		System.out.println("\nExecuting query 2:");
		MongoCollection<Document> col = database.getCollection("orders");
		Document result = col.find(new Document("o_orderkey", orderId)).first();
		String orderDate = result != null ? result.getString("o_orderdate") : null;
		System.out.println("Order Date: " + orderDate);
		return orderDate;
	}

	public static String query2Nest(String orderId) {
		System.out.println("\nExecuting query 2 nested:");
		MongoCollection<Document> col = database.getCollection("custorders");
		Document result = col.find(new Document("orders.o_orderkey", orderId)).first();

		List<Document> orders = (List<Document>) result.get("orders");
		for (Document order : orders) {
			if (orderId.equalsIgnoreCase(order.getString("o_orderkey"))) {
				String orderDate = order.getString("o_orderdate");
				System.out.println("Order Date: " + orderDate);
				return orderDate;
			}
		}
		return null;
	}

	// Method to count total number of orders
	public static long query3() {
		System.out.println("\nExecuting query 3:");
		MongoCollection<Document> col = database.getCollection("orders");
		long count = col.countDocuments();
		System.out.println("Total number of Orders: " + count);
		return count;
	}

	// Method to count total number of orders using nested collection
	public static long query3Nest() {
		System.out.println("\nExecuting query 3 nested:");
		MongoCollection<Document> col = database.getCollection("custorders");
		long count = 0;
		for (Document doc : col.find()) {
			count += ((List<?>) doc.get("orders")).size();
		}
		System.out.println("Total number of Orders: " + count);
		return count;
	}

	// Method to get top 5 customers based on total order amount
	public static void query4() {
		System.out.println("\nExecuting query 4:");
		MongoCollection<Document> customers = database.getCollection("customers");
		MongoCollection<Document> orders = database.getCollection("orders");
		MongoCursor<Document> cursor = null;
		try {
			cursor = orders.aggregate(Arrays.asList(
					project(new Document("o_custkey", 1).append("o_totalprice",
							new Document("$toDouble", "$o_totalprice"))),
					group("$o_custkey", sum("totalAmount", "$o_totalprice")),
					lookup("customers", "_id", "c_custkey", "customerDetails"), sort(descending("totalAmount")),
					limit(5))).iterator();
			System.out.println(toString(cursor));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (cursor != null)
				cursor.close();
		}

	}

	// Method to get top 5 customers based on total order amount using nested
	// collection
	public static void query4Nest() {
		System.out.println("\nExecuting query 4 nested:");
		MongoCollection<Document> col = database.getCollection("custorders");
		MongoCursor<Document> cursor = null;
		try {

			cursor = col.aggregate(Arrays.asList(unwind("$orders"),
					project(new Document("c_custkey", 1).append("o_totalprice",
							new Document("$toDouble", "$orders.o_totalprice"))),
					group("$c_custkey", sum("totalAmount", "$o_totalprice")),
					lookup("customers", "_id", "c_custkey", "customerDetails"), sort(descending("totalAmount")),
					limit(5))).iterator();
			System.out.println(toString(cursor));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (cursor != null)
				cursor.close();
		}
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
		}
		buf.append("Number of rows: " + count);
		return buf.toString();
	}
}

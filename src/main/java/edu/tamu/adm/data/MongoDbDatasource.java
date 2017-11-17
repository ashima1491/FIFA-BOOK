package edu.tamu.adm.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;

public class MongoDbDatasource {

	private static MongoClient mongo;
	private static DB db;
	private static DBCollection collection;

	private MongoDbDatasource() {

	}

	public static MongoClient createMongoDbInstance() {
		System.out.println("***************Start of method getMongoDbInstance*******************");
		if (null == mongo)
			mongo = new MongoClient("localhost", 27017);
		System.out.println("***************End of method getMongoDbInstance*******************");

		return mongo;

	}

	public static DBCollection createDatabaseCollection(String databaseName, String collectionName) {
		System.out.println("***************Start of method createDatabaseCollection*******************");

		db = mongo.getDB(databaseName);
		collection = db.getCollection(collectionName);
		System.out.println("***************End of method init*******************");
		return collection;
	}

	public static void deleteDatabaseCollection() {
		System.out.println("***************Start of method deleteData*******************");

		WriteResult result = collection.remove(new BasicDBObject());
		System.out.println(result.toString());
		System.out.println("***************End of method deleteData*******************");

	}

	public static void insertData(File file) throws FileNotFoundException, IOException, ParseException {
		System.out.println("***************Start of method loadData*******************");

		// File file = new File("dataFiles/data.json");

		parseData(collection, file);

		DBCursor cursor = collection.find();
		/*
		 * while (cursor.hasNext()) { System.out.println(cursor.next()); }
		 */
		cursor.close();
		System.out.println("***************End of method loadData*******************");

	}

	private static void parseData(DBCollection collection, File file)
			throws FileNotFoundException, IOException, ParseException {
		System.out.println("***************Start of method parseData*******************");

		JSONParser parser = new JSONParser();
		Object obj = parser.parse(new FileReader(file));

		JSONArray jsonObject = (JSONArray) obj;
		BasicDBList data = (BasicDBList) JSON.parse(jsonObject.toString());
		for (int i = 0; i < data.size(); i++) {
			collection.insert((DBObject) data.get(i));
		}
		System.out.println("***************End of method parseData*******************");

	}

	public static void fetchAllData(DBCollection collection) {
		System.out.println("***************Start of method fetchData*******************");

		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Rating", new BasicDBObject("$gt", 90));
		DBCursor cursor = collection.find(gtQuery);
		/*
		 * while (cursor.hasNext()) { System.out.println(cursor.next()); }
		 */

		System.out.println("***************End of method fetchData*******************");

	}

	public static void disconnect() {
		System.out.println("***************Start of method closeResources*******************");

		mongo.close();
		System.out.println("***************End of method closeResources*******************");
	}

	public static void loadDistinctKeys(DBCollection fifa) {

		DBCursor cursor = fifa.find();

		for (String key : cursor.next().keySet()) {

			System.out.println(key);

		}

	}

	public static void fetchTeamAggregates(String teamName) {
		ArrayList<String> playerNames = null;
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Club", new BasicDBObject("$eq", teamName));
		DBCursor cursor = collection.find(gtQuery);
		cursor.sort(new BasicDBObject("Rating", -1));

		int sumDefence = 0;
		int sumMidField = 0;
		int sumAttack = 0;
		int sumGoalKeeping = 0;
		int counterDefence = 0;
		int counterMidField = 0;
		int counterAttack = 0;
		int counterGoalKeeping = 0;
		List<String> DefenceSkillsList = Arrays.asList("Ball_Control", "Dribbling", "Marking", "Sliding_Tackle",
				"Standing_Tackle", "Aggression", "Reactions", "Interceptions", "Vision", "Composure", "Short_Pass",
				"Long_Pass", "Acceleration", "Speed", "Stamina", "Strength", "Balance", "Agility", "Jumping", "Heading",
				"Shot_Power", "Long_Shots", "Volleys");
		List<String> MidFieldSkillsList = Arrays.asList("Ball_Control", "Dribbling", "Marking", "Sliding_Tackle",
				"Standing_Tackle", "Aggression", "Reactions", "Attacking_Position", "Interceptions", "Vision",
				"Composure", "Crossing", "Short_Pass", "Long_Pass", "Acceleration", "Speed", "Stamina", "Strength",
				"Balance", "Agility", "Jumping", "Heading", "Long_Shots", "Curve", "Freekick_Accuracy", "Penalties",
				"Volleys");
		List<String> AttackSkillsList = Arrays.asList("Ball_Control", "Dribbling", "Standing_Tackle", "Reactions",
				"Attacking_Position", "Interceptions", "Vision", "Composure", "Crossing", "Short_Pass", "Acceleration",
				"Speed", "Stamina", "Aggression", "Strength", "Balance", "Agility", "Jumping", "Heading", "Shot_Power",
				"Finishing", "Long_Shots", "Curve", "Freekick_Accuracy", "Penalties", "Volleys");
		List<String> GoalKeeperSkillsList = Arrays.asList("Reactions", "Composure", "Jumping", "Shot_Power",
				"Long_Shots", "GK_Reflexes", "GK_Positioning", "GK_Kicking", "GK_Handling", "GK_Diving");

		if (cursor.count() == 0) {
			System.out.println(" No results found !! ");
			return;
		}
		playerNames = new ArrayList<String>();
		while (cursor.hasNext()) {

			DBObject dbObj = cursor.next();
			String prefPosition = (String) dbObj.get("Preffered_Position");
			playerNames.add((String) dbObj.get("Name"));
			String lastLetter = prefPosition.substring(prefPosition.length() - 1);

			if ("B".equalsIgnoreCase(lastLetter)) {
				Integer defence = 0;
				for (String temp : DefenceSkillsList) {
					defence += (Integer) dbObj.get(temp);
				}
				sumDefence += defence;
				counterDefence++;
			}

			if ("M".equalsIgnoreCase(lastLetter)) {
				Integer midField = 0;
				for (String temp : MidFieldSkillsList) {
					midField += (Integer) dbObj.get(temp);
				}
				sumMidField += midField;
				counterMidField++;
			}

			if ("F".equalsIgnoreCase(lastLetter) || "S".equalsIgnoreCase(lastLetter) || "T".equalsIgnoreCase(lastLetter)
					|| "W".equalsIgnoreCase(lastLetter)) {
				Integer attack = 0;
				for (String temp : AttackSkillsList) {
					attack += (Integer) dbObj.get(temp);
				}
				sumAttack += attack;
				counterAttack++;
				Integer I = AttackSkillsList.size();
			}

			if ("K".equalsIgnoreCase(lastLetter)) {
				Integer goalKeeping = 0;
				for (String temp : GoalKeeperSkillsList) {
					goalKeeping += (Integer) dbObj.get(temp);
				}
				sumGoalKeeping += goalKeeping;
				counterGoalKeeping++;
			}
		}

		float avgDefence = sumDefence / (counterDefence * DefenceSkillsList.size());
		float avgMidField = sumMidField / (counterMidField * MidFieldSkillsList.size());
		float avgAttack = sumAttack / (counterAttack * AttackSkillsList.size());
		float avgGoalKeeping = sumGoalKeeping / (counterGoalKeeping * GoalKeeperSkillsList.size());

		System.out.println();
		System.out.printf("Average Defence of Team  = %20.4s\n", avgDefence);
		System.out.printf("Average Mid Field of Team = %20.4s\n", avgMidField);
		System.out.printf("Average Attack of Team = %20.4s\n", avgAttack);
		System.out.printf("Average Goal Keeping of Team = %20.4s\n", avgGoalKeeping);
		System.out.println();

		String map = "function() {emit(this.Nationality, 1);}";
		String reduce = "function(key,values) {return Array.sum(values)}";

		MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, null, MapReduceCommand.OutputType.INLINE,
				gtQuery);

		MapReduceOutput out = collection.mapReduce(cmd);
		System.out.println("Nationalities represented");
		for (DBObject o : out.results()) {
			System.out.println(o.get("_id") + " :: " + o.get("value"));
		}

		
		// Top five players

		gtQuery.put("Club", new BasicDBObject("$eq", teamName));
		cursor = collection.find(gtQuery);
		cursor.sort(new BasicDBObject("Rating", -1));
		cursor.limit(5);

		System.out.println("*************Top Five Players***************");
		System.out.println();

		System.out.println("*********Player***Rating********");
		System.out.println();

		while (cursor.hasNext()) {
			DBObject dbObj = cursor.next();
			String name = (String) dbObj.get("Name");
			Integer rating = (Integer) dbObj.get("Rating");
			System.out.println(name.trim() + "  " + rating.toString().trim());
		}

		System.out.println();
		System.out.println();

		cursor.close();
		displayPlayerList(teamName, playerNames);
	}
	/*
	 * @input: club name and player list
	 * method to display all players of a particular team
	 */

	private static void displayPlayerList(String club, ArrayList<String> playerNames) {

		System.out.print("***********All players in this team ::");
		for (String player : playerNames) {
			System.out.println(player);
		}

		System.out.print("*********Enter your player name from the list above ::");
		System.out.println();

		Scanner scanner = new Scanner(System.in);

		String player = scanner.nextLine();
		fetchPlayerData(club, player);
		scanner.close();

	}

	/*
	 * @input: team name and player name method to fetch players for a team
	 * method to fetch player statistics
	 */
	private static void fetchPlayerData(String team, String player) {
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Name", new BasicDBObject("$eq", player));
		gtQuery.put("Club", new BasicDBObject("$eq", team));
		DBCursor cursor = collection.find(gtQuery);

		if (cursor.count() == 0) {
			System.out.println(" No results found !! ");
			return;
		}

		while (cursor.hasNext()) {
			DBObject dbObj = cursor.next();
			Integer Stamina = (Integer) dbObj.get("Stamina");
			Integer Strength = (Integer) dbObj.get("Strength");
			Integer Acceleration = (Integer) dbObj.get("Acceleration");
			Integer Speed = (Integer) dbObj.get("Speed");

			System.out.println("*******Player Statistics*********");
			System.out.println();
			System.out.println("Stamina::" + Stamina);
			System.out.println("Strength::" + Strength);
			System.out.println("Acceleration::" + Acceleration);
			System.out.println("Speed::" + Speed);
		}
	}

	/*
	 * @input: field name whose values are to be fetched from the database method to
	 * fetch distinct club names from the database
	 */
	public static List<String> loadFieldValues(String fieldName) {

		List<String> list = collection.distinct("Club");
		Collections.sort(list);
		return list;

	}

}

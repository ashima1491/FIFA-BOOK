package edu.tamu.adm;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import edu.tamu.adm.data.MongoDbDatasource;

public class Main {

	public static void main(String[] args) throws Exception {
		try {

			try {
				MongoDbDatasource.createMongoDbInstance();
				MongoDbDatasource.createDatabaseCollection("FIFA", "FIFA_Collection");
				MongoDbDatasource.deleteDatabaseCollection();
				File file = new File("dataFiles/data.json");
				MongoDbDatasource.insertData(file);
				// MongoDbDatasource.fetchAllData(collection);
				List<String> clubList = MongoDbDatasource.loadFieldValues("Club");
				String club = getInputFromUser(clubList);
				MongoDbDatasource.fetchTeamAggregates(club);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				MongoDbDatasource.disconnect();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

		/*
		 * @input: list of clubs from the database
		 * method displays list of clubs
		 * user enters a club name from the list
		 * @return : club name
		 */
	private static String getInputFromUser(List<String> clubList) {
		Scanner scanner = new Scanner(System.in);
		for (String club : clubList) {
			System.out.println(club);
		}
		System.out.println(" *******Enter your club from above list ::");
		String club = scanner.nextLine();
		return club;
		
	}

}

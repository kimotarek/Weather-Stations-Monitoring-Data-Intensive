package com.example.datastore;
import java.util.ArrayList;
import java.util.List;

public class DatastoreApplication {
	public static void main(String[] args) {
		Run();
	}

	public static void Run(){
		 ParquetController parquetController = new ParquetController();
		 List<StationMessage> messages = new ArrayList<>();

         messages.add(new StationMessage(1, 5, "Low", System.currentTimeMillis(), new Weather(50, 25, 10)));
         messages.add(new StationMessage(1, 7, "High", System.currentTimeMillis(), new Weather(60, 30, 15)));
         messages.add(new StationMessage(1, 10, "Medium", System.currentTimeMillis(), new Weather(55, 28, 12)));

		 for(StationMessage m: messages){
		 	parquetController.AddToParquet(m);
		 }
//		 parquetController.ReadParquet();
	}

	
}

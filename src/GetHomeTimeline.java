
import java.io.*;
import java.text.SimpleDateFormat;

import twitter4j.*;
import twitter4j.Query.ResultType;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;



public class GetHomeTimeline {
	static Twitter twitter;
	 File file;
	public static void main(String[] args) {
		OutputStreamWriter osw= null;
		String output=args[0];//"F:/CS242data.txt";//change output folder!
		int queryNum = Integer.parseInt(args[1]);
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("wOXL0s2TYmYss6x9BuEKfPV2d")
		  .setOAuthConsumerSecret("0owQg0PKVfgbzCemKCXd7YNEiDCSi4Jwjb8CqQ7USWwuJoMrH4")
		  .setOAuthAccessToken("4606527374-6I9wbSvqpdUvYqnyA12WtMBsGisq6Ypn5uUiVIo")
		  .setOAuthAccessTokenSecret("mC0Vknww3JRt3Kid9K5WnAUDi7njERwR3LOEuzDaEAwNG");
		TwitterFactory tf = new TwitterFactory(cb.build());
		 twitter = tf.getInstance();
		 
		 try {
			 long minId = Long.MAX_VALUE;
			 osw = new OutputStreamWriter(new FileOutputStream(output,false));
			 //recusively do the query!, the maxId of next query is the LAST ID of Last Result
			 

			  
			  //System.out.println(minId);
			  for(int i=1;i<=queryNum;i++){//Search is rate limited at 180 queries per 15 minute window.
				  Query query = new Query(args[2]);
						   // query.getSince();
					//	query.setUntil("2015-12-1");
				  
				  query.count(100);//sets the number of tweets to return per page, up to a max of 100
				  query.setLang("en");
				  query.setMaxId(minId-1);
				  QueryResult result = twitter.search(query);
				  for (Status status : result.getTweets())
				  { 
					  
					  if(minId>status.getId()){
							 minId = status.getId();
					  }

					  osw.write(status.getText().replaceAll("\n|\r",""));
					  osw.write("\n"); 
				  } 
				  System.out.println(minId);
				  if(i%179==0){
					  System.out.println("Search is rate limited at 180 queries per 15 minute window.i="+i+",Query Num:"+queryNum);
					  Thread.sleep(1000000);
				  }
			  }
			  osw.close();
		 	}catch (TwitterException te) 
	        {
	            te.printStackTrace();
	            System.out.println("Failed to get timeline: " + te.getMessage());
	            try {
					osw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            System.exit(-1);
	        }catch (IOException e) {
				// TODO: handle exception
	        	e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}//eof setup
}

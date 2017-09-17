package org.gilbre.process;


import java.util.HashMap;
import org.apache.activemq.transport.stomp.StompConnection; 
import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe; 
import org.apache.activemq.transport.stomp.StompFrame;

public class MQClient { 
	private static final String PERSISTENT = "persistent";
 	private static String URLhost = "localhost";
	private static int port = 61613;
	private static String queuePath = "/queue/test";
	
	public static String sendMessage(String messageText) { 
	    try {     
	      StompConnection connection = new StompConnection(); 
	      HashMap<String, String> header = new HashMap<String, String>();
	      header.put(PERSISTENT, "true");
	      connection.open(URLhost, port); 
	      connection.connect("", ""); 
	      connection.begin("MQClient");
	      connection.send(queuePath, messageText, "MQClient", header); 
	      connection.commit("MQClient");				
	      connection.disconnect(); 

	    } catch (Exception e) { 
	    	 e.printStackTrace(); 
		      return "ACTIVEMQ SERVICE ERROR";
	    }
		return ""; 
	  } 

	  public static String receiveMessage() {
	      try {
		  StompConnection connection = new StompConnection(); 
	      connection.open(URLhost, port); 
	      connection.connect("", ""); 
	      connection.subscribe(queuePath, Subscribe.AckModeValues.CLIENT); 
	      connection.begin("MQClient");
	      Thread.sleep(1000);//below not a good NO DATA test .. worked by making thread sleep a while
	      if (connection.getStompSocket().getInputStream().available() > 1) 
	      {
	    	  StompFrame message = connection.receive(); 
	    	  connection.ack(message, "MQClient");
		      connection.commit("MQClient");
		      connection.disconnect();
		      return message.getBody(); 
	      }
	      else
	    	  return "No queued message";
	      
	      } catch (Exception e) { 
		      e.printStackTrace(); 
		      return "NO SERVICE";
	      }

	  }
	  
	  public static String receiveMessage(String URL, int no, String queue) {
		  URLhost = URL;
		  port = no;
		  queuePath = queue;
		  return receiveMessage();
	  }
	  
	  public static void setURLpath(String url) {
		  URLhost = url;
	  }
	  public static void setQueuePath(String path){
		  queuePath = path;
	  }
	  public static void setPort(int no){
		  port = no;
	  }

	public static void setParams(String host, int no, String path) {
		setURLpath(host);
		setPort(no);
		setQueuePath(path);
 	}
	  
} 




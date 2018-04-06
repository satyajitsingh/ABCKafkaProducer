package com.ABC;

import com.ABC.Contact;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Scanner;
import java.util.Locale;
import java.util.List;
import java.text.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import kafka.utils.ZkUtils;
import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;

public class Producer {
	
	private static Scanner in;
	private static DateTimeFormatter dstf = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss",Locale.UK);
	private static final String ZOOKEEPER_CONNECT = "10.96.10.2:2181";
	//final static Logger logger = Logger.getLogger(Producer.class.getName());
	private final static Logger slf4jLogger = LoggerFactory.getLogger(Producer.class);
	
	@SuppressWarnings("finally")
	public static List<Broker> getAllBrokers() {
		  try {
	      final ZkConnection zkConnection = new ZkConnection(ZOOKEEPER_CONNECT);
		  final int sessionTimeoutMs = 10 * 1000;
		  final int connectionTimeoutMs = 10 * 1000;
		  List<Broker> brokers = null;
		  final ZkClient zkClient = new ZkClient(ZOOKEEPER_CONNECT,
		                                  sessionTimeoutMs,
		                                  connectionTimeoutMs,
		                                  ZKStringSerializer$.MODULE$);
	      final ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);

	      brokers =  scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
	      return brokers;
		  }catch(Exception ex) {
			  //ex.printStackTrace();
		  }
		  finally {
			  return null;
		  }	  
		}
	
	public static void main(String[] args) throws ParseException {
		
		if (args.length != 2) {
            System.err.println("Please specify 3 parameters: Topic, No. of Records to create and Kafka broker information");
            System.exit(-1);
        }      
        in = new Scanner(System.in);
        String topicName = args[0];
        int nRec = Integer.parseInt(args[1]);
        String kafka_connect= args[2];
        
        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafka_connect);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
        
        System.out.println("Trying to Get Connection To Kafka");
        List<Broker> brokers = null;
        try{
        	brokers = getAllBrokers();
        }catch(Exception ex) {
			 ex.printStackTrace();
		  }
        if (brokers == null || brokers.isEmpty()) {
        	System.out.println("Not able to connect to Kafka");
        } else {
        	for(Broker brk:brokers){
        		System.out.println(brk.toString());
        	}
        	System.out.println("Connected.. Creating Records..");
        
			org.apache.kafka.clients.producer.Producer<String, JsonNode> producer = new KafkaProducer <String, JsonNode>(configProperties);
		    ObjectMapper objectMapper = new ObjectMapper();
		    LocalDateTime initDate = LocalDateTime.now();
		    int ctr = 0;
		    for(int i = 1; i<=nRec; i++) {
		    	
		    	String toPort0 = "Aleppo Syria";
		    	String toPort1 = "Moscow Russia";
		    	String toPort2 = "Beijing China";
		    	String toPort3 = "Colombo SriLanka";
		    	String toPort4 = "NewDelhi India";
		    	String fromPort1 = "Heathrow London";
		    	String fromPort2 = "Luton London";
		    	String fromPort3 = "Birmingham";
		    	String fromPort4 = "Bristol";
		    	
		    	Contact contact = new Contact();
		    	if (i % 2 == 0 ) {
		    		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort1, fromPort1);
		    	}
		    	else if(i % 3 == 0) {
		    		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort2, fromPort2);
		    	}
		    	else if(i % 4 == 0) {
		    		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort3, fromPort3);
		    	}
		    	else if(i % 5 == 0) {
		    		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort4, fromPort4);
		    		initDate = initDate.plusDays(5);
		    	}
		    	else {
		    		contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort0, fromPort1);
		    	}
		    	try {
		            JsonNode  jsonNode = objectMapper.valueToTree(contact);
		            
		            ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName,jsonNode);
		            producer.send(rec);
		            
		            }catch (Exception e) {
		    			e.printStackTrace();
		            }	
		    	ctr = i;
		    }
		    System.out.println(Integer.toString(ctr) + " Records Created");
		    producer.close();
        }
	}

}

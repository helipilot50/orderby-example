package com.aerospike.examples.orderby;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;


/**
@author Peter Milne
*/
public class SimpleOrderby {
	private static final String SHOE_COLOUR_BIN = "shoeColour";
	private static final String SHOE_SIZE_BIN = "shoeSize";
	private static final int MAX_RECORDS = 10000;
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	private WritePolicy writePolicy;
	private Policy policy;
	
	private String[] colours = new String[] {"blue", "red", "yellow", "green", "brown", "white", "black", "purple"};

	private static Logger log = Logger.getLogger(SimpleOrderby.class);
	public SimpleOrderby(String host, int port, String namespace, String set) throws AerospikeException {
		this.client = new AerospikeClient(host, port);
		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public SimpleOrderby(AerospikeClient client, String namespace, String set) throws AerospikeException {
		this.client = client;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public static void main(String[] args) throws AerospikeException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("l", "load", false, "Load Data.");
			options.addOption("q", "query", false, "Query Data.");
			options.addOption("o", "order", false, "Query with Orderby Data.");
			options.addOption("a", "all", false, "Scan ALL with Orderby Data.");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);


			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			log.debug("Host: " + host);
			log.debug("Port: " + port);
			log.debug("Namespace: " + namespace);
			log.debug("Set: " + set);

			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cmds.size() == 0 && cl.hasOption("u")) {
				logUsage(options);
				return;
			}

			SimpleOrderby as = new SimpleOrderby(host, port, namespace, set);
			/*
			 * create index
			 */
			IndexTask it = as.client.createIndex(null, as.namespace, as.set, "shoe_size", SHOE_SIZE_BIN, IndexType.NUMERIC);
			it.waitTillComplete();
			/*
			 * register UDF
			 */
			
			RegisterTask rt = as.client.register(null, "udf/qualifiers.lua", "qualifiers.lua", Language.LUA);
			rt.waitTillComplete();
			/*
			 * process options
			 */
			if (cl.hasOption("l"))
				as.load();
			else if (cl.hasOption("q"))
				as.query();
			else if (cl.hasOption("o"))
				as.order();
			else if (cl.hasOption("a"))
				as.orderWithScan();
			else
				logUsage(options);
			
		} catch (Exception e) {
			log.error("Critical error", e);
		}
	}
	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = SimpleOrderby.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}

	public void order()  {
		/*
		 * Prepare a statement
		 */
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.set);
		stmt.setBinNames(SHOE_SIZE_BIN, SHOE_COLOUR_BIN);
		stmt.setFilters(Filter.range(SHOE_SIZE_BIN, 8, 16));
		/*
		 * exxecute query
		 */
		ResultSet rs = this.client.queryAggregate(null, stmt, "qualifiers", "orderby");
		/*
		 * process results as they arrive from the cluster
		 */
		while (rs.next()){
			Map<String, List<Map<String,Object>>> result = (Map<String,List<Map<String,Object>>>) rs.getObject();
			for (String mapKey : result.keySet()) {
				List<Map<String, Object>> group = result.get(mapKey);
				System.out.println(mapKey + " size:" + group.size());
				for (Map<String, Object> element : group){
					System.out.println("\t" + element);
				}
			}
		}
	}

	public void orderWithScan()  {
		/*
		 * Prepare a statement
		 */
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.set);
		stmt.setBinNames(SHOE_SIZE_BIN, SHOE_COLOUR_BIN);
		/*
		 * exxecute query
		 */
		ResultSet rs = this.client.queryAggregate(null, stmt, "qualifiers", "orderby");
		/*
		 * process results as they arrive from the cluster
		 */
		while (rs.next()){
			Map<String, List<Map<String,Object>>> result = (Map<String,List<Map<String,Object>>>) rs.getObject();
			for (String mapKey : result.keySet()) {
				List<Map<String, Object>> group = result.get(mapKey);
				System.out.println(mapKey + " size:" + group.size());
				for (Map<String, Object> element : group){
					System.out.println("\t" + element);
				}
			}
		}
	}

	public void query()  {
		/*
		 * Prepare a statement
		 */
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.set);
		stmt.setBinNames(SHOE_SIZE_BIN, SHOE_COLOUR_BIN);
		stmt.setFilters(Filter.range(SHOE_SIZE_BIN, 1, 16));
		/*
		 * exxecute query
		 */
		RecordSet rs = this.client.query(null, stmt);
		/*
		 * process results as they arrive from the cluster
		 */
		while (rs.next()){
			System.out.println(rs.getRecord());
		}
	}
	
	public void load(){
		int colourBound = this.colours.length - 1;
		Random rand = new Random();
		Random rand2 = new Random();
		for (int i = 0; i < MAX_RECORDS; i++){
			String shoeColour = this.colours[rand.nextInt(colourBound)];
			int shoeSize = rand2.nextInt(16);
			Key key = new Key(this.namespace, this.set, "user-"+i);
			Bin shoeSizeBin = new Bin(SHOE_SIZE_BIN, shoeSize);
			Bin shoeColourBin = new Bin(SHOE_COLOUR_BIN, shoeColour);
			this.client.put(null, key, shoeSizeBin, shoeColourBin);
			System.out.println("Added: " + key);
		}
	}

}
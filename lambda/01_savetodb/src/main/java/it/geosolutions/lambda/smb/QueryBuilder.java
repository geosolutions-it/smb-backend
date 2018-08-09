package it.geosolutions.lambda.smb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.amazonaws.services.lambda.runtime.Context;

public class QueryBuilder {

	public Set<String> queries;

	public Map<String, String> currentQuery;
	public List<String> headers;

	protected Context context;
	protected String username;
	protected String sessionId;
	
	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
	
	public QueryBuilder(Context context) {
		super();
		this.context = context;
		this.queries = new HashSet<String>();
		this.currentQuery = new HashMap<String, String>();
		
	}

	static public String cropLine(String line) {
		if (line != null) {
			if (line.lastIndexOf(",") == line.length() - 1) {
				return line.substring(0, line.length() - 1);
			}
			return line;
		}
		return "";
	}
	public StringBuilder sb = new StringBuilder();
			
	public QueryBuilder parseLine(String line) {

		if (line == null || line.isEmpty()) {
			context.getLogger().log("Got empty line, skipping it.");
			return this;
		}

		if (line.contains("sessionId")) {
			headers = Arrays.asList(cropLine(line).split(","));
			// context.getLogger().log("Header: " + headers);
		} else {

			String[] values = cropLine(line).split(",");
			
			// Number of values must match number of headers
			if (values.length != headers.size()) {
				context.getLogger().log("Line <-> Header");
				return this;
			}
			
			// Set the sessionId (will be used to set the created_at field in the Track object)
			sessionId = values[headers.indexOf("sessionId")];
			
			//Clear the buffer
			sb.setLength(0);
			
			sb.append("INSERT INTO ").append(DatabaseConfig.getTableName("datapoints")).append(" (");
			for (int i = 0; i < headers.size(); i++) {
				
				// context.getLogger().log(headers[i] + " : "+ values[i]);
				currentQuery.put(headers.get(i), values[i]);
				if( !headers.get(i).equalsIgnoreCase("latitude") 
						&& !headers.get(i).equalsIgnoreCase("longitude")
						&& !headers.get(i).equalsIgnoreCase("vehicleMode")
						&& !headers.get(i).equalsIgnoreCase("serialVersionUID")
						) {
					sb.append(headers.get(i)).append(",");
				}
				
				if( headers.get(i).equalsIgnoreCase("vehicleMode")) {
					sb.append("vehicle_type").append(",");
				}

			}
			/*
			if(username != null && !username.isEmpty()) {
				sb.append("username,");
			}
			*/
			sb.append("icon_color,the_geom, track_id) VALUES (");
			
			for (int i = 0; i < headers.size(); i++) {
				
				if(!headers.get(i).equalsIgnoreCase("latitude") && !headers.get(i).equalsIgnoreCase("longitude") && !headers.get(i).equalsIgnoreCase("serialVersionUID")) {
					sb.append(currentQuery.get(headers.get(i))).append(",");
				}
			}
			/*
			if(username != null && !username.isEmpty()) {
				sb.append("'").append(username).append("',");
			}
			*/
			long color = 128;
			try {
				long sessionId = Long.parseLong(currentQuery.get("sessionId"));
				color = sessionId % 255;
			}catch (NumberFormatException nfe) {
				color = 128;
			}
			
			sb.append(color).append(",st_setsrid(st_point(").append(currentQuery.get("longitude")).append(",").append(currentQuery.get("latitude")).append("), 4326)").append(",").append("?").append(");");
			
			queries.add(sb.toString());
			context.getLogger().log(sb.toString());
		}
		return this;
	}
}

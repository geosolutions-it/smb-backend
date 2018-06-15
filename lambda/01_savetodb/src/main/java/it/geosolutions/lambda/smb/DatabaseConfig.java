package it.geosolutions.lambda.smb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to configure the database names 
 * @author gnafu
 *
 */
public class DatabaseConfig {
	
	public static final Map<String, String> TABLE_NAMES;
    static {
        Map<String, String> initializer = new HashMap<>();
        initializer.put("datapoints"                    , "tracks_collectedpoint"         );
        initializer.put("users"                         , "profiles_smbuser"              );
        initializer.put("vehicles"                      , "vehicles_bike"                 );
        initializer.put("tags"                          , "vehicles_physicaltag"          );
        initializer.put("vehiclemonitor_bikeobservation", "vehiclemonitor_bikeobservation");
        initializer.put("users_mapping"                 , "bossoidc_keycloak"             );
        TABLE_NAMES = Collections.unmodifiableMap(initializer);
    }
    
    public static String getTableName(String key) {
    	if(key == null) {
    		return null;
    	}
    	
    	return TABLE_NAMES.getOrDefault(key, key);
    }
}

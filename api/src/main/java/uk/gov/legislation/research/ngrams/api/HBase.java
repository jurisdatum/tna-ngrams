package uk.gov.legislation.research.ngrams.api;

import org.apache.hadoop.conf.Configuration;

class HBase {

    private static final String prop = "hbase.zookeeper.quorum";
    private static Configuration singleton = new Configuration();

    static {
        String zookeeperQuorum = System.getenv(prop);
        if (zookeeperQuorum == null)
            zookeeperQuorum = System.getProperty(prop);
        if (zookeeperQuorum != null)
            singleton.set("hbase.zookeeper.quorum", zookeeperQuorum);
    }

    static Configuration config() { return singleton; };

}

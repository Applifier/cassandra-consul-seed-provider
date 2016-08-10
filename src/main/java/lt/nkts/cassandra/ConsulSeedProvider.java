package lt.nkts.cassandra;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.cassandra.locator.SeedProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;

public class ConsulSeedProvider implements SeedProvider {
    private static final Logger logger = LoggerFactory.getLogger(ConsulSeedProvider.class);

    private ConsulClient client;
    private URL consulUrl;
    private boolean useKeyValue;
    private String consulKeyValuePrefix;
    private String consulServiceName;
    private List<String> consulServiceTags;
    private List<InetAddress> defaultSeeds;

    public ConsulSeedProvider(Map<String, String> params) {
        defaultSeeds = getDefaultSeeds(params);

        try {
            consulUrl = new URL(System.getProperty("consul.url", "http://localhost:8500/"));
        } catch (MalformedURLException e) {
            logger.error("Could not parse consul.url", e);
        }
        useKeyValue = Boolean.parseBoolean(System.getProperty("consul.kv.enabled", "false"));
        consulKeyValuePrefix = System.getProperty("consul.kv.prefix", "cassandra/seeds");
        consulServiceName = System.getProperty("consul.service.name", "cassandra");
        consulServiceTags = split(System.getProperty("consul.service.tags", ""), ",");

        logger.debug("consulUrl {}", consulUrl);
        logger.debug("consulServiceName {}", consulServiceName);
        logger.debug("consulServiceTags {}", consulServiceTags.toString());
        logger.debug("consulServiceTags size [{}]", consulServiceTags.size());
        logger.debug("useKeyValue {}", useKeyValue);
        logger.debug("consulKeyValuePrefix {}", consulKeyValuePrefix);
        logger.debug("defaultSeeds {}", defaultSeeds);

        client = new ConsulClient(String.format("%s:%s", consulUrl.getHost(), consulUrl.getPort()));
    }

    @Override
    public List<InetAddress> getSeeds() {
        ArrayList<InetAddress> seeds = useKeyValue ? getKeyValueSeeds() : getServiceSeeds();
        if (seeds.isEmpty()) {
            seeds.addAll(defaultSeeds);
        }
        logger.info("Seeds {}", seeds.toString());
        return Collections.unmodifiableList(seeds);
    }

    private ArrayList<InetAddress> getKeyValueSeeds() {
        ArrayList<InetAddress> seeds = new ArrayList<>();
        Response<List<GetValue>> response = client.getKVValues(consulKeyValuePrefix);
        if (response.getValue() == null) {
            return seeds;
        }

        for (GetValue record : response.getValue()) {
            logger.debug("kv: {}", record);

            String[] parts = record.getKey().split("/");
            String host = parts[parts.length - 1];

            InetAddress address = toInetAddress(host);
            if (address != null) {
                seeds.add(address);
            }
        }
        return seeds;
    }

    private ArrayList<InetAddress> getServiceSeeds() {
        ArrayList<InetAddress> seeds = new ArrayList<>();
        Response<List<CatalogService>> response = client.getCatalogService(consulServiceName, null);
        if (response.getValue() == null) {
            return seeds;
        }

        for (CatalogService service : response.getValue()) {
            logger.debug("Service [{}]", service.toString());

            if (!consulServiceTags.isEmpty()) {
                List<String> serviceTags = service.getServiceTags();

                logger.debug("Service tagged with {}", serviceTags.toString());
                logger.debug("I'm looking for {}", consulServiceTags.toString());

                if (consulServiceTags.containsAll(serviceTags)) {
                    InetAddress address = toInetAddress(service.getServiceAddress());
                    if (address != null) {
                        seeds.add(address);
                    }
                }
            } else {
                InetAddress address = toInetAddress(service.getServiceAddress());
                if (address != null) {
                    seeds.add(address);
                }
            }

        }
        return seeds;
    }

    private static ArrayList<InetAddress> getDefaultSeeds(Map<String, String> params) {
        ArrayList<InetAddress> seeds = new ArrayList<>();
        for (String host : split(params.get("seeds"), ",")) {
            InetAddress address = toInetAddress(host);
            if (address != null) {
                seeds.add(address);
            }
        }
        return seeds;
    }

    private static InetAddress toInetAddress(String serviceAddress) {
        try {
            return InetAddress.getByName(serviceAddress);
        } catch (UnknownHostException e) {
            logger.warn("Error while adding seed " + serviceAddress, e);
            return null;
        }
    }

    private static ArrayList<String> split(String str, String regex) {
        ArrayList<String> partsList = new ArrayList<>();
        if (str == null) {
            return partsList;
        }
        String[] parts = str.split(regex);
        for (String s : parts) {
            String trimmed = s.trim();
            if (!trimmed.isEmpty()) {
                partsList.add(trimmed);
            }
        }
        return partsList;
    }
}

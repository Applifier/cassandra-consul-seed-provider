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

    private URL consulUrl;
    private Boolean consulUseKv;
    private String consulKvPrefix;
    private String consulServiceName;
    private Collection<String> consulServiceTags;
    private List<InetAddress> defaultSeeds;

    public ConsulSeedProvider(Map<String, String> args) {
        // These are used as a fallback if we get nothing from Consul
        defaultSeeds = new ArrayList<>();
        for (String host : split(args.get("seeds"), ",")) {
            try {
                defaultSeeds.add(InetAddress.getByName(host));
            } catch (UnknownHostException ex) {
                logger.warn("Seed provider couldn't lookup host " + host);
            }
        }

        try {
            consulUrl = new URL(System.getProperty("consul.url", "http://localhost:8500/"));
        } catch (MalformedURLException e) {
            logger.error("Could not parse consul.url", e);
        }
        consulUseKv = Boolean.parseBoolean(System.getProperty("consul.kv.enabled", "false"));
        consulKvPrefix = System.getProperty("consul.kv.prefix", "cassandra/seeds");
        consulServiceName = System.getProperty("consul.service.name", "cassandra");
        consulServiceTags = split(System.getProperty("consul.service.tags", ""), ",");

        logger.debug("consulUrl {}", consulUrl);
        logger.debug("consulServiceName {}", consulServiceName);
        logger.debug("consulServiceTags {}", consulServiceTags.toString());
        logger.debug("consulServiceTags size [{}]", consulServiceTags.size());
        logger.debug("consulUseKv {}", consulUseKv);
        logger.debug("consulKvPrefix {}", consulKvPrefix);
        logger.debug("defaultSeeds {}", defaultSeeds);
    }

    public List<InetAddress> getSeeds() {
        ConsulClient client = new ConsulClient(String.format("%s:%s", consulUrl.getHost(), consulUrl.getPort()));

        List<InetAddress> seeds = new ArrayList<>();

        if (consulUseKv) {
            Response<List<GetValue>> response = client.getKVValues(consulKvPrefix);
            List<GetValue> all = response.getValue();
            if (all == null) {
                return Collections.unmodifiableList(defaultSeeds);
            }

            for (Object gv : all) {
                logger.info("kv: {}", gv);

                GetValue record = (GetValue) gv;
                String[] parts = record.getKey().split("/");
                String host = parts[parts.length - 1];

                try {
                    seeds.add(InetAddress.getByName(host));
                } catch (UnknownHostException ex) {
                    logger.warn("Seed provider couldn't lookup host {}", host);
                }
            }

        } else {
            Response<List<CatalogService>> response = client.getCatalogService(consulServiceName, null);

            for (CatalogService svc : response.getValue()) {
                try {
                    logger.debug("Service [{}]", svc.toString());

                    if (!consulServiceTags.isEmpty()) {
                        List<String> stags = svc.getServiceTags();

                        logger.debug("Service tagged with {}", stags.toString());
                        logger.debug("I'm looking for {}", consulServiceTags.toString());

                        if (consulServiceTags.containsAll(stags)) {
                            seeds.add(InetAddress.getByName(svc.getServiceAddress()));
                        }
                    } else {
                        seeds.add(InetAddress.getByName(svc.getServiceAddress()));
                    }

                } catch (Exception e) {
                    logger.warn("Error while adding seed " + svc, e);
                }
            }
        }
        if (seeds.isEmpty()) {
            seeds = defaultSeeds;
        }
        logger.info("Seeds {}", seeds.toString());
        return Collections.unmodifiableList(seeds);
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

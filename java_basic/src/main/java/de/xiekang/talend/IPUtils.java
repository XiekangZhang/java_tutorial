package de.xiekang.talend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class IPUtils {
    static final Logger LOGGER = LogManager.getLogger(IPUtils.class.getName());

    public static List<String> subnetCalculator(String ipString) {
        List<String> subnets = new ArrayList<>();


        return subnets;
    }

    public static void main(String... args) {
        try {
            InetAddress localHost = Inet4Address.getLocalHost();
            NetworkInterface networkInterface = NetworkInterface.getByInetAddress(localHost);
            List<InterfaceAddress> addressList = networkInterface.getInterfaceAddresses();

            for (InterfaceAddress address: addressList) {
                System.out.println("IP Address: " + address.getAddress());
                System.out.println("Subnet Prefix Length: " + address.getNetworkPrefixLength());

                int prefixLength = address.getNetworkPrefixLength();
                int mask = -1 << (32 - prefixLength);
                String subnetMask = String.format(
                        "%d.%d.%d.%d",
                        (mask >>> 24) & 0xff,
                        (mask >>> 16) & 0xff,
                        (mask >>> 8) & 0xff,
                        mask & 0xff
                );
                System.out.println("Subnet Mask: " + subnetMask);
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}

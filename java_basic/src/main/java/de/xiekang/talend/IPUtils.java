package de.xiekang.talend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.*;


public class IPUtils {
    static int nMaskBits;
    static final Logger LOGGER = LogManager.getLogger(IPUtils.class.getName());
    static InetAddress ipRange;

    public static boolean IPAddressMatcher(String localIP, String IPRangeWithCIDR) {
        if (IPRangeWithCIDR.indexOf("/") > 0) {
            String[] addressAndMask = IPRangeWithCIDR.split("/");
            IPRangeWithCIDR = addressAndMask[0];
            nMaskBits = Integer.parseInt(addressAndMask[1]);
        } else {
            nMaskBits = -1;
        }
        ipRange = parseAddress(IPRangeWithCIDR);
        InetAddress localIPAddress = parseAddress(localIP);
        LOGGER.info("Local IP Addresse: {}", localIPAddress);
        if (!ipRange.getClass().equals(localIPAddress.getClass())) {
            return false;
        }
        if (nMaskBits < 0) {
            return ipRange.equals(localIPAddress);
        }
        byte[] localIPAddr = localIPAddress.getAddress();
        byte[] ipRangeAddr = ipRange.getAddress();
        int nMaskFullBytes = nMaskBits / 8;
        byte finalByte = (byte) (0xFF00 >> (nMaskBits & 0x07));
        for (int i = 0; i < nMaskFullBytes; i++) {
            if (localIPAddr[i] != ipRangeAddr[i]) {
                return false;
            }
        }
        if (finalByte != 0) {
            return (localIPAddr[nMaskFullBytes] & finalByte) == (ipRangeAddr[nMaskFullBytes] & finalByte);
        }
        return true;
    }

    private static InetAddress parseAddress(String address) {
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(String.format("The given IP {%s} can not be parsed.", address));
        }
    }
}

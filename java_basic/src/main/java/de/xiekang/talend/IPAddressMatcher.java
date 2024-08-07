package de.xiekang.talend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPAddressMatcher {
    private int nMaskBits;
    private InetAddress requiredAddress;
    static final Logger LOGGER = LogManager.getLogger(IPAddressMatcher.class.getName());

    public IPAddressMatcher(String ipAddress) {
        if (ipAddress.indexOf("/") > 0) {
            String[] addessAndMask = ipAddress.split("/");
            ipAddress = addessAndMask[0];
            nMaskBits = Integer.parseInt(addessAndMask[1]);
        } else {
            nMaskBits = -1;
        }
        requiredAddress = parseAddress(ipAddress);
    }

    public boolean matches(String address) {
        InetAddress remoteAddress = parseAddress(address);
        LOGGER.info("Local IP Address: {}", remoteAddress);
        if (!requiredAddress.getClass().equals(remoteAddress.getClass())) {
            return false;
        }
        if (nMaskBits < 0) {
            return remoteAddress.equals(requiredAddress);
        }

        byte[] remAddr = remoteAddress.getAddress();
        byte[] reqAddr = requiredAddress.getAddress();
        int nMaskFullBytes = nMaskBits / 8;
        byte finalByte = (byte) (0xFF00 >> (nMaskBits & 0x07));

        for (int i = 0; i < nMaskFullBytes; i++) {
            if (remAddr[i] != reqAddr[i]) {
                return false;
            }
        }

        if (finalByte != 0) {
            return (remAddr[nMaskFullBytes] & finalByte) == (reqAddr[nMaskFullBytes] & finalByte);
        }

        return true;
    }

    private InetAddress parseAddress(String address) {
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException("Can not parser given address!");
        }
    }

    public void setnMaskBits(int nMaskBits) {
        this.nMaskBits = nMaskBits;
    }

    public void setRequiredAddress(InetAddress requiredAddress) {
        this.requiredAddress = requiredAddress;
    }

    public int getnMaskBits() {
        return nMaskBits;
    }

    public InetAddress getRequiredAddress() {
        return requiredAddress;
    }

}

package de.xiekang.talend;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.*;

class IPAddressMatcherTest {

    @Test
    void IPAddressMatcherTest() throws UnknownHostException {
        IPAddressMatcher ipAddressMatcher = new IPAddressMatcher("10.52.128.0/24");
        System.out.println(ipAddressMatcher.matches(InetAddress.getLocalHost().getHostAddress()));
    }

}
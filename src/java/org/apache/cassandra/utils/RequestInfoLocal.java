package org.apache.cassandra.utils;

import java.net.InetAddress;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class RequestInfoLocal
{
    public static ThreadLocal<InetAddress> from = new ThreadLocal<>();

    private RequestInfoLocal() {
    }


}

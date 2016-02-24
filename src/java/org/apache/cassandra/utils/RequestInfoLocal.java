package org.apache.cassandra.utils;

/**
 * @author Alexander Filipchik (alexander.filipchik@am.sony.com)
 */
public class RequestInfoLocal
{
    public static ThreadLocal<String> from = new ThreadLocal<String>();

    private RequestInfoLocal() {
    }


}

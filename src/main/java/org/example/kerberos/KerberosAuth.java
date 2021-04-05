package org.example.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;


public class KerberosAuth {
    public void kerberosAuth(Boolean debug) {
        try {
            System.setProperty("java.security.krb5.conf", "config/krb5.conf");
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            if (debug) {
                System.setProperty("sun.security.krb5.debug", "true");
            }
            ;
            Configuration conf = new Configuration();

            conf.set("hadoop.security.authentication", "Kerberos");

            UserGroupInformation.setConfiguration(conf);

            UserGroupInformation.loginUserFromKeytab("flink@HADOOP.COM", "config/flink.keytab");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


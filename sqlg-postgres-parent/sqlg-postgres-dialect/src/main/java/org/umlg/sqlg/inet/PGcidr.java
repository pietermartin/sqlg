package org.umlg.sqlg.inet;

import org.postgresql.PGConnection;
import org.postgresql.util.PGobject;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

public class PGcidr extends PGobject implements Serializable, Cloneable {

    private String address;

    public PGcidr() {
        type = "cidr";
    }

    public PGcidr(String address) {
        this();
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PGcidr pGcidr)) return false;
        if (!super.equals(o)) return false;
        return Objects.equals(address, pGcidr.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), address);
    }

    @Override
    public void setValue(String s) throws SQLException {
        address = s;
    }

    @Override
    public String getValue() {
        return address;
    }

    public InetAddress toInetAddress() {
        try {
            String host = address.replaceAll(
                    "\\/.*$", ""
            );
            return Inet4Address.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void registerType(Connection conn) throws SQLException {
        conn.unwrap(PGConnection.class).addDataType("cidr", PGcidr.class);
    }

    @Override
    public PGcidr clone() {
        try {
            PGcidr clone = (PGcidr) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}

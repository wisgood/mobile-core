package com.bi.comm.dm.pojo.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class DMTopnURLDAO {

    private Set<String> topnURLSet = null;

    public Set<String> parseDMObj(File file) throws IOException {
        this.topnURLSet = new HashSet<String>();
        BufferedReader in = null;
        try {

            in = new BufferedReader(new InputStreamReader(new FileInputStream(
                    file)));
            String line;
            while ((line = in.readLine()) != null) {
                if (line.contains("#")) {
                    continue;
                }
                String[] strPlate = line.split("\t");
                topnURLSet.add(strPlate[0]);
            }
        } finally {
            in.close();
        }
        return topnURLSet;
    }

    public boolean isContainsURL(String url) {
        if (null != this.topnURLSet) {

            return this.topnURLSet.contains(url);

        }
        return false;
    }

}

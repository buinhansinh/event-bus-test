package com.flystar.data.test.integration;

import com.flystar.data.collector.DataSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by zack on 6/6/2016.
 */
public class FileDataSource implements DataSource {

    private File file;

    public FileDataSource(File file) {
        this.file = file;
    }

    @Override
    public InputStream getInputStream() throws Exception {
        return new FileInputStream(file);
    }
}

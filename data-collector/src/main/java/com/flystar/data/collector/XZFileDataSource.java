package com.flystar.data.collector;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.kafka.common.utils.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Created by zack on 6/6/2016.
 */
public class XZFileDataSource implements DataSource{

    private final File file;

    public XZFileDataSource(File file) {
        this.file = file;
    }

    @Override
    public InputStream getInputStream() throws Exception{
        return new XZCompressorInputStream(new FileInputStream(file));
    }
}

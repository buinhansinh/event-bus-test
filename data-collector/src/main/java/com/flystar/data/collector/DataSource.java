package com.flystar.data.collector;

import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Created by zack on 6/6/2016.
 */
public interface DataSource {

    InputStream getInputStream() throws Exception;
}

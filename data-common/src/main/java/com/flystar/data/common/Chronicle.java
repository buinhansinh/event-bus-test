package com.flystar.data.common;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zack on 6/5/2016.
 */
public class Chronicle {

    public static <T>  ChronicleSet<T> createSet(Class cl, T keySize, long entries){
        final ChronicleSet<T> set = ChronicleSet.of(cl).constantKeySizeBySample(keySize).entries(entries).create();
        return set;
    }

    public static <T,K> ChronicleMap<T,K> createMap(Class key,Class value,T keySize, long entries){
        final ChronicleMap<T,K> map = ChronicleMap.of(key,value).entries(entries).constantKeySizeBySample(keySize).create();
        return map;
    }

    public static <T,K> ChronicleMap<T,K> createMap(Class key,Class value,T keySize, K valueSize, long entries){
        final ChronicleMap<T,K> map = ChronicleMap.of(key,value).entries(entries).constantKeySizeBySample(keySize).averageValue(valueSize).create();
        return map;
    }
}

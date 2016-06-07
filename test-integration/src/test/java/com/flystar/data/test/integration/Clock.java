package com.flystar.data.test.integration;

public class Clock{
       public long start;
       public long stop;

        public void start(){
            this.start = now();
        }

        public void stop(){
            this.stop = now();
        }

        public long duration(){
            return this.stop - this.start;
        }

        public long now(){
            return System.currentTimeMillis();
        }
    }
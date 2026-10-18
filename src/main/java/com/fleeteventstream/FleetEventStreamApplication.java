package com.fleeteventstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FleetEventStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(FleetEventStreamApplication.class, args);
    }
}

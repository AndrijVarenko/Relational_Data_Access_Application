package com.example.relationaldataaccess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
public class RelationalDataAccessApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RelationalDataAccessApplication.class);

    public RelationalDataAccessApplication(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public static void main(String [] args) {
        SpringApplication.run(RelationalDataAccessApplication.class, args);
    }

    final JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... strings) {

        String SQL_Query_String = "Ostap";

        log.info("Creating tables");

        jdbcTemplate.execute("DROP TABLE customers IF EXISTS");
        jdbcTemplate.execute("CREATE TABLE customers (" +
                "id SERIAL, first_name VARCHAR(255), last_name VARCHAR(255))");

        // Split up the array of whole names into an array of first/last names
        List<Object[]> splitUpNames = Arrays.asList("Ivan Javelin", "Anton Shevchenko", "Ostap Shevchuk",
                        "Andrii Savchuk", "Oleg Shevchuk", "Pavlo Shevchenko", "Ostap Antonov").
                stream()
                .map(name -> name.split(" "))
                .collect(Collectors.toList());

        // Use a Java stream to print out each tuple of the list
        splitUpNames.forEach(name -> log.info(String.format("Inserting customer record for %s %s", name[0], name[1])));

        // Uses JdbcTemplate's batchUpdate operation to bulk load data
        jdbcTemplate.batchUpdate("INSERT INTO customers(first_name, last_name) VALUES (?,?)", splitUpNames);

        log.info(String.format("Querying for customer records where first_name = '%s':", SQL_Query_String));

        jdbcTemplate.query("SELECT id, first_name, last_name FROM customers WHERE first_name = ?",
                (rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"),
                        rs.getString("last_name")),
                new Object[] {SQL_Query_String}
                ).forEach(customer -> log.info(customer.toString()));
    }
}
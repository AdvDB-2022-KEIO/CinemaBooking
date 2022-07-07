package com.panda;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.TransactionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

import java.io.IOException;
import java.net.URL;

@SpringBootApplication
public class PandaAdminApplication {

    @Bean
    @Scope("singleton")
    DistributedTransactionManager createScalarDBTransactionManager() throws IOException {
        String databaseProp = "database.properties";
        DatabaseConfig scalarDBConfig =
                new DatabaseConfig(new URL("classpath:" + databaseProp).openConnection().getInputStream());
        TransactionFactory factory = new TransactionFactory(scalarDBConfig);
        return factory.getTransactionManager();
    }

    public static void main(String[] args) {
        SpringApplication.run(PandaAdminApplication.class, args);
    }

}

package com.panda.framework.config;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.TransactionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

import java.io.IOException;
import java.net.URL;

public class ScalarConfig {


    @Bean
    @Scope("singleton")
    DistributedTransactionManager createScalarDBTransactionManager() throws IOException {
        String databaseProp = "database.properties";
        DatabaseConfig scalarDBConfig =
                new DatabaseConfig(new URL("classpath:" + databaseProp).openConnection().getInputStream());
        TransactionFactory factory = new TransactionFactory(scalarDBConfig);

//        TransactionFactory transactionFactory = TransactionFactory.create("<configuration file path>");
//        DistributedTransactionAdmin admin = transactionFactory.getTransactionAdmin();

        return factory.getTransactionManager();
    }

//    @Bean
//    public ModelMapper getModelMapper() {
//        ModelMapper mapper = new ModelMapper();
//        mapper
//                .getConfiguration()
//                .setDestinationNameTransformer(NameTransformers.builder())
//                .setDestinationNamingConvention(NamingConventions.builder())
//                .setMatchingStrategy(MatchingStrategies.STANDARD);
//        return mapper;
//    }
}

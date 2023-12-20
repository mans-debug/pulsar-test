package org.example.conf;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.pulsar.client.api.Schema;
import org.example.User;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.SchemaResolver;

import javax.sql.DataSource;

@ComponentScan("org.example")
@Configuration
public class AppConfig {
    @Bean
    public SchemaResolver.SchemaResolverCustomizer<DefaultSchemaResolver> schemaResolverCustomizer() {
        return (schemaResolver) -> {
            schemaResolver.addCustomSchemaMapping(User.class, Schema.JSON(User.class));
        };
    }
    @Bean
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl( "jdbc_url" );
        config.setUsername( "database_username" );
        config.setPassword( "database_password" );
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        return new HikariDataSource(config);
    }
}

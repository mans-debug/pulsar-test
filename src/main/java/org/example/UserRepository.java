package org.example;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.SQLException;

@Component
@Slf4j
@RequiredArgsConstructor
public class UserRepository {
    private final DataSource dataSource;

    public User createUser(User user) {
        //language=sql
        var query = "insert into human(name, age) values (:1, :2)";
        try (var conn = dataSource.getConnection()) {
            var preparedStatement = conn.prepareStatement(query);
            preparedStatement.setString(1, user.getName());
            preparedStatement.setInt(2, user.getAge());
            int affectedRows = preparedStatement.executeUpdate();
            var keys = preparedStatement.getGeneratedKeys();
            if (keys.next()) {
                user.setId(keys.getLong(1));
            } else {
                log.error("Could not create user, query hasn't returned id");
                throw new RuntimeException();
            }
            return user;
        } catch (SQLException e) {
            log.error("Could not create user", e);
            throw new RuntimeException(e);
        }

    }
}

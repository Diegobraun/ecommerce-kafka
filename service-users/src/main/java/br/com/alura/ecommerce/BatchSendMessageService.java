package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class BatchSendMessageService {
    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        try{
            connection.createStatement().execute("CREATE TABLE Users(uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException exception){
            exception.printStackTrace();
        }

    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        var batchService = new BatchSendMessageService();
        try(var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS", batchService::parse,
                String.class,
                new HashMap<>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<User>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws InterruptedException, ExecutionException, SQLException {
        System.out.println("----------------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic:" + record.value());

        var message = record.value();

        for(User user : getAllUsers()) {
            userDispatcher.send(message.getPayload(), user.getUuid(), user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();
        while(results.next()){
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}

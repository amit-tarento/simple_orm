import com.practice.ConnectionFactory;
import com.practice.Person;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class App {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        List<Person> personList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID uuid = UUID.randomUUID();
            personList.add(new Person(uuid.toString(), "Amit"+i, "Kumar"+i, "amit"+i+"@gmail.com"));
        }

        ConnectionFactory connectionFactory = ConnectionFactory.getConnection();

        personList.forEach(person -> {
            try {
                connectionFactory.save(person);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        personList.forEach(person -> {
            try {
                person = (Person) connectionFactory.read(Person.class, person.getId());
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(person);
        });
        connectionFactory.close();
    }

}

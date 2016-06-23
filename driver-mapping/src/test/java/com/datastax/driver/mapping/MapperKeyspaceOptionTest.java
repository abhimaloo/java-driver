package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TestUtils;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

public class MapperKeyspaceOptionTest extends CCMTestsSupport {

    private MappingManager mappingManager;
    private Mapper<User> mapper;
    private String otherKeyspace;

    @Override
    public void onTestContextInitialized() {
        otherKeyspace = TestUtils.generateIdentifier("ks_");
        execute("CREATE TABLE user (key int primary key, v text)",
                String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", otherKeyspace),
                String.format("CREATE TABLE %s.user (key int primary key, v text)", otherKeyspace));

        mappingManager = new MappingManager(session());
        mapper = mappingManager.mapper(User.class);
    }

    @BeforeMethod(groups = "short")
    public void setup() {
        execute("TRUNCATE user", String.format("TRUNCATE %s.user", otherKeyspace));
    }

    @Test(groups = "short")
    public void should_use_session_keyspace_when_no_option() {
        should_use_keyspace_for_options(keyspace);
    }

    @Test(groups = "short")
    public void should_use_keyspace_option() {
        should_use_keyspace_for_options(otherKeyspace, Mapper.Option.keyspace(otherKeyspace));
    }

    private void should_use_keyspace_for_options(String expectedKeyspace, Mapper.Option... options) {
        User user = new User(42, "helloworld");
        mapper.save(user, options);
        Row row = session().execute(String.format("SELECT v FROM %s.user WHERE key = 42", expectedKeyspace)).one();
        assertThat(row.getString(0)).isEqualTo("helloworld");

        Object[] getOptions = new Object[options.length + 1];
        getOptions[0] = 42;
        System.arraycopy(options, 0, getOptions, 1, options.length);
        assertThat(mapper.get(getOptions).getV()).isEqualTo("helloworld");

        mapper.delete(user, options);
        row = session().execute(String.format("SELECT v FROM %s.user WHERE key = 42", expectedKeyspace)).one();
        assertThat(row).isNull();
    }

    @Test(groups = "short")
    public void should_use_option_over_annotation() {
        User2 user = new User2(42, "helloworld");
        mappingManager.mapper(User2.class).save(user, Mapper.Option.keyspace(otherKeyspace));
        Row row = session().execute(String.format("SELECT v FROM %s.user WHERE key = 42", otherKeyspace)).one();
        assertThat(row.getString(0)).isEqualTo("helloworld");
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private int key;
        private String v;

        public User() {
        }

        public User(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }
    }

    @Table(keyspace = "doesnotexist", name = "user")
    public static class User2 {
        @PartitionKey
        private int key;
        private String v;

        public User2() {
        }

        public User2(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }
    }
}

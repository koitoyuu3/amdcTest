package com.example.amdctest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
class AmdcTestApplicationTests {

    private static final String REDIS_HOST = "10.3.242.122"; // 替换为实际的 Redis 主机地址
    private static final int REDIS_PORT = 6359;
    private static final int TEST_DATA_SIZE = 10000; // 测试数据量
    private static final int THREAD_COUNT = 50; // 模拟的并发线程数
    private static final int REQUEST_COUNT = 10000; // 每个线程发送的请求数量
    private static JedisPool jedisPool;

    @BeforeEach
    void setUp() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(50); // 最大连接数
        poolConfig.setMaxIdle(10);  // 最大空闲连接数
        poolConfig.setMinIdle(2);   // 最小空闲连接数
        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
    }
    @Test
    public void stage_1() {
        try (Jedis jedis = jedisPool.getResource()) {
            // 插入数据并测量时间
            long startInsertTime = System.nanoTime();
            for (int i = 0; i < TEST_DATA_SIZE; i++) {
                String key = "key" + i;
                String value = "value" + i;
                jedis.set(key, value);
            }
            long endInsertTime = System.nanoTime();
            double avgInsertTime = (endInsertTime - startInsertTime) / (double) TEST_DATA_SIZE;
            System.out.println("Average time per insert (ns): " + avgInsertTime);

            // 获取数据并测量时间
            long startGetTime = System.nanoTime();
            for (int i = 0; i < TEST_DATA_SIZE; i++) {
                String key = "key" + i;
                jedis.get(key);
            }
            long endGetTime = System.nanoTime();
            double avgGetTime = (endGetTime - startGetTime) / (double) TEST_DATA_SIZE;
            System.out.println("Average time per get (ns): " + avgGetTime);

            // 计算吞吐量
            double insertThroughput = 1e9 / avgInsertTime; // 每秒插入操作数
            double getThroughput = 1e9 / avgGetTime;       // 每秒获取操作数
            System.out.println("Insert throughput (ops/sec): " + insertThroughput);
            System.out.println("Get throughput (ops/sec): " + getThroughput);
        } finally {
            jedisPool.close();
        }
    }

    @Test
    public void stage_2() throws InterruptedException {
        try (Jedis jedis = jedisPool.getResource()) {
            for (int i = 0; i < TEST_DATA_SIZE; i++) {
                jedis.set("key" + i, "value" + i);
            }
        }

        // 并发请求测试
        AtomicInteger hitCount = new AtomicInteger(0); // 命中次数
        AtomicInteger missCount = new AtomicInteger(0); // 未命中次数
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        // 启动多个线程并发请求 Redis 缓存
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.execute(() -> {
                try (Jedis jedis = jedisPool.getResource()) {
                    for (int j = 0; j < REQUEST_COUNT; j++) {
                        String key = "key" + (int) (Math.random() * TEST_DATA_SIZE * 1.5); // 随机请求缓存中的数据
                        if (jedis.get(key) != null) {
                            hitCount.incrementAndGet(); // 如果缓存中有该 key，则命中
                        } else {
                            missCount.incrementAndGet(); // 否则未命中
                        }
                    }
                } finally {
                    latch.countDown(); // 完成一个线程
                }
            });
        }

        // 等待所有线程完成
        latch.await();
        executorService.shutdown();

        // 计算并输出命中率
        int totalRequests = hitCount.get() + missCount.get();
        double hitRate = (hitCount.get() / (double) totalRequests) * 100;
        System.out.println("Total Requests: " + totalRequests);
        System.out.println("Cache Hits: " + hitCount.get());
        System.out.println("Cache Misses: " + missCount.get());
        System.out.println("Cache Hit Rate: " + hitRate + "%");

        // 关闭 Jedis 连接池
        jedisPool.close();
    }

    @Test
    public void stage_3() {
        int MAX_KEYS = 20000;
        try (Jedis jedis = jedisPool.getResource()) {
            // 配置 Redis 的 LRU 策略和最大内存
            jedis.configSet("maxmemory", "10B"); // 根据实际测试需求调整
            jedis.configSet("maxmemory-policy", "allkeys-lru");

            // 验证配置是否生效
            String maxMemory = jedis.configGet("maxmemory").get(1);
            String policy = jedis.configGet("maxmemory-policy").get(1);
            System.out.println("Max Memory: " + maxMemory);
            System.out.println("Max Memory Policy: " + policy);

            // 插入数据，模拟超过缓存容量的情况
            for (int i = 0; i < MAX_KEYS; i++) {
                String key = "key" + i;
                String value = "value" + i;
                jedis.set(key, value);

                // 每插入 100 个键，随机访问已插入的某些键。
                // 这些被访问的键将被视为“最近使用”，可以避免被优先淘汰。
                // 随机访问一些键，以测试 LRU 策略
                if (i % 100 == 0) {
                    int randomKeyIndex = new Random().nextInt(i + 1); // 随机访问之前插入的键
                    jedis.get("key" + randomKeyIndex);
                }
            }

            // 验证 LRU 策略：检查某些键是否已经被淘汰
            int evictedCount = 0;
            for (int i = 0; i < MAX_KEYS; i++) {
                String key = "key" + i;
                if (jedis.get(key) == null) {
                    System.out.println(key + " has been evicted");
                    evictedCount++;
                }
            }
            System.out.println("Total evicted keys: " + evictedCount);
        } finally {
            jedisPool.close();
        }
    }

    @Test
    public void stage_4() {
        String DB_URL = "jdbc:mysql://localhost:3306/student_db";
        String DB_USER = "root";
        String DB_PASSWORD = "123456";
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
             Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

            // 插入数据库和缓存数据
            for (int i = 0; i < 100; i++) {
                int key = i;
                String value = "value" + i;

                // 记录数据库更新开始时间
                Instant dbUpdateStart = Instant.now();

                // 数据库插入操作
                boolean dbSuccess = insertIntoDatabase(conn, key, value);
                if (!dbSuccess) {
                    System.out.println("数据库插入失败: " + key);
                    continue;
                }

                // 将数据写入 Redis 缓存
                jedis.set("key" + key, value);

                // 记录缓存更新完成时间
                Instant cacheUpdateEnd = Instant.now();

                // 计算同步延迟
                Duration delay = Duration.between(dbUpdateStart, cacheUpdateEnd);
                System.out.println("同步延迟 (毫秒) for " + key + ": " + delay.toMillis());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static boolean insertIntoDatabase(Connection conn, int key, String value) {
        String sql = "INSERT INTO class_info (key_column, value_column) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE value_column = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, key);
            stmt.setString(2, value);
            stmt.setString(3, value);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

}

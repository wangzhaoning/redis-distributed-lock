package com.github.hcsp.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.UUID;
import java.util.concurrent.Callable;

public class DistributedLock {
    /**
     * The lock name. A lock with same name might be shared in multiple JVMs.
     */
    private String name;
    JedisPool pool = new JedisPool();
    private final Subscriber subscriber = new Subscriber();

    protected long internalLockLeaseTime = 30000;
    SetParams params = SetParams.setParams().nx().px(internalLockLeaseTime);

    public DistributedLock(String name) {
        this.name = name;
    }

    /**
     * Run a given action under lock.
     *
     * @param callable the action to be executed
     * @param <T>      return type
     * @return the result
     */
    public <T> T runUnderLock(Callable<T> callable) throws Exception {
        T res;
        try (Jedis jedis = pool.getResource()) {
            String uuid = lock(jedis);
            res = callable.call();
            unlock(uuid, jedis);
        }
        return res;
    }


    public String lock(Jedis jedis) throws InterruptedException {
        long start = System.currentTimeMillis();
        String uuid = UUID.randomUUID().toString();
        while (true) {
            String res = jedis.set(name, uuid, params);
            if ("OK".equals(res)) {
                return uuid;
            }

            long spendTime = System.currentTimeMillis() - start;
            long timeout = 999999;
            if (spendTime >= timeout) {
                throw new RuntimeException("acquire lock timeout");
            }
            jedis.subscribe(subscriber,name);
        }
    }

    public void unlock(String uuid, Jedis jedis) throws InterruptedException {
            String currentLock = jedis.get(name);

            if (uuid.equals(currentLock)) {
                jedis.del(name);
                jedis.publish(name, "ok");
        }
    }
}

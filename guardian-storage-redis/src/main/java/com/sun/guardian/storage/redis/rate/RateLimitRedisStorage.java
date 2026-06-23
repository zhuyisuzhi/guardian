package com.sun.guardian.storage.redis.rate;

import com.sun.guardian.rate.limit.core.domain.token.RateLimitToken;
import com.sun.guardian.rate.limit.core.enums.algorithm.RateLimitAlgorithm;
import com.sun.guardian.rate.limit.core.storage.RateLimitStorage;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 限流存储 - Redis
 * 滑动窗口：ZSET（score=时间戳，member=时间戳-随机数），令牌桶：HASH（tokens + lastRefill）
 *
 * @author scj
 * @version java version 1.8
 * @since 2026-02-12 17:13
 */
@RequiredArgsConstructor
public class RateLimitRedisStorage implements RateLimitStorage {

    private final StringRedisTemplate redisTemplate;

    private static final DefaultRedisScript<Boolean> REDISSON_TRY_SET_RATE_SCRIPT;
    private static final DefaultRedisScript<Long> REDISSON_TRY_ACQUIRE_SCRIPT;

    static {
        REDISSON_TRY_SET_RATE_SCRIPT = new DefaultRedisScript<>();
        /*
          Redisson 参考RedissonRateLimiter.trySetRate方法
         */
        REDISSON_TRY_SET_RATE_SCRIPT.setScriptText(
                "redis.call('hsetnx', KEYS[1], 'rate', ARGV[1]);"
                        + "redis.call('hsetnx', KEYS[1], 'interval', ARGV[2]);"
                        + "redis.call('hsetnx', KEYS[1], 'keepAliveTime', ARGV[4]);"
                        + "local res = redis.call('hsetnx', KEYS[1], 'type', ARGV[3]);"
                        + "if res == 1 and tonumber(ARGV[4]) > 0 then "
                        + "redis.call('pexpire', KEYS[1], ARGV[4]); "
                        + "end; "
                        + "return res;"
        );
        REDISSON_TRY_SET_RATE_SCRIPT.setResultType(Boolean.class);

        REDISSON_TRY_ACQUIRE_SCRIPT = new DefaultRedisScript<>();
        /*
          Redisson 参考RedissonRateLimiter.tryAcquire方法
          返回 true=放行 false=拒绝
         */
        REDISSON_TRY_ACQUIRE_SCRIPT.setScriptText(
                "local rate = redis.call('hget', KEYS[1], 'rate');"
                        + "local interval = redis.call('hget', KEYS[1], 'interval');"
                        + "local type = redis.call('hget', KEYS[1], 'type');"
                        + "assert(rate ~= false and interval ~= false and type ~= false, 'RateLimiter is not initialized')"

                        + "local valueName = KEYS[2];"
                        + "local permitsName = KEYS[4];"
                        + "if type == '1' then "
                        + "valueName = KEYS[3];"
                        + "permitsName = KEYS[5];"
                        + "end;"

                        + "assert(tonumber(rate) >= tonumber(ARGV[1]), 'Requested permits amount cannot exceed defined rate'); "

                        + "local currentValue = redis.call('get', valueName); "
                        + "local res;"
                        + "if currentValue ~= false then "
                        + "local expiredValues = redis.call('zrangebyscore', permitsName, 0, tonumber(ARGV[2]) - interval); "
                        + "local released = 0; "
                        + "for i, v in ipairs(expiredValues) do "
                        + "local random, permits = struct.unpack('Bc0I', v);"
                        + "released = released + permits;"
                        + "end; "

                        + "if released > 0 then "
                        + "redis.call('zremrangebyscore', permitsName, 0, tonumber(ARGV[2]) - interval); "
                        + "if tonumber(currentValue) + released > tonumber(rate) then "
                        + "local values = redis.call('zrange', permitsName, 0, -1); "
                        + "local used = 0; "
                        + "for i, v in ipairs(values) do "
                        + "local random, permits = struct.unpack('Bc0I', v);"
                        + "used = used + permits;"
                        + "end; "
                        + "currentValue = tonumber(rate) - used; "
                        + "else "
                        + "currentValue = tonumber(currentValue) + released; "
                        + "end; "
                        + "redis.call('set', valueName, currentValue);"
                        + "end;"

                        + "if tonumber(currentValue) < tonumber(ARGV[1]) then "
                        + "local firstValue = redis.call('zrange', permitsName, 0, 0, 'withscores'); "
                        + "res = 3 + interval - (tonumber(ARGV[2]) - tonumber(firstValue[2]));"
                        + "else "
                        + "redis.call('zadd', permitsName, ARGV[2], struct.pack('Bc0I', string.len(ARGV[3]), ARGV[3], ARGV[1])); "
                        + "redis.call('decrby', valueName, ARGV[1]); "
                        + "res = nil; "
                        + "end; "
                        + "else "
                        + "redis.call('set', valueName, rate); "
                        + "redis.call('zadd', permitsName, ARGV[2], struct.pack('Bc0I', string.len(ARGV[3]), ARGV[3], ARGV[1])); "
                        + "redis.call('decrby', valueName, ARGV[1]); "
                        + "res = nil; "
                        + "end;"

                        + "local keepAliveTime = redis.call('hget', KEYS[1], 'keepAliveTime'); "
                        + "if (keepAliveTime ~= false and tonumber(keepAliveTime) > 0) then "
                        + "redis.call('pexpire', KEYS[1], keepAliveTime); "
                        + "redis.call('pexpire', valueName, keepAliveTime); "
                        + "redis.call('pexpire', permitsName, keepAliveTime); "
                        + "else "
                        + "local ttl = redis.call('pttl', KEYS[1]); "
                        + "if ttl > 0 then "
                        + "redis.call('pexpire', valueName, ttl); "
                        + "redis.call('pexpire', permitsName, ttl); "
                        + "end; "
                        + "end; "
                        + "return res;"
        );
        REDISSON_TRY_ACQUIRE_SCRIPT.setResultType(Long.class);
    }

    /**
     * KEYS[1]=限流Key, ARGV[1]=now, ARGV[2]=windowStart, ARGV[3]=maxCount, ARGV[4]=expireSeconds
     * 返回 1=放行 0=拒绝
     */
    private static final DefaultRedisScript<Long> SLIDING_WINDOW_SCRIPT;

    static {
        SLIDING_WINDOW_SCRIPT = new DefaultRedisScript<>();
        SLIDING_WINDOW_SCRIPT.setScriptText(
                "local key = KEYS[1]\n" +
                        "local now = tonumber(ARGV[1])\n" +
                        "local windowStart = tonumber(ARGV[2])\n" +
                        "local maxCount = tonumber(ARGV[3])\n" +
                        "local expireSeconds = tonumber(ARGV[4])\n" +
                        "redis.call('ZREMRANGEBYSCORE', key, 0, windowStart)\n" +
                        "local current = redis.call('ZCARD', key)\n" +
                        "if current < maxCount then\n" +
                        "    redis.call('ZADD', key, now, tostring(now) .. '-' .. math.random(100000))\n" +
                        "    redis.call('EXPIRE', key, expireSeconds)\n" +
                        "    return 1\n" +
                        "else\n" +
                        "    return 0\n" +
                        "end"
        );
        SLIDING_WINDOW_SCRIPT.setResultType(Long.class);
    }

    /**
     * KEYS[1]=限流Key, ARGV[1]=ratePerSecond, ARGV[2]=capacity, ARGV[3]=now(ms), ARGV[4]=expireSeconds
     * 返回 1=放行 0=拒绝
     */
    private static final DefaultRedisScript<Long> TOKEN_BUCKET_SCRIPT;

    static {
        TOKEN_BUCKET_SCRIPT = new DefaultRedisScript<>();
        TOKEN_BUCKET_SCRIPT.setScriptText(
                "local key = KEYS[1]\n" +
                        "local ratePerSecond = tonumber(ARGV[1])\n" +
                        "local capacity = tonumber(ARGV[2])\n" +
                        "local now = tonumber(ARGV[3])\n" +
                        "local expireSeconds = tonumber(ARGV[4])\n" +
                        "local last = redis.call('HMGET', key, 'tokens', 'lastRefill')\n" +
                        "local tokens = tonumber(last[1])\n" +
                        "local lastRefill = tonumber(last[2])\n" +
                        "if tokens == nil then\n" +
                        "    tokens = capacity\n" +
                        "    lastRefill = now\n" +
                        "end\n" +
                        "local elapsed = math.max(0, (now - lastRefill) / 1000)\n" +
                        "tokens = math.min(capacity, tokens + elapsed * ratePerSecond)\n" +
                        "local allowed = 0\n" +
                        "if tokens >= 1 then\n" +
                        "    tokens = tokens - 1\n" +
                        "    allowed = 1\n" +
                        "end\n" +
                        "redis.call('HMSET', key, 'tokens', tostring(tokens), 'lastRefill', tostring(now))\n" +
                        "redis.call('EXPIRE', key, expireSeconds)\n" +
                        "return allowed"
        );
        TOKEN_BUCKET_SCRIPT.setResultType(Long.class);
    }

    /**
     * 尝试获取限流许可
     */
    @Override
    public boolean tryAcquire(RateLimitToken token) {
        if (token.getAlgorithm() == RateLimitAlgorithm.REDISSON) {
            return tryAcquireRedisson(token);
        }
        if (token.getAlgorithm() == RateLimitAlgorithm.TOKEN_BUCKET) {
            return tryAcquireTokenBucket(token);
        }
        return tryAcquireSlidingWindow(token);
    }

    /**
     * Redisson限流判定
     */
    private boolean tryAcquireRedisson(RateLimitToken token) {
        redisTemplate.execute(
                REDISSON_TRY_SET_RATE_SCRIPT,
                Collections.singletonList(token.getKey()),
                String.valueOf(token.getQps()),
                String.valueOf(token.getWindowMillis()),
                String.valueOf(0),
                String.valueOf(token.getWindowMillis() + 3600_000)
        );
        long currentTimeMillis = System.currentTimeMillis();
        Long res = redisTemplate.execute(
                REDISSON_TRY_ACQUIRE_SCRIPT,
                Arrays.asList(token.getKey(), suffixName(token.getKey(), "value"), suffixName(token.getKey(), "value"), suffixName(token.getKey(), "permits"), suffixName(token.getKey(), "permits")),
                "1",
                Long.toString(currentTimeMillis),
                currentTimeMillis + "-" + ThreadLocalRandom.current().nextInt(1, 100001)
        );
        return res == null;
    }

    private String suffixName(String name, String suffix) {
        if (name.contains("{")) {
            return name + ":" + suffix;
        }
        return "{" + name + "}:" + suffix;
    }

    /**
     * 滑动窗口限流判定
     */
    private boolean tryAcquireSlidingWindow(RateLimitToken token) {
        long now = System.currentTimeMillis();
        long windowStart = now - token.getWindowMillis();

        Long result = redisTemplate.execute(SLIDING_WINDOW_SCRIPT,
                Collections.singletonList(token.getKey()),
                String.valueOf(now),
                String.valueOf(windowStart),
                String.valueOf(token.getMaxCount()),
                String.valueOf(token.getWindowSeconds() + 1));

        return result != null && result == 1L;
    }

    /**
     * 令牌桶限流判定
     */
    private boolean tryAcquireTokenBucket(RateLimitToken token) {
        long now = System.currentTimeMillis();

        Long result = redisTemplate.execute(TOKEN_BUCKET_SCRIPT,
                Collections.singletonList(token.getKey()),
                String.valueOf(token.getRefillRatePerSecond()),
                String.valueOf(token.getEffectiveCapacity()),
                String.valueOf(now),
                String.valueOf(token.getBucketExpireSeconds()));

        return result != null && result == 1L;
    }
}

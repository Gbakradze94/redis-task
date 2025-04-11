package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.args.ExpiryOption;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class JedisRateLimitService implements RateLimitService {

    private final List<RateLimitRule> rateLimitRules;
    private final JedisCluster jedisCluster;

    public JedisRateLimitService(List<RateLimitRule> rateLimitRules, JedisCluster jedisCluster) {
        this.rateLimitRules = rateLimitRules;
        this.jedisCluster = jedisCluster;
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {
        return requestDescriptors.stream()
                .anyMatch(this::isRateLimitReached);
    }

    private boolean isRateLimitReached(RequestDescriptor requestDescriptor) {
        // check if rate limit is reached
        return rateLimitRules.stream()
                .filter(rule -> isDescriptorMatchingRule(requestDescriptor, rule))
                .findFirst()
                .map(rateLimitRule -> isRequestLimitReached(requestDescriptor, rateLimitRule))
                .orElse(true);
    }

    private boolean isRequestLimitReached(RequestDescriptor requestDescriptor, RateLimitRule rateLimitRule) {
        long currentRequestCount = incrementAndGetRequestValue(requestDescriptor, rateLimitRule.getTimeInterval());
        return rateLimitRule.hasExceededLimit(currentRequestCount);
    }

    private long incrementAndGetRequestValue(RequestDescriptor requestDescriptor, RateLimitTimeInterval timeInterval) {
        String key = generateKey(requestDescriptor, timeInterval);
        long requestCount = jedisCluster.incr(key);
        long keyTtl = TimeUnit.HOURS.toSeconds(1);
        jedisCluster.expire(key, keyTtl, ExpiryOption.NX);
        return requestCount;
    }

    private String generateKey(RequestDescriptor requestDescriptor, RateLimitTimeInterval timeInterval) {
        String accountId = requestDescriptor.getAccountId().orElse("");
        String clientIp = requestDescriptor.getClientIp().orElse("");
        String requestType = requestDescriptor.getRequestType().orElse("");

        return String.format("requestDescriptor:%s:%s:%s:%s_%s",
                accountId, clientIp, requestType, timeInterval.name(), getCurrentTime(timeInterval));
    }

    private Integer getCurrentTime(RateLimitTimeInterval timeInterval) {
        if (timeInterval.equals(RateLimitTimeInterval.MINUTE)) {
            return LocalTime.now().getMinute();
        } else if (timeInterval.equals(RateLimitTimeInterval.HOUR)) {
            return LocalTime.now().getHour();
        } else {
            throw new IllegalArgumentException("Invalid time interval: " + timeInterval);
        }
    }

    private boolean isDescriptorMatchingRule(RequestDescriptor requestDescriptor, RateLimitRule rule) {
        return isMatchingOrUnique(requestDescriptor.getAccountId(), rule.getAccountId())
                && isMatchingOrUnique(requestDescriptor.getClientIp(), rule.getClientIp())
                && isMatchingOrUnique(requestDescriptor.getRequestType(), rule.getRequestType());
    }

    private boolean isMatchingOrUnique(Optional<String> descriptorProperty, Optional<String> ruleField) {
        return doFieldsMatch(descriptorProperty, ruleField) ||
                isFieldUniqueWhenRuleEmpty(descriptorProperty, ruleField);
    }

    private boolean doFieldsMatch(Optional<String> descriptorProperty, Optional<String> ruleField) {
        return descriptorProperty.isEmpty() &&
                ruleField.isEmpty()
                || descriptorProperty.isPresent() &&
                ruleField.isPresent() &&
                descriptorProperty.equals(ruleField);
    }

    private boolean isFieldUniqueWhenRuleEmpty(Optional<String> descriptorField, Optional<String> ruleField) {
        return ruleField.isPresent() && ruleField.get().isEmpty()
                && descriptorField.isPresent()
                && rateLimitRules.stream()
                .noneMatch(r -> descriptorField.get().equals(r.getAccountId().orElse(""))
                        || descriptorField.get().equals(r.getClientIp().orElse(""))
                        || descriptorField.get().equals(r.getRequestType().orElse("")));

    }
}

package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class JedisRateLimitService implements RateLimitService{

    private final List<RateLimitRule> rateLimitRules;

    public JedisRateLimitService(List<RateLimitRule> rateLimitRules) {
        this.rateLimitRules = rateLimitRules;
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {
        return requestDescriptors.stream()
                .anyMatch(this::isRateLimitReached);
    }

    private boolean isRateLimitReached(RequestDescriptor requestDescriptor) {
        // check if rate limit is reached
        return false;
    }
}

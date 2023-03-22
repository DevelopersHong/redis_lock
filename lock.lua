if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2])
end

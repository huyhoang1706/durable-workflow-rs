-- KEYS[1] = key; ARGV[1] = ttl_ms applied only when the key is created (0 = none)
local v = redis.call('INCR', KEYS[1])
if v == 1 and tonumber(ARGV[1]) > 0 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return v

-- KEYS[1] = the run's set of stream names, KEYS[2] = the run's set of closed streams
-- ARGV[1] = ttl_ms, ARGV[2] = max_len, ARGV[3] = prefix of the run's stream keys
-- Appends a closed marker to every open stream (all keys share the run's hash tag).
local names = redis.call('SMEMBERS', KEYS[1])
for _, name in ipairs(names) do
  if redis.call('SISMEMBER', KEYS[2], name) == 0 then
    local key = ARGV[3] .. name
    redis.call('XADD', key, 'MAXLEN', '~', ARGV[2], '*', 'k', '', 'a', '0', 't', 'closed', 'd', 'null')
    redis.call('SADD', KEYS[2], name)
    redis.call('PEXPIRE', key, ARGV[1])
  end
end
if #names > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  redis.call('PEXPIRE', KEYS[2], ARGV[1])
end
return #names

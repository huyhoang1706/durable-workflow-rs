-- KEYS[1] = stream, KEYS[2] = the run's set of stream names, KEYS[3] = the run's set of closed streams
-- ARGV[1] = ttl_ms, ARGV[2] = max_len, ARGV[3] = stream name,
-- then 4 args per chunk: step key, attempt, kind (data|started|failed|closed), json
-- Chunks after a closed chunk in the same call are dropped.
-- Returns the id of the last appended entry, or the string CLOSED if the stream was closed.
local name = ARGV[3]
if redis.call('SISMEMBER', KEYS[3], name) == 1 then
  return 'CLOSED'
end
local last = ''
local i = 4
while i <= #ARGV do
  last = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[2], '*',
    'k', ARGV[i], 'a', ARGV[i + 1], 't', ARGV[i + 2], 'd', ARGV[i + 3])
  if ARGV[i + 2] == 'closed' then
    redis.call('SADD', KEYS[3], name)
    -- Nothing may follow the closed marker.
    break
  end
  i = i + 4
end
redis.call('SADD', KEYS[2], name)
for k = 1, 3 do
  redis.call('PEXPIRE', KEYS[k], ARGV[1])
end
return last

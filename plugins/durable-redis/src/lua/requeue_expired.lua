-- KEYS[1] = scope prefix; ARGV = now_ms. Returns the number of runs requeued.
local P = KEYS[1]
local function s(n) return string.format('%.0f', n) end
local now = tonumber(ARGV[1])
local count = 0
for _, run in ipairs(redis.call('ZRANGEBYSCORE', P .. ':inflight', '-inf', s(now))) do
  local ik = P .. ':item:' .. run
  redis.call('ZREM', P .. ':inflight', run)
  if redis.call('HGET', ik, 'state') == 'l' then
    local ckey = redis.call('HGET', ik, 'ckey')
    if ckey and ckey ~= '' then redis.call('ZREM', P .. ':inflight:k:' .. ckey, run) end
    local due = tonumber(redis.call('HGET', ik, 'due'))
    local kind = redis.call('HGET', ik, 'kind')
    local rerun = redis.call('HGET', ik, 'rerun_due')
    if rerun then
      due = math.min(due, tonumber(rerun))
      kind = 'resume'
    end
    due = math.min(due, now)
    redis.call('HSET', ik, 'state', 'q', 'due', s(due), 'kind', kind)
    redis.call('HDEL', ik, 'token', 'expires', 'rerun_due')
    redis.call('ZADD', P .. ':q:' .. kind, due, run)
    count = count + 1
  end
end
return count

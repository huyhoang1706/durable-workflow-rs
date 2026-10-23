-- KEYS[1] = scope prefix; ARGV = now_ms, lease_ms, then pairs [run, token]
-- Returns JSON array of the runs whose lease was lost.
local P = KEYS[1]
local function s(n) return string.format('%.0f', n) end
local now, lease = tonumber(ARGV[1]), tonumber(ARGV[2])
local lost = {}
for i = 3, #ARGV, 2 do
  local run, token = ARGV[i], ARGV[i + 1]
  local ik = P .. ':item:' .. run
  local state = redis.call('HGET', ik, 'state')
  local cur = redis.call('HGET', ik, 'token')
  if state == 'l' and cur == token then
    local expires = now + lease
    redis.call('HSET', ik, 'expires', s(expires))
    redis.call('ZADD', P .. ':inflight', expires, run)
    local ckey = redis.call('HGET', ik, 'ckey')
    if ckey and ckey ~= '' then redis.call('ZADD', P .. ':inflight:k:' .. ckey, expires, run) end
  else
    lost[#lost + 1] = run
  end
end
if #lost == 0 then return '[]' end
return cjson.encode(lost)

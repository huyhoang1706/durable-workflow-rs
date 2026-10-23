-- KEYS[1] = scope prefix
-- ARGV    = now_ms, max, lease_ms, workflow_limit, key_limit,
--           throttle_limit, throttle_period_ms, throttle_burst   (limits are -1 when unset)
-- Returns JSON: {leases = [[run, token, expires, kind, due, json], ...], next_due = ms | -1}
local P = KEYS[1]
local function s(n) return string.format('%.0f', n) end
local now, max, lease = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local wf_limit, key_limit = tonumber(ARGV[4]), tonumber(ARGV[5])
local th_limit, th_period, th_burst = tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8])

local next_due = nil
local function note(t) if next_due == nil or t < next_due then next_due = t end end

-- Capacity counts executing leases only; expired leases free themselves.
local inflight = redis.call('ZCOUNT', P .. ':inflight', '(' .. s(now), '+inf')

for _, kind in ipairs({ 'resume', 'start' }) do
  local head = redis.call('ZRANGEBYSCORE', P .. ':q:' .. kind, '(' .. s(now), '+inf',
                          'WITHSCORES', 'LIMIT', 0, 1)
  if head[2] then note(tonumber(head[2])) end
end

local out = {}
local scan = max * 8 + 64
-- In-flight runs (resumes) win over new ones (starts).
for _, kind in ipairs({ 'resume', 'start' }) do
  local blocked = wf_limit >= 0 and inflight >= wf_limit
  if #out < max and not blocked then
    local runs = redis.call('ZRANGEBYSCORE', P .. ':q:' .. kind, '-inf', s(now), 'LIMIT', 0, scan)
    for _, run in ipairs(runs) do
      if #out >= max or (wf_limit >= 0 and inflight >= wf_limit) then break end
      local ik = P .. ':item:' .. run
      local ckey = redis.call('HGET', ik, 'ckey')
      local ok = true
      if key_limit >= 0 and ckey ~= '' then
        local n = redis.call('ZCOUNT', P .. ':inflight:k:' .. ckey, '(' .. s(now), '+inf')
        if n >= key_limit then ok = false end
      end
      if ok and kind == 'start' and th_limit >= 0 then
        -- GCRA on starts only: delays runs, never drops them.
        local tk = P .. ':throttle:' .. redis.call('HGET', ik, 'tkey')
        local tat = tonumber(redis.call('GET', tk)) or now
        if tat < now then tat = now end
        local emission = math.floor(th_period / th_limit)
        local allow_at = tat - emission * (math.max(th_burst, 1) - 1)
        if now < allow_at then
          note(allow_at)
          ok = false
        else
          redis.call('SET', tk, s(tat + emission), 'PX', s(th_period * 2))
        end
      end
      if ok then
        local token = redis.call('INCR', P .. ':tokens')
        local expires = now + lease
        redis.call('ZREM', P .. ':q:' .. kind, run)
        redis.call('HSET', ik, 'state', 'l', 'token', token, 'expires', s(expires))
        redis.call('ZADD', P .. ':inflight', expires, run)
        if ckey ~= '' then redis.call('ZADD', P .. ':inflight:k:' .. ckey, expires, run) end
        inflight = inflight + 1
        out[#out + 1] = { run, tostring(token), s(expires), kind,
                          redis.call('HGET', ik, 'due'), redis.call('HGET', ik, 'json') }
      end
    end
  end
end
return cjson.encode({ leases = out, next_due = next_due or -1 })

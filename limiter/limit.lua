local key = KEYS[1]
local window = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])

--删除窗口之外的数据
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local cnt = redis.call('ZCOUNT', key, '-inf', '+inf')

if cnt >= threshold then
    return 'true'
else
    redis.call('ZADD', key, now, now)
    redis.call('PEXPIRE', key, window)
    return 'false'
end
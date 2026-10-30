# vim:set ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_KAFKA_HOST} = '127.0.0.1';
$ENV{TEST_NGINX_KAFKA_PORT} = '9092';
$ENV{TEST_NGINX_KAFKA_ERR_PORT} = '9091';

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: simple pack
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local request = require "resty.kafka.request"
            local req = request:new(request.ProduceRequest, 1, "clientid")

            local function printx()
                local str = req._req[#req._req]
                for i = 1, #str do
                    ngx.print(bit.tohex(string.byte(str, i), 2))
                end
                ngx.say("")
            end

            req:int16(-1 * math.pow(2, 15)); printx()
            req:int16(math.pow(2, 15) - 1); printx()
            req:int16(-1); printx()
            req:int32(-1 * math.pow(2, 31)); printx()
            req:int32(math.pow(2, 31) - 1); printx()
            req:int64(-1LL * math.pow(2, 32) * math.pow(2, 31)); printx()
            req:int64(1ULL * math.pow(2, 32) * math.pow(2, 31) - 1); printx()
        ';
    }
--- request
GET /t
--- response_body
8000
7fff
ffff
80000000
7fffffff
8000000000000000
7fffffffffffffff
--- no_error_log
[error]



=== TEST 2: response unpack
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local request = require "resty.kafka.request"
            local response = require "resty.kafka.response"

            local function compare(func, number)
                local req = request:new(request.ProduceRequest, 1, "clientid")
                req:int32(100)
                local correlation_id = req._req[#req._req]

                req[func](req, number)
                local str = correlation_id .. req._req[#req._req]

                local resp = response:new(str, req.api_version)

                local cnumber = resp[func](resp)

                ngx.say(func, ": ", tostring(number), ", ", number == cnumber)
            end

            compare("int16", 0x7fff)
            compare("int16", 0x7fff * -1 - 1)
            compare("int32", 0x7fffffff)
            compare("int32", 0x7fffffff * -1 - 1)
            compare("int64", 1ULL * math.pow(2, 32) * math.pow(2, 31) - 1)
            compare("int64", -1LL * math.pow(2, 32) * math.pow(2, 31))
        ';
    }
--- request
GET /t
--- response_body
int16: 32767, true
int16: -32768, true
int32: 2147483647, true
int32: -2147483648, true
int64: 9223372036854775807ULL, true
int64: -9223372036854775808LL, true
--- no_error_log
[error]



=== TEST 3: message format v2 record batch
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local request = require "resty.kafka.request"
            local response = require "resty.kafka.response"
            local record = require "resty.kafka.protocol.record"
            local protocol = require "resty.kafka.protocol.common"
            local producer = require "resty.kafka.producer"

            local function message_set_bytes(api_version, messages, index)
                local req = request:new(request.ProduceRequest, 1, "cid", api_version)
                local start_slot = req.offset
                req:message_set(messages, index)

                local parts = {}
                for i = start_slot, req.offset - 1 do
                    parts[#parts + 1] = req._req[i]
                end
                return table.concat(parts)
            end

            local function decode_message_set(ms)
                local resp = response:new(string.char(0, 0, 0, 1) .. ms, 3)
                return record.message_set_decode(resp, 0)
            end

            local function u32_hex(n)
                if n < 0 then
                    n = n + 4294967296
                end
                return string.format("%08x", n)
            end

            local function all_ff(str, i, n)
                for j = i, i + n - 1 do
                    if string.byte(str, j) ~= 255 then
                        return false
                    end
                end
                return true
            end

            ngx.say("crc32c: ", u32_hex(protocol.crc32c("123456789")))

            local big = string.rep("x", 300) .. string.char(0) .. "y"
            local ms = message_set_bytes(request.API_VERSION_V3, { "key", "hello", nil, big }, 4)
            local msgs = decode_message_set(ms)

            ngx.say("magic0: ", string.byte(message_set_bytes(0, { "k", "v" }, 2), 21))
            ngx.say("magic1: ", string.byte(message_set_bytes(2, { "k", "v" }, 2), 21))
            ngx.say("magic2: ", string.byte(ms, 21))
            ngx.say("magic8: ", string.byte(message_set_bytes(8, { "k", "v" }, 2), 21))
            ngx.say("leader_epoch: ", all_ff(ms, 17, 4))
            ngx.say("producer_id: ", all_ff(ms, 48, 8))
            ngx.say("producer_epoch: ", all_ff(ms, 56, 2))
            ngx.say("base_sequence: ", all_ff(ms, 58, 4))

            local batch_len = response.to_int32(string.sub(ms, 13, 16))
            ngx.say("batch_len: ", batch_len == #ms - 16)

            ngx.say("count: ", #msgs)
            ngx.say("m1: ", msgs[1].key, ",", msgs[1].value, ",", tonumber(msgs[1].offset))
            ngx.say("m2_key_empty: ", msgs[2].key == "")
            ngx.say("m2_value: ", msgs[2].value == big)
            ngx.say("m2_off: ", tonumber(msgs[2].offset))
            ngx.say("same_ts: ", msgs[1].timestamp == msgs[2].timestamp and msgs[1].timestamp > 0)

            local empty = message_set_bytes(3, {}, 0)
            ngx.say("empty: ", empty == string.char(0, 0, 0, 0))

            local fake = {
                correlation_id = 1,
                client = { client_id = "cid" },
                api_version = 3,
                required_acks = 1,
                request_timeout = 2000,
            }
            local encoded = producer._encode_produce(fake, {
                topic_num = 1,
                topics = {
                    t = {
                        partition_num = 1,
                        partitions = {
                            [0] = { queue = { "k", "v" }, index = 2 },
                        },
                    },
                },
            })
            local raw = table.concat(encoded:package())
            ngx.say("txn_null: ", string.byte(raw, 18) == 255 and string.byte(raw, 19) == 255)
            local produced = decode_message_set(string.sub(raw, 41))
            ngx.say("wire: ", produced[1].key, ",", produced[1].value, ",", tonumber(produced[1].offset))

            fake.api_version = 2
            fake.correlation_id = 1
            local legacy = producer._encode_produce(fake, {
                topic_num = 1,
                topics = {
                    t = {
                        partition_num = 1,
                        partitions = {
                            [0] = { queue = { "k", "v" }, index = 2 },
                        },
                    },
                },
            })
            local legacy_raw = table.concat(legacy:package())
            ngx.say("v2_no_txn: ", string.byte(legacy_raw, 18) == 0 and string.byte(legacy_raw, 19) == 1)

            local function be16(n)
                if n < 0 then n = n + 65536 end
                return string.char(math.floor(n / 256) % 256, n % 256)
            end

            local function be32(n)
                if n < 0 then n = n + 4294967296 end
                local s = {}
                for i = 3, 0, -1 do
                    s[#s + 1] = string.char(math.floor(n / 256 ^ i) % 256)
                end
                return table.concat(s)
            end

            local function be64(n)
                if n == -1 then
                    return string.rep(string.char(255), 8)
                end
                local s = {}
                for i = 7, 0, -1 do
                    s[#s + 1] = string.char(math.floor(n / 256 ^ i) % 256)
                end
                return table.concat(s)
            end

            local function partition_v2(id, offset)
                return be32(id) .. be16(0) .. be64(offset) .. be64(-1)
            end

            local v2_body = be32(1) .. be16(1) .. "t" .. be32(1) .. partition_v2(0, 4) .. be32(9)
            local v2 = producer._decode_produce(response:new(string.char(0, 0, 0, 1) .. v2_body, 2))
            ngx.say("resp_v2: ", v2.t[0].errcode, ",", tonumber(v2.t[0].offset), ",", tonumber(v2.t[0].timestamp))

            local function partition_v8(id, offset, err_msg)
                local errors
                if err_msg then
                    errors = be32(1) .. be32(0) .. be16(#err_msg) .. err_msg
                else
                    errors = be32(0)
                end
                return be32(id) .. be16(0) .. be64(offset) .. be64(-1) .. be64(0) .. errors .. be16(-1)
            end

            local v8_body = be32(1) .. be16(1) .. "t" .. be32(2)
                .. partition_v8(0, 5, "bad")
                .. partition_v8(1, 9, nil)
                .. be32(0)
            local v8 = producer._decode_produce(response:new(string.char(0, 0, 0, 1) .. v8_body, 8))
            ngx.say("resp_v8: ", tonumber(v8.t[0].offset), ",", tonumber(v8.t[1].offset), ",", v8.t[0].errcode)
        ';
    }
--- request
GET /t
--- response_body
crc32c: e3069283
magic0: 0
magic1: 1
magic2: 2
magic8: 2
leader_epoch: true
producer_id: true
producer_epoch: true
base_sequence: true
batch_len: true
count: 2
m1: key,hello,0
m2_key_empty: true
m2_value: true
m2_off: 1
same_ts: true
empty: true
txn_null: true
wire: k,v,0
v2_no_txn: true
resp_v2: 0,4,-1
resp_v8: 5,9,0
--- no_error_log
[error]

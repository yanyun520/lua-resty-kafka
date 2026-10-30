local protocol = require("resty.kafka.protocol.common")
local bit = require("bit")
local math = require("math")
local ffi = require("ffi")

local crc32c = protocol.crc32c
local ngx_log = ngx.log
local ngx_crc32 = ngx.crc32_long
local ngx_now = ngx.now
local ERR = ngx.ERR
local INFO = ngx.INFO
local table_insert = table.insert
local table_concat = table.concat
local band = bit.band
local rshift = bit.rshift
local char = string.char
local math_abs = math.abs
local math_floor = math.floor

-- Sentinels used by the official producer for a non-idempotent batch.
-- See org.apache.kafka.common.record.RecordBatch.
local NO_PRODUCER_ID = -1
local NO_PRODUCER_EPOCH = -1
local NO_SEQUENCE = -1
local NO_PARTITION_LEADER_EPOCH = -1


local _M = {}


--- Decode MessageSet v0 and v1 (the old format) data.
-- Tip: The return value message contains the int64 value, which is of
-- type cdata and cannot be used directly in some scenarios.
-- @author bzp2010 <bzp2010@apache.org>
local function _message_set_v0_1_decode(resp, ret)
    local message = {}

    message.offset = resp:int64()

    -- Sometimes in low version protocols many useless empty data are
    -- returned, they are characterized by a -1 offset at the beginning
    -- and 0 for each offset after that, we will discard the data after that.
    -- The response will be closed (offset is set to the end),
    -- i.e. it cannot continue reading any data.
    -- Tip: offset uses int64 storage, and it is almost impossible for us
    -- to write so many messages, so the case where offset does have a
    -- negative value is not considered here.
    if message.offset < 0 then
        resp:close()
        return "empty message" -- return error
    end

    local message_size = resp:int32()
    local crc = resp:int32()
    local crc_content = resp:peek_bytes(resp.offset, message_size - 4)
    local calc_crc = ngx_crc32(crc_content)
    if crc ~= calc_crc and math_abs(crc) + math_abs(calc_crc) ~= 4294967296 then
        return "crc checksum error"
    end

    local magic_byte = resp:int8()

    -- TODO: support compressed Message Set
    local attributes = resp:int8()

    -- message version 1 added timestamp
    if magic_byte == protocol.API_VERSION_V1 then
        message.timestamp = resp:int64()
    else
        message.timestamp = 0
    end

    message.key = resp:bytes()
    message.value = resp:bytes()

    table_insert(ret, message)

    return nil -- error
end


--- Decode MessageSet v2 (aka RecordBatch) data.
-- Tip: The return value message contains the int64 value, which is of
-- type cdata and cannot be used directly in some scenarios.
-- @author bzp2010 <bzp2010@apache.org>
local function _message_set_v2_decode(resp, ret, fetch_offset)
    ret = ret or {}

    -- RecordBatch decoder, refer to this documents
    -- https://kafka.apache.org/documentation/#recordbatch
    local base_offset = resp:int64() -- baseOffset
    local batch_length = resp:int32() -- batchLength
    local partition_leader_epoch = resp:int32() -- partitionLeaderEpoch
    local magic_byte = resp:int8() -- magic
    local crc = resp:int32() -- crc

    -- Get all remaining message bodies by length for crc. The crc content
    -- starts with attributes, so here we need to reduce its middle
    -- three fields' length, include partition_leader_epoch (4 byte),
    -- magic_byte (1 byte) and crc (4 byte).
    local crc_content = resp:peek_bytes(resp.offset, batch_length - 4 - 1 - 4)
    local calc_crc = crc32c(crc_content)
    if crc ~= calc_crc then
        return "crc checksum error"
    end

    -- TODO: support compressed Message Set
    local attributes = resp:int16() -- attributes

    local last_offset_delta = resp:int32() -- lastOffsetDelta
    local last_offset = base_offset + last_offset_delta

    -- If the last record's offset is also less than fetch's offset,
    -- all outdated records are discarded.
    if last_offset < fetch_offset then
        resp:close()
        return "all records outdated"
    end

    -- RecordBatch contains the timestamp starting value and the
    -- maximum value of these records.
    local first_timestamp = resp:int64() -- firstTimestamp
    local max_timestamp = resp:int64() -- maxTimestamp

    -- These fields are intended to support idempotent messages.
    -- The features are NYI
    local producer_id = resp:int64() -- producerId
    local producer_epoch = resp:int16() -- producerEpoch
    local base_sequence = resp:int32() -- baseSequence

    local record_num = resp:int32() -- [records] array length

    for i = 1, record_num do
        local message = {}

        -- Record decoder, refer to this documents
        -- https://kafka.apache.org/documentation/#record
        local len = resp:varint()
        local message_end = resp.offset + len

        -- According to the protocol, only reserved.
        local record_attributes = resp:int8()

        -- Offset of this Record from RecordBatch's base value.
        local timestamp_delta = resp:varlong()
        local offset_delta = resp:varint()

        -- The sixth bit of isControlBatch (bit 5) in attributes has a value of 1,
        -- i.e. the current MessageSet (RecordBatch) is a ControlBatch.
        -- !! Not process Conrtol Batch for now, they will be skipped !!
        if band(attributes, 0x20) > 0 then
            resp:varint() -- keyLength skipped
            resp:int16() -- ControlBatch version skipped
            resp:int16() -- ControlBatch type skipped

            ngx_log(INFO, "A Control Batch was skipped during the parsing of the message v2")

            goto continue
        end

        message.offset = base_offset + offset_delta
        message.timestamp = first_timestamp + timestamp_delta
        message.key = resp:varint_bytes()
        message.value = resp:varint_bytes()

        table_insert(ret, message)

        -- Calculates the length of the header field by the expected end position
        -- of the message and skips the specified number of bytes.
        -- !! Not parse message headers for now, they will be skipped !!
        local header_len = message_end - resp.offset
        resp.offset = resp.offset + header_len
        ::continue::
    end

    return nil
end


---------
-- Decode the message set, a different version of decoder will be selected
-- automatically according to the MagicByte inside the message
-- @author bzp2010 <bzp2010@apache.org>
function _M.message_set_decode(resp, fetch_offset)
    local ret = {}
    local message_set_size = resp:int32()

    -- Keep parsing the message until all the data in the
    -- current response is exhausted
    while resp:remain() > 0 do
        -- Get 1 byte integer after 2 byte offset
        -- [MessageSet] message magic_byte, it contains Message version
        local message_version = resp:peek_int(16, 1)

        local messages, messages_set_info, err
        if message_version == 0 or message_version == 1 then
            -- old MessageSet v0 or v1
            err = _message_set_v0_1_decode(resp, ret)
        else
            -- MessageSet v2 aka RecordBatch
            err = _message_set_v2_decode(resp, ret, fetch_offset)
        end

        if err then
            ngx_log(ERR, "failed to decode message set, err: ", err)
        end
    end

    return ret
end


local function str_int8(int)
    return char(band(int, 0xff))
end


local function str_int16(int)
    return char(band(rshift(int, 8), 0xff),
                band(int, 0xff))
end


local function str_int32(int)
    return char(band(rshift(int, 24), 0xff),
                band(rshift(int, 16), 0xff),
                band(rshift(int, 8), 0xff),
                band(int, 0xff))
end


-- XX int can be cdata: LL or lua number
local function str_int64(int)
    int = int * 1LL

    return char(tonumber(band(rshift(int, 56), 0xff)),
                tonumber(band(rshift(int, 48), 0xff)),
                tonumber(band(rshift(int, 40), 0xff)),
                tonumber(band(rshift(int, 32), 0xff)),
                tonumber(band(rshift(int, 24), 0xff)),
                tonumber(band(rshift(int, 16), 0xff)),
                tonumber(band(rshift(int, 8), 0xff)),
                tonumber(band(int, 0xff)))
end


--- Encode a non-negative integer as an unsigned varint (7 data bits per byte).
-- Values used here (record lengths, header counts) fit in 32 bits, well below
-- the 2^53 exact-integer range of a Lua number.
local function encode_unsigned_varint(num)
    local out = {}
    local i = 0

    repeat
        i = i + 1
        local b = num % 128
        num = math_floor(num / 128)
        if num > 0 then
            b = b + 128
        end
        out[i] = char(b)
    until num == 0

    return table_concat(out)
end


--- ZigZag-encode a signed integer and write it as an unsigned varint.
-- Matches Kafka's ByteUtils.writeVarint / writeVarlong.
local function encode_zigzag_varint(num)
    if num >= 0 then
        return encode_unsigned_varint(num * 2)
    end

    return encode_unsigned_varint((-num) * 2 - 1)
end


--- Encode one Record (magic 2).
-- https://kafka.apache.org/documentation/#record
local function encode_record(key, value, offset_delta)
    -- Preserve the legacy producer behavior: a missing key or value is an
    -- empty byte string, not a null record field.
    if key == nil then
        key = ""
    end
    if value == nil then
        value = ""
    end

    local body = table_concat({
        str_int8(0),                              -- attributes (unused)
        encode_zigzag_varint(0),                  -- timestampDelta
        encode_zigzag_varint(offset_delta),       -- offsetDelta
        encode_zigzag_varint(#key),               -- keyLength
        key,
        encode_zigzag_varint(#value),             -- valueLength
        value,
        encode_zigzag_varint(0),                  -- headers count
    })

    -- length covers every byte of the record except the length field itself
    return encode_zigzag_varint(#body) .. body
end


--- Encode messages as one uncompressed RecordBatch (message format v2).
-- `messages` is a flat {key, value, key, value, ...} array and `index` is its
-- used length (not necessarily `#messages`). The returned string is the batch
-- itself, starting at baseOffset; the caller writes the int32 size prefix.
-- https://kafka.apache.org/documentation/#recordbatch
function _M.encode_record_batch(messages, index)
    local records = {}
    local count = 0

    for i = 1, index, 2 do
        count = count + 1
        records[count] = encode_record(messages[i], messages[i + 1], count - 1)
    end

    local now_ms = ffi.new("int64_t", ngx_now() * 1000)

    -- CRC32C covers every byte that follows the crc field, from attributes
    -- through the end of the records.
    local after_crc = table_concat({
        str_int16(0),                         -- attributes: no compression, CreateTime
        str_int32(count - 1),                 -- lastOffsetDelta
        str_int64(now_ms),                    -- baseTimestamp / firstTimestamp
        str_int64(now_ms),                    -- maxTimestamp
        str_int64(NO_PRODUCER_ID),            -- producerId
        str_int16(NO_PRODUCER_EPOCH),         -- producerEpoch
        str_int32(NO_SEQUENCE),               -- baseSequence
        str_int32(count),                     -- records count
        table_concat(records),
    })

    -- batchLength counts every byte after the batchLength field.
    local tail = table_concat({
        str_int32(NO_PARTITION_LEADER_EPOCH), -- partitionLeaderEpoch
        str_int8(2),                          -- magic
        str_int32(crc32c(after_crc)),         -- crc
        after_crc,
    })

    return table_concat({
        str_int64(0),                         -- baseOffset, assigned by the broker
        str_int32(#tail),                     -- batchLength
        tail,
    })
end


return _M

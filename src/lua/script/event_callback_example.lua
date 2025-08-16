-- The callbacks are called serially, and the callback function MUST return immediately.
-- If a callback function takes a long time to execute, it may block a whole pipeline.
-- Callback function name should be `on_event`
function on_event(event_data)
    -- The following code prints the event
    if event_data.event_type == 1 << 1 then
        print("PIPELINE_START")
    elseif event_data.event_type == 1 << 2 then
        print("PIPELINE_END")
    elseif event_data.event_type == 1 << 3 then
        print("SYNC_START")
    elseif event_data.event_type == 1 << 4 then
        print("SYNC_COMPLETE")
    elseif event_data.event_type == 1 << 5 then
        print("SYNC_DELETE")
    elseif event_data.event_type == 1 << 6 then
        print("SYNC_ETAG_VERIFIED")
    elseif event_data.event_type == 1 << 7 then
        print("SYNC_CHECKSUM_VERIFIED")
    elseif event_data.event_type == 1 << 8 then
        print("SYNC_ETAG_MISMATCH")
    elseif event_data.event_type == 1 << 9 then
        print("SYNC_CHECKSUM_MISMATCH")
    elseif event_data.event_type == 1 << 10 then
        print("SYNC_WARNING")
    elseif event_data.event_type == 1 << 11 then
        print("PIPELINE_ERROR")
    elseif event_data.event_type == 1 << 12 then
        print("SYNC_CANCEL")
    elseif event_data.event_type == 1 << 13 then
        print("SYNC_WRITE")
    else
        print("UNKNOWN_EVENT")
    end

    print("key:", event_data.key)
    print("source_version_id:", event_data.source_version_id)
    print("target_version_id:", event_data.target_version_id)
    print("source_last_modified:", event_data.source_last_modified)
    print("target_last_modified:", event_data.target_last_modified)
    print("source_size:", event_data.source_size)
    print("target_size:", event_data.target_size)
    print("checksum_algorithm:", event_data.checksum_algorithm)
    print("source_checksum:", event_data.source_checksum)
    print("target_checksum:", event_data.target_checksum)
    print("source_etag:", event_data.source_etag)
    print("target_etag:", event_data.target_etag)
    print("byte_written", event_data.byte_written)
    print("upload_id", event_data.upload_id)
    print("part_number", event_data.part_number)
    print("message:", event_data.message)
    print("")
end
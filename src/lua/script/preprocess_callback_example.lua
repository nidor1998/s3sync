-- Callback function name should be `preprocess_callback`
-- The callbacks are called serially, and the callback function MUST return immediately.
-- If a callback function takes a long time to execute, it may block a whole pipeline.
function preprocess_callback(source_object, upload_metadata)
    -- The following code prints the source object attributes.
    print("source_object.key: ", source_object.key)
    print("source_object.accept_ranges: ", source_object.accept_ranges)
    print("source_object.bucket_key_enabled: ", source_object.bucket_key_enabled)
    print("source_object.cache_control: ", source_object.cache_control)
    print("source_object.checksum_crc32: ", source_object.checksum_crc32)
    print("source_object.checksum_crc32_c: ", source_object.checksum_crc32_c)
    print("source_object.checksum_crc64_nvme: ", source_object.checksum_crc64_nvme)
    print("source_object.checksum_sha1: ", source_object.checksum_sha1)
    print("source_object.checksum_sha256: ", source_object.checksum_sha256)
    print("source_object.checksum_type: ", source_object.checksum_type)
    print("source_object.content_disposition:", source_object.content_disposition)
    print("source_object.content_encoding:", source_object.content_encoding)
    print("source_object.content_language:", source_object.content_language)
    print("source_object.content_length:", source_object.content_length)
    print("source_object.content_range:", source_object.content_range)
    print("source_object.content_type:", source_object.content_type)
    print("source_object.e_tag:", source_object.e_tag)
    print("source_object.expires_string:", source_object.expires_string)
    print("source_object.last_modified:", source_object.last_modified)
    print("source_object.missing_meta:", source_object.missing_meta)
    print("source_object.object_lock_legal_hold_status:", source_object.object_lock_legal_hold_status)
    print("source_object.object_lock_mode:", source_object.object_lock_mode)
    print("source_object.object_lock_retain_until_date:", source_object.object_lock_retain_until_date)
    print("source_object.parts_count:", source_object.parts_count)
    print("source_object.replication_status:", source_object.replication_status)
    print("source_object.request_charged:", source_object.request_charged)
    print("source_object.restore:", source_object.restore)
    print("source_object.server_side_encryption:", source_object.server_side_encryption)
    print("source_object.sse_customer_algorithm:", source_object.sse_customer_algorithm)
    print("source_object.sse_customer_key_md5:", source_object.sse_customer_key_md5)
    print("source_object.ssekms_key_id:", source_object.ssekms_key_id)
    print("source_object.storage_class:", source_object.storage_class)
    print("source_object.tag_count:", source_object.tag_count)
    print("source_object.version_id:", source_object.version_id)
    print("source_object.website_redirect_location:", source_object.website_redirect_location)
    for key, value in pairs(source_object.metadata) do
        print("source_object.metadata:", key, value)
    end

    -- Modify `upload_metadata` to change the upload object's metadata.
    -- The following code modifies user-defined metadata and other properties.
    upload_metadata.acl = "bucket-owner-full-control"
    upload_metadata.cache_control = "s-maxage=1604800"
    upload_metadata.content_disposition = "inline"
    upload_metadata.content_encoding = "gzip"
    upload_metadata.content_language = "en-US"
    upload_metadata.content_type = "video/mpeg"
    upload_metadata.request_payer = "requester"
    upload_metadata.storage_class = "INTELLIGENT_TIERING"
    upload_metadata.website_redirect_location = "https://example.com/redirect"
    upload_metadata.tagging = "tag1=value1&tag777=value777"
    upload_metadata.expires_string = "2029-02-19T12:00:00Z"
    upload_metadata.metadata["my_custom_key"] = "new_value3"

    -- The following code prints the upload metadata attributes.
    print("upload_metadata.acl:", upload_metadata.acl)
    print("upload_metadata.cache_control:", upload_metadata.cache_control)
    print("upload_metadata.content_disposition:", upload_metadata.content_disposition)
    print("upload_metadata.content_encoding:", upload_metadata.content_encoding)
    print("upload_metadata.content_language:", upload_metadata.content_language)
    print("upload_metadata.content_type:", upload_metadata.content_type)
    print("upload_metadata.expires_string:", upload_metadata.expires_string)
    print("upload_metadata.request_payer", upload_metadata.request_payer)
    print("upload_metadata.storage_class", upload_metadata.storage_class)
    print("upload_metadata.website_redirect_location:", upload_metadata.website_redirect_location)
    print("upload_metadata.tagging:", upload_metadata.tagging)
    for key, value in pairs(upload_metadata.metadata) do
        print("upload_metadata.metadata:", key, value)
    end
    print("")

    -- If an error occurs, you can call error() to stop the upload and whole pipeline will fail.
    -- error("This is a test error to demonstrate error handling in preprocess_callback.")

    -- The first argument of the return value is a boolean indicating whether the upload should proceed.
    -- If you want to skip the upload, you can return false.
    -- The second argument is the modified upload metadata.
    return true, upload_metadata
end

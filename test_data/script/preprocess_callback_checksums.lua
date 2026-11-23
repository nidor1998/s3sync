-- Copies the checksum fields of the source object into the user-defined metadata,
-- so that the test can verify which values are visible from Lua.
function preprocess_callback(source_object, upload_metadata)
    local fields = {
        "checksum_crc32",
        "checksum_crc32_c",
        "checksum_crc64_nvme",
        "checksum_sha1",
        "checksum_sha256",
        "checksum_md5",
        "checksum_sha512",
        "checksum_xxhash128",
        "checksum_xxhash3",
        "checksum_xxhash64",
        "checksum_type",
    }

    for _, field in ipairs(fields) do
        if source_object[field] ~= nil then
            upload_metadata.metadata[field] = source_object[field]
        end
    end

    return true, upload_metadata
end

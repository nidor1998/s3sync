-- Callback function name should be `filter`
function filter(source_object)
    print("key:", source_object.key)
    print("last_modified:", source_object.last_modified)
    print("version_id:", source_object.version_id)
    print("e_tag:", source_object.e_tag)
    print("checksum_algorithm:", source_object.checksum_algorithm)
    print("checksum_type:", source_object.checksum_type)
    print("is_latest:", source_object.is_latest)
    print("is_delete_marker:", source_object.is_delete_marker)
    print("")

    -- If you want to filter out the object, return false.
    return true
end
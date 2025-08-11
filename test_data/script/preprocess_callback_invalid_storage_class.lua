-- Callback function name should be `preprocess_callback`
function preprocess_callback(source_object, upload_metadata)
    upload_metadata.storage_class = "INVALID"

    return true, upload_metadata
end

-- Callback function name should be `preprocess_callback`
function preprocess_callback(source_object, upload_metadata)
    upload_metadata.expires_string = "ABC2029-02-19T12:00:00Z"

    return true, upload_metadata
end
